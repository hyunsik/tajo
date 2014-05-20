/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.physical;

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.WindowFunctionEval;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.WindowAggNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;

/**
 * This is the sort-based window operator.
 */
public class WinAggregateExec extends UnaryPhysicalExec {
  protected final int nonFunctionColumnNum;
  protected final int nonFunctionColumns[];
  protected final int partitionKeyNum;
  protected final int partitionKeyIds[];
  protected final int functionNum;
  protected final WindowFunctionEval functions[];
  private FunctionContext contexts [];
  private FunctionContext newContexts [];
  private Tuple lastKey = null;
  private boolean noMoreTuples = false;
  private final boolean hasPartitionKeys;

  private int [] windowFuncIndices;
  private int [] aggFuncIndices;

  enum WindowState {
    INIT,
    ACCUMULATING,
    AGGREGATION,
    RETRIEVING,
    END_OF_TUPLE
  }

  boolean firstTime = true;
  List<Tuple> accumulated = null;
  List<Tuple> nextAccumulated = null;
  WindowState state = WindowState.INIT;
  Iterator<Tuple> windowTupleIterator = null;

  public WinAggregateExec(TaskAttemptContext context, WindowAggNode plan, PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);

    if (plan.hasPartitionKeys()) {
      final Column[] keyColumns = plan.getPartitionKeys();
      partitionKeyNum = keyColumns.length;
      partitionKeyIds = new int[partitionKeyNum];
      Column col;
      for (int idx = 0; idx < plan.getPartitionKeys().length; idx++) {
        col = keyColumns[idx];
        partitionKeyIds[idx] = inSchema.getColumnId(col.getQualifiedName());
      }
      hasPartitionKeys = true;
    } else {
      partitionKeyNum = 0;
      partitionKeyIds = null;
      hasPartitionKeys = false;
    }

    if (plan.hasAggFunctions()) {
      functions = plan.getWindowFunctions();
      functionNum = functions.length;

      List<Integer> windowFuncList = Lists.newArrayList();
      List<Integer> aggFuncList = Lists.newArrayList();
      for (int i = 0; i < functions.length; i++) {
        FunctionType type = functions[i].getFuncDesc().getFuncType();
        switch (type) {
        case WINDOW:
          windowFuncList.add(i); break;
        default:
          aggFuncList.add(i);
        }
      }
      windowFuncIndices = new int[windowFuncList.size()];
      for (int i = 0; i < aggFuncList.size(); i++) {
        windowFuncIndices[i] = windowFuncList.get(i);
      }
      aggFuncIndices = new int[aggFuncList.size()];
      for (int i = 0; i < aggFuncList.size(); i++) {
        aggFuncIndices[i] = aggFuncList.get(i);
      }

    } else {
      functions = new WindowFunctionEval[0];
      functionNum = 0;
    }

    nonFunctionColumnNum = plan.getTargets().length - functionNum;
    nonFunctionColumns = new int[nonFunctionColumnNum];
    for (int idx = 0; idx < plan.getTargets().length - functionNum; idx++) {
      nonFunctionColumns[idx] = inSchema.getColumnId(plan.getTargets()[idx].getCanonicalName());
    }
  }

  private void transition(WindowState state) {
    this.state = state;
  }

  @Override
  public Tuple next() throws IOException {
    Tuple currentKey = null;
    Tuple readTuple = null;

    while(!context.isStopped() && state != WindowState.END_OF_TUPLE) {

      if (state == WindowState.INIT) {
        initWindow();
        transition(WindowState.ACCUMULATING);
      }

      if (state != WindowState.RETRIEVING) { // read an input tuple and build a partition key
        readTuple = child.next();

        if (readTuple == null) { // the end of tuple
          noMoreTuples = true;
          transition(WindowState.AGGREGATION);
        }

        if (readTuple != null && hasPartitionKeys) { // get a key tuple
          currentKey = new VTuple(partitionKeyIds.length);
          for (int i = 0; i < partitionKeyIds.length; i++) {
            currentKey.put(i, readTuple.get(partitionKeyIds[i]));
          }
        }
      }

      if (state == WindowState.ACCUMULATING) {
        accumulatingWindow(currentKey, readTuple);
      }

      if (state == WindowState.AGGREGATION) {
        aggregationWindow();
        transition(WindowState.RETRIEVING);
      }

      if (state == WindowState.RETRIEVING) {
        if (windowTupleIterator.hasNext()) {
          return windowTupleIterator.next();
        } else {
          finalizeWindow();
        }
      }
    }

    return null;
  }

  private void initWindow() {
    if (firstTime) {
      accumulated = Lists.newArrayList();
      contexts = new FunctionContext[functionNum];
      for(int evalIdx = 0; evalIdx < functionNum; evalIdx++) {
        contexts[evalIdx] = functions[evalIdx].newContext();
      }
      firstTime = false;
    }
  }

  private void accumulatingWindow(Tuple currentKey, Tuple tuple) {
    if (lastKey == null || lastKey.equals(currentKey)) {
      accumulatingWindowContinuously(tuple);

      if (!hasPartitionKeys) {
        transition(WindowState.AGGREGATION);
      }
    } else {
      preAccumulatingNextWindow(tuple);
      transition(WindowState.AGGREGATION);
    }

    lastKey = currentKey;
  }

  private void accumulatingWindowContinuously(Tuple tuple) {
    int columnIdx = 0;
    Tuple outputTuple = new VTuple(outSchema.size());
    for(; columnIdx < nonFunctionColumnNum; columnIdx++) {
      outputTuple.put(columnIdx, tuple.get(nonFunctionColumns[columnIdx]));
    }

    // aggregate
    for (int i = 0; i < functionNum; i++) {
      functions[i].merge(contexts[i], inSchema, tuple);
    }
    for (int i = 0; i < windowFuncIndices.length; i++) {
      outputTuple.put(columnIdx + i, functions[i].terminate(contexts[i]));
    }
    accumulated.add(outputTuple);
  }

  private void preAccumulatingNextWindow(Tuple tuple) {
    int columnIdx = 0;
    Tuple outputTuple = new VTuple(outSchema.size());
    for(; columnIdx < nonFunctionColumnNum; columnIdx++) {
      outputTuple.put(columnIdx, tuple.get(nonFunctionColumns[columnIdx]));
    }

    newContexts = new FunctionContext[functions.length];
    for (int i = 0; i < functionNum; i++) {
      newContexts[i] = functions[i].newContext();
      functions[i].merge(newContexts[i], inSchema, tuple);

      if (functions[i].getFuncDesc().getFuncType() == FunctionType.WINDOW) {
        outputTuple.put(columnIdx + i, functions[i].terminate(newContexts[i]));
      }
    }

    nextAccumulated = Lists.newArrayList();
    nextAccumulated.add(outputTuple);
  }

  private void aggregationWindow() {
    // aggregation accumulated one
    Datum [] aggregatedValues = new Datum[aggFuncIndices.length];
    for(int i = 0; i < aggFuncIndices.length; i++) {
      aggregatedValues[i] = functions[aggFuncIndices[i]].terminate(contexts[aggFuncIndices[i]]);
    }

    for (Tuple t : accumulated) {
      for (int i = 0; i < aggFuncIndices.length; i++) {
        t.put(nonFunctionColumnNum + aggFuncIndices[i], aggregatedValues[i]);
      }
    }

    windowTupleIterator = accumulated.iterator();
  }

  private void finalizeWindow() {
    accumulated.clear();
    if (hasPartitionKeys) {
      transition(WindowState.INIT);
      contexts = newContexts;
      accumulated = nextAccumulated;
    } else {
      transition(WindowState.ACCUMULATING);
    }

    if (noMoreTuples) {
      transition(WindowState.END_OF_TUPLE);
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastKey = null;
    noMoreTuples = false;
  }
}
