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
import org.apache.tajo.algebra.WindowSpecExpr;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.WindowFunctionEval;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.WindowAggNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.tajo.algebra.WindowSpecExpr.WindowFrameStartBoundType;
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
  private Tuple lastKey = null;
  private boolean noMoreTuples = false;
  private final boolean hasPartitionKeys;

  private boolean [] windowFuncFlags;
  private boolean [] aggregatedFuncFlags;
  private boolean [] orderedFuncFlags;
  private boolean [] currentRowFlags;

  enum WindowState {
    INIT,
    AGGREGATING,
    EVALUATION,
    RETRIEVING,
    END_OF_TUPLE
  }

  boolean firstTime = true;
  List<Tuple> evaluatedTuples = null;
  List<Tuple> accumulatedInTuples = null;
  List<Tuple> nextAccumulatedProjected = null;
  List<Tuple> nextAccumulatedInTuples = null;
  WindowState state = WindowState.INIT;
  Iterator<Tuple> tupleInFrameIterator = null;

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

      windowFuncFlags = new boolean[functions.length];
      aggregatedFuncFlags = new boolean[functions.length];
      orderedFuncFlags = new boolean[functions.length];
      currentRowFlags = new boolean[functions.length];
      List<Integer> aggFuncIdxList = Lists.newArrayList();

      for (int i = 0; i < functions.length; i++) {
        FunctionType type = functions[i].getFuncDesc().getFuncType();

        if (functions[i].getWindowFrame().getStartBound().getBoundType() == WindowFrameStartBoundType.CURRENT_ROW) {
          currentRowFlags[i] = true;
        }

        switch (type) {
        case WINDOW:
          if (functions[i].hasSortSpecs()) {
            orderedFuncFlags[i] = true;
          } else {
            windowFuncFlags[i] = true;
          }
          break;
        default:
          aggregatedFuncFlags[i] = true;
          aggFuncIdxList.add(i);
        }
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
        transition(WindowState.AGGREGATING);
      }

      if (state != WindowState.RETRIEVING) { // read an input tuple and build a partition key
        readTuple = child.next();

        if (readTuple == null) { // the end of tuple
          noMoreTuples = true;
          transition(WindowState.EVALUATION);
        }

        if (readTuple != null && hasPartitionKeys) { // get a key tuple
          currentKey = new VTuple(partitionKeyIds.length);
          for (int i = 0; i < partitionKeyIds.length; i++) {
            currentKey.put(i, readTuple.get(partitionKeyIds[i]));
          }
        }
      }

      if (state == WindowState.AGGREGATING) {
        accumulatingWindow(currentKey, readTuple);
      }

      if (state == WindowState.EVALUATION) {
        evaluationWindowFrame();

        tupleInFrameIterator = evaluatedTuples.iterator();
        transition(WindowState.RETRIEVING);
      }

      if (state == WindowState.RETRIEVING) {
        if (tupleInFrameIterator.hasNext()) {
          return tupleInFrameIterator.next();
        } else {
          finalizeWindow();
        }
      }
    }

    return null;
  }

  private void initWindow() {
    if (firstTime) {
      accumulatedInTuples = Lists.newArrayList();

      contexts = new FunctionContext[functionNum];
      for(int evalIdx = 0; evalIdx < functionNum; evalIdx++) {
        contexts[evalIdx] = functions[evalIdx].newContext();
      }
      firstTime = false;
    }
  }

  private void accumulatingWindow(Tuple currentKey, Tuple inTuple) {
    if (lastKey == null || lastKey.equals(currentKey)) {
      accumulatedInTuples.add(new VTuple(inTuple));

      if (!hasPartitionKeys) {
        transition(WindowState.EVALUATION);
      }
    } else {
      preAccumulatingNextWindow(inTuple);
      transition(WindowState.EVALUATION);
    }

    lastKey = currentKey;
  }

  private void preAccumulatingNextWindow(Tuple inTuple) {
    Tuple projectedTuple = new VTuple(outSchema.size());
    for(int idx = 0; idx < nonFunctionColumnNum; idx++) {
      projectedTuple.put(idx, inTuple.get(nonFunctionColumns[idx]));
    }
    nextAccumulatedProjected = Lists.newArrayList();
    nextAccumulatedProjected.add(projectedTuple);
    nextAccumulatedInTuples = Lists.newArrayList();
    nextAccumulatedInTuples.add(new VTuple(inTuple));
  }

  private void evaluationWindowFrame() {
    TupleComparator comp;
    for (int idx = 0; idx < functions.length; idx++) {

      if (orderedFuncFlags[idx]) {
        comp = new TupleComparator(inSchema, functions[idx].getSortSpecs());
        Collections.sort(accumulatedInTuples, comp);
      }

      for (int i = 0; i < accumulatedInTuples.size(); i++) {
        Tuple inTuple = accumulatedInTuples.get(i);

        Tuple projectedTuple = new VTuple(outSchema.size());
        for(int c = 0; c < nonFunctionColumnNum; c++) {
          projectedTuple.put(c, inTuple.get(nonFunctionColumns[c]));
        }

        functions[idx].merge(contexts[idx], inSchema, inTuple);
        if (currentRowFlags[idx]) {
          Datum result = functions[idx].terminate(contexts[idx]);
          projectedTuple.put(nonFunctionColumnNum + idx, result);
        }
      }

      if (aggregatedFuncFlags[idx]) {
        Datum result = functions[idx].terminate(contexts[idx]);
        for (int i = 0; i < accumulatedInTuples.size(); i++) {
          Tuple outTuple = evaluatedTuples.get(i);
          outTuple.put(nonFunctionColumnNum + idx, result);
        }
      }
    }
  }

  private void finalizeWindow() {
    accumulatedInTuples.clear();
    evaluatedTuples.clear();

    if (noMoreTuples) {
      transition(WindowState.END_OF_TUPLE);
    } else {
      if (hasPartitionKeys) {
        evaluatedTuples = nextAccumulatedProjected;
        accumulatedInTuples = nextAccumulatedInTuples;
      } else {
        // it reuses the firstly created lists.
        evaluatedTuples.clear();
        accumulatedInTuples.clear();
      }
      contexts = new FunctionContext[functionNum];
      for(int evalIdx = 0; evalIdx < functionNum; evalIdx++) {
        contexts[evalIdx] = functions[evalIdx].newContext();
      }
      transition(WindowState.INIT);
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastKey = null;
    noMoreTuples = false;
  }
}
