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
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.WindowAggNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * This is the sort-based window operator.
 */
public class WinAggregateExec extends UnaryPhysicalExec {
  protected final int nonFunctionColumnNum;
  protected final int nonFunctionColumns[];
  protected final int partitionKeyNum;
  protected final int partitionKeyIds[];
  protected final int aggFunctionsNum;
  protected final AggregationFunctionCallEval functions[];
  private FunctionContext contexts [];
  private FunctionContext newContexts [];
  private Tuple lastKey = null;
  private boolean noMoreTuples = false;
  private boolean finish = false;
  private final boolean hasPartitionKeys;

  enum WindowState {
    INIT,
    ACCUMULATING,
    AGGREGATION,
    RETRIVING,
    FINISH
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
      aggFunctionsNum = functions.length;
    } else {
      functions = new AggregationFunctionCallEval[0];
      aggFunctionsNum = 0;
    }

    nonFunctionColumnNum = plan.getTargets().length - aggFunctionsNum;
    nonFunctionColumns = new int[nonFunctionColumnNum];
    for (int idx = 0; idx < plan.getTargets().length - aggFunctionsNum; idx++) {
      nonFunctionColumns[idx] = inSchema.getColumnId(plan.getTargets()[idx].getCanonicalName());
    }
  }

  private void transition(WindowState state) {
    this.state = state;
  }

  @Override
  public Tuple next() throws IOException {
    Tuple currentKey = null;
    Tuple tuple = null;

    while(!context.isStopped() && state != WindowState.FINISH) {

      if (state == WindowState.INIT) {
        if (firstTime) {
          accumulated = Lists.newArrayList();
          contexts = new FunctionContext[aggFunctionsNum];
          for(int evalIdx = 0; evalIdx < aggFunctionsNum; evalIdx++) {
            contexts[evalIdx] = functions[evalIdx].newContext();
          }
          firstTime = false;
        }
        transition(WindowState.ACCUMULATING);
      }

      if (state != WindowState.RETRIVING) {
        tuple = child.next();

        if (tuple == null) {
          noMoreTuples = true;
          transition(WindowState.AGGREGATION);
        }

        if (tuple != null && hasPartitionKeys) {
          // get a key tuple
          currentKey = new VTuple(partitionKeyIds.length);
          for (int i = 0; i < partitionKeyIds.length; i++) {
            currentKey.put(i, tuple.get(partitionKeyIds[i]));
          }
        }
      }

      if (state == WindowState.ACCUMULATING) {
        if (lastKey == null || lastKey.equals(currentKey)) {

          // aggregate
          for (int i = 0; i < aggFunctionsNum; i++) {
            functions[i].merge(contexts[i], inSchema, tuple);
          }

          int columnIdx = 0;
          Tuple outputTuple = new VTuple(outSchema.size());
          for(; columnIdx < nonFunctionColumnNum; columnIdx++) {
            outputTuple.put(columnIdx, tuple.get(nonFunctionColumns[columnIdx]));
          }
          accumulated.add(outputTuple);

          if (!hasPartitionKeys) {
            transition(WindowState.AGGREGATION);
          }
        } else {

          int columnIdx = 0;
          Tuple outputTuple = new VTuple(outSchema.size());
          for(; columnIdx < nonFunctionColumnNum; columnIdx++) {
            outputTuple.put(columnIdx, tuple.get(nonFunctionColumns[columnIdx]));
          }
          nextAccumulated = Lists.newArrayList();
          nextAccumulated.add(outputTuple);

          newContexts = new FunctionContext[functions.length];
          for (int i = 0; i < aggFunctionsNum; i++) {
            newContexts[i] = functions[i].newContext();
            functions[i].merge(newContexts[i], inSchema, tuple);
          }

          transition(WindowState.AGGREGATION);
        }

        lastKey = currentKey;
      }

      if (state == WindowState.AGGREGATION) {
        // aggregation accumulated one
        Tuple aggregatedTuple = new VTuple(aggFunctionsNum);
        for(int aggFuncIdx = 0; aggFuncIdx < aggFunctionsNum; aggFuncIdx++) {
          aggregatedTuple.put(aggFuncIdx, functions[aggFuncIdx].terminate(contexts[aggFuncIdx]));
        }

        for (Tuple t : accumulated) {
          t.put(nonFunctionColumnNum, aggregatedTuple);
        }

        transition(WindowState.RETRIVING);
        windowTupleIterator = accumulated.iterator();
      }

      if (state == WindowState.RETRIVING) {
        if (windowTupleIterator.hasNext()) {
          return windowTupleIterator.next();
        } else {
          accumulated.clear();
          if (hasPartitionKeys) {
            transition(WindowState.INIT);
            contexts = newContexts;
            accumulated = nextAccumulated;
          } else {
            transition(WindowState.ACCUMULATING);
          }

          if (noMoreTuples) {
            transition(WindowState.FINISH);
          }
        }
      }
    }

    return null;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastKey = null;
    noMoreTuples = false;
  }
}
