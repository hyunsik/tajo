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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.WindowAggNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * This is the sort-based aggregation operator.
 *
 * <h3>Implementation</h3>
 * Sort Aggregation has two states while running.
 *
 * <h4>Aggregate state</h4>
 * If lastkey is null or lastkey is equivalent to the current key, sort aggregation is changed to this state.
 * In this state, this operator aggregates measure values via aggregation functions.
 *
 * <h4>Finalize state</h4>
 * If currentKey is different from the last key, it computes final aggregation results, and then
 * it makes an output tuple.
 */
public class WinAggregateExec extends UnaryPhysicalExec {
  protected int nonAggregatedColumnNum;
  protected int nonAggregatedColumns[];
  protected int partitionKeyNum;
  protected int partitionKeyIds[];
  protected final int aggFunctionsNum;
  protected final AggregationFunctionCallEval aggFunctions[];
  private Tuple lastKey = null;
  private boolean finished = false;
  private FunctionContext contexts[];

  public WinAggregateExec(TaskAttemptContext context, WindowAggNode plan, PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    contexts = new FunctionContext[plan.getAggFunctions().length];

    if (plan.hasPartitionKeys()) {
      final Column[] keyColumns = plan.getPartitionKeys();
      partitionKeyNum = keyColumns.length;
      partitionKeyIds = new int[partitionKeyNum];
      Column col;
      for (int idx = 0; idx < plan.getPartitionKeys().length; idx++) {
        col = keyColumns[idx];
        partitionKeyIds[idx] = inSchema.getColumnId(col.getQualifiedName());
      }
    }

    if (plan.hasAggFunctions()) {
      aggFunctions = plan.getAggFunctions();
      aggFunctionsNum = aggFunctions.length;
    } else {
      aggFunctions = new AggregationFunctionCallEval[0];
      aggFunctionsNum = 0;
    }

    nonAggregatedColumnNum = plan.getTargets().length - aggFunctionsNum;
    nonAggregatedColumns = new int[nonAggregatedColumnNum];
    for (int idx = 0; idx < plan.getTargets().length - aggFunctionsNum; idx++) {
      nonAggregatedColumns[idx] = inSchema.getColumnId(plan.getTargets()[idx].getCanonicalName());
    }
  }

  boolean accumulatedPhase = false;
  boolean hasPartitionKeys = false;

  @Override
  public Tuple next() throws IOException {
    Tuple currentKey = null;
    Tuple tuple;
    Tuple outputTuple = null;

    while(!context.isStopped() && (tuple = child.next()) != null) {

      if (hasPartitionKeys) {
        // get a key tuple
        currentKey = new VTuple(partitionKeyIds.length);
        for (int i = 0; i < partitionKeyIds.length; i++) {
          currentKey.put(i, tuple.get(partitionKeyIds[i]));
        }
      }

      /** Aggregation State */
      if (hasPartitionKeys && (lastKey == null || lastKey.equals(currentKey))) {
        if (lastKey == null) {
          for(int i = 0; i < aggFunctionsNum; i++) {
            contexts[i] = aggFunctions[i].newContext();
            aggFunctions[i].merge(contexts[i], inSchema, tuple);
          }
          lastKey = currentKey;
        } else {
          // aggregate
          for (int i = 0; i < aggFunctionsNum; i++) {
            aggFunctions[i].merge(contexts[i], inSchema, tuple);
          }
        }

      } else { /** Finalization State */
        // finalize aggregate and return
        outputTuple = new VTuple(outSchema.size());
        int columnIdx = 0;


        for(; columnIdx < nonAggregatedColumnNum; columnIdx++) {
          outputTuple.put(columnIdx, tuple.get(nonAggregatedColumns[columnIdx]));
        }

        for(int aggFuncIdx = 0; aggFuncIdx < aggFunctionsNum; columnIdx++, aggFuncIdx++) {
          outputTuple.put(columnIdx, aggFunctions[aggFuncIdx].terminate(contexts[aggFuncIdx]));
        }

        for(int evalIdx = 0; evalIdx < aggFunctionsNum; evalIdx++) {
          contexts[evalIdx] = aggFunctions[evalIdx].newContext();
          aggFunctions[evalIdx].merge(contexts[evalIdx], inSchema, tuple);
        }

        lastKey = currentKey;
        return outputTuple;
      }
    } // while loop

    if (!finished) {
      outputTuple = new VTuple(outSchema.size());
      int tupleIdx = 0;
      for(; tupleIdx < partitionKeyNum; tupleIdx++) {
        outputTuple.put(tupleIdx, lastKey.get(tupleIdx));
      }
      for(int aggFuncIdx = 0; aggFuncIdx < aggFunctionsNum; tupleIdx++, aggFuncIdx++) {
        outputTuple.put(tupleIdx, aggFunctions[aggFuncIdx].terminate(contexts[aggFuncIdx]));
      }
      finished = true;
    }
    return outputTuple;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastKey = null;
    finished = false;
  }
}
