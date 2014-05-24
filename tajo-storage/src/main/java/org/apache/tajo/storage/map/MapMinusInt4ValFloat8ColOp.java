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

package org.apache.tajo.storage.map;

import org.apache.tajo.storage.vector.SizeOf;
import org.apache.tajo.storage.vector.UnsafeUtil;
import org.apache.tajo.storage.vector.VecRowBlock;
import sun.misc.Unsafe;

public class MapMinusInt4ValFloat8ColOp {
  static Unsafe unsafe = UnsafeUtil.unsafe;

  public static void map(int vecNum, long result, VecRowBlock vecRowBlock, int lhsValue, int rhsIdx, int [] selVec) {

    long rhsPtr = vecRowBlock.getValueVecPtr(rhsIdx);

    if (selVec == null) {
      for (int i = 0; i < vecRowBlock.limitedVecSize(); i++) {
        double rhsValue = unsafe.getDouble(rhsPtr);
        unsafe.putDouble(result, lhsValue - rhsValue);

        result += SizeOf.SIZE_OF_LONG;
        rhsPtr += SizeOf.SIZE_OF_LONG;
      }
    } else {
      long selectedPtr;

      for (int i = 0; i < vecRowBlock.limitedVecSize(); i++) {
        selectedPtr = rhsPtr + (selVec[i] * SizeOf.SIZE_OF_LONG);
        double rhsValue = unsafe.getDouble(selectedPtr);
        unsafe.putDouble(result, lhsValue - rhsValue);
        result += SizeOf.SIZE_OF_LONG;
      }
    }
  }
}