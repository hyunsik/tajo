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

package org.apache.tajo.engine.eval;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.algebra.WindowSpecExpr.*;

public class WindowOverEval extends EvalNode {

  @Expose private WindowFunctionEval function;

  @Expose private Column[] partitionKeys;

  @Expose private SortSpec [] orderByKeys;

  @Expose private WindowFrame windowFrame;

  public WindowOverEval(EvalType type) {
    super(type);
  }

  public boolean hasPartitionKeys() {
    return partitionKeys != null;
  }

  public Column [] getPartitionKeys() {
    return partitionKeys;
  }

  public boolean hasOrderBy() {
    return orderByKeys != null;
  }

  public SortSpec [] getOrderByKeys() {
    return orderByKeys;
  }

  public boolean hasWindowFrame() {
    return windowFrame != null;
  }

  public WindowFrame getWindowFrame() {
    return windowFrame;
  }

  @Override
  public TajoDataTypes.DataType getValueType() {
    return function.getValueType();
  }

  @Override
  public String getName() {
    return function.getName();
  }

  @Override
  public <T extends Datum> T eval(Schema schema, Tuple tuple) {
    throw new UnsupportedOperationException("Cannot execute eval() of OVER clause");
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof WindowOverEval) {
      WindowOverEval another = (WindowOverEval) obj;
      return TUtil.checkEquals(function, another.function) &&
          TUtil.checkEquals(partitionKeys, another.partitionKeys) &&
          TUtil.checkEquals(orderByKeys, another.orderByKeys) &&
          TUtil.checkEquals(windowFrame, another.windowFrame);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(function, partitionKeys, orderByKeys, windowFrame);
  }

  public static class WindowFrame {
    WindowFrameUnit unit;
    private WindowStartBound startBound;
    private WindowEndBound endBound;

    public WindowFrame(WindowFrameUnit unit, WindowStartBound startBound) {
      this.unit = unit;
      this.startBound = startBound;
    }

    public WindowFrame(WindowFrameUnit unit, WindowStartBound startBound, WindowEndBound endBound) {
      this(unit, startBound);
      this.endBound = endBound;
    }

    public WindowStartBound getStartBound() {
      return startBound;
    }

    public boolean hasEndBound() {
      return endBound != null;
    }

    public WindowEndBound getEndBound() {
      return endBound;
    }
  }

  public static class WindowStartBound {
    private WindowFrameStartBoundType boundType;
    private EvalNode number;

    public WindowStartBound(WindowFrameStartBoundType type) {
      this.boundType = type;
    }

    public WindowFrameStartBoundType getBoundType() {
      return boundType;
    }

    public void setNumber(EvalNode number) {
      this.number = number;
    }

    public EvalNode getNumber() {
      return number;
    }
  }

  public static class WindowEndBound {
    private WindowFrameEndBoundType boundType;
    private EvalNode number;

    public WindowEndBound(WindowFrameEndBoundType type) {
      this.boundType = type;
    }

    public WindowFrameEndBoundType getBoundType() {
      return boundType;
    }

    public EvalNode setNumber(EvalNode number) {
      return number;
    }

    public EvalNode getNumber() {
      return number;
    }
  }
}
