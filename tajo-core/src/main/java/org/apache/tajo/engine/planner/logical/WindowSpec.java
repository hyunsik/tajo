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

package org.apache.tajo.engine.planner.logical;


import com.google.gson.annotations.Expose;
import org.apache.tajo.algebra.WindowSpecExpr;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.util.TUtil;

import java.util.Comparator;

public class WindowSpec {
  @Expose private String windowName;

  @Expose private Column[] partitionKeys;

  @Expose private WindowFrame windowFrame;

  public String getWindowName() {
    return windowName;
  }

  public boolean hasPartitionKeys() {
    return partitionKeys != null;
  }

  public Column [] getPartitionKeys() {
    return partitionKeys;
  }

  public boolean hasWindowFrame() {
    return windowFrame != null;
  }

  public WindowFrame getWindowFrame() {
    return windowFrame;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof WindowSpec) {
      WindowSpec another = (WindowSpec) obj;
      return TUtil.checkEquals(partitionKeys, another.partitionKeys) &&

          TUtil.checkEquals(windowFrame, another.windowFrame);
    } else {
      return false;
    }
  }

  public static class WindowFrame {
    @Expose WindowSpecExpr.WindowFrameUnit unit;
    @Expose private WindowStartBound startBound;
    @Expose private WindowEndBound endBound;

    public WindowFrame(WindowSpecExpr.WindowFrameUnit unit, WindowStartBound startBound) {
      this.unit = unit;
      this.startBound = startBound;
    }

    public WindowFrame(WindowSpecExpr.WindowFrameUnit unit, WindowStartBound startBound, WindowEndBound endBound) {
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
    private WindowSpecExpr.WindowFrameStartBoundType boundType;
    private EvalNode number;

    public WindowStartBound(WindowSpecExpr.WindowFrameStartBoundType type) {
      this.boundType = type;
    }

    public WindowSpecExpr.WindowFrameStartBoundType getBoundType() {
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
    private WindowSpecExpr.WindowFrameEndBoundType boundType;
    private EvalNode number;

    public WindowEndBound(WindowSpecExpr.WindowFrameEndBoundType type) {
      this.boundType = type;
    }

    public WindowSpecExpr.WindowFrameEndBoundType getBoundType() {
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
