package org.apache.tajo.engine.planner.logical;


import com.google.gson.annotations.Expose;
import org.apache.tajo.algebra.WindowSpecExpr;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.util.TUtil;

public class WindowSpec {
  @Expose private String windowName;

  @Expose private Column[] partitionKeys;

  @Expose private SortSpec[] orderByKeys;

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
  public boolean equals(Object obj) {
    if (obj instanceof WindowSpec) {
      WindowSpec another = (WindowSpec) obj;
      return TUtil.checkEquals(partitionKeys, another.partitionKeys) &&
          TUtil.checkEquals(orderByKeys, another.orderByKeys) &&
          TUtil.checkEquals(windowFrame, another.windowFrame);
    } else {
      return false;
    }
  }

  public static class WindowFrame {
    WindowSpecExpr.WindowFrameUnit unit;
    private WindowStartBound startBound;
    private WindowEndBound endBound;

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
