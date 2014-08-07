package org.apache.tajo.engine.eval;

import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.json.GsonObject;

public class EvalTree implements GsonObject {
  @Expose public EvalNode root;

  public EvalTree(EvalNode root) {
    this.root = root;
  }

  public EvalNode getRoot() {
    return this.root;
  }

  public void setRoot(EvalNode root) {
    this.root = root;
  }

  @Override
  public String toString() {
    return root.toString();
  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, EvalTree.class);
  }
}
