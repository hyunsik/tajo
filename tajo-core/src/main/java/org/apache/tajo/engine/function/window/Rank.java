package org.apache.tajo.engine.function.window;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Int8Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.function.WindowAggFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "rank",
    description = " The number of rows for "
        + "which the supplied expressions are unique and non-NULL.",
    example = "> SELECT rank() OVER (ORDER BY x) FROM ...;",
    returnType = TajoDataTypes.Type.INT8,
    paramTypes = {@ParamTypes(paramTypes = {})}
)
public final class Rank extends WindowAggFunction {

  public Rank() {
    super(new Column[] {
        new Column("expr", TajoDataTypes.Type.ANY)
    });
  }

  @Override
  public void eval(FunctionContext context, Tuple params) {
    CountDistinctValueContext distinctContext = (CountDistinctValueContext) context;
    Datum value = params.get(0);
    if ((distinctContext.latest == null ||
        (!distinctContext.latest.equals(value)) && !(value instanceof NullDatum))) {
      distinctContext.latest = value;
      distinctContext.count++;
    }
  }

  @Override
  public Int8Datum terminate(FunctionContext ctx) {
    return DatumFactory.createInt8(((CountDistinctValueContext) ctx).count);
  }

  @Override
  public FunctionContext newContext() {
    return new CountDistinctValueContext();
  }

  private class CountDistinctValueContext implements FunctionContext {
    long count = 0;
    Datum latest = null;
  }

  @Override
  public CatalogProtos.FunctionType getFunctionType() {
    return CatalogProtos.FunctionType.WINDOW;
  }
}
