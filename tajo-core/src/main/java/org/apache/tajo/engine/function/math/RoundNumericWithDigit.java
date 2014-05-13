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

package org.apache.tajo.engine.function.math;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.NumericDatum;
import org.apache.tajo.engine.eval.FunctionEval;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.NumberFormat;

/**
 * Function definition
 *
 * NUMERIC round(value NUMERIC, int roundPoint)
 */
@Description(
    functionName = "round",
    description = "Round to s decimalN places.",
    example = "> SELECT round(42.4382, 2)\n"
        + "42.44",
    returnType = TajoDataTypes.Type.NUMERIC,
    paramTypes = {
        @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.INT4}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.NUMERIC, TajoDataTypes.Type.INT4})
    }
)
public class RoundNumericWithDigit extends GeneralFunction {
  public RoundNumericWithDigit() {
    super(new Column[] {
        new Column("value", TajoDataTypes.Type.NUMERIC),
        new Column("roundPoint", TajoDataTypes.Type.INT4)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum valueDatum = params.get(0);
    Datum roundDatum = params.get(1);

    if(valueDatum instanceof NullDatum || roundDatum instanceof  NullDatum) {
      return NullDatum.get();
    }

    BigDecimal numeric = valueDatum.asNumeric();
    int roundPoint = roundDatum.asInt4();


    return DatumFactory.createNumeric(numeric.setScale(roundPoint, NumericDatum.DEFAULT_ROUND_MODE));
  }
}