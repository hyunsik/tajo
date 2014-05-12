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

package org.apache.tajo.datum;

import com.google.gson.annotations.Expose;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.NumberUtil;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;


public class NumericDatum extends NumberDatum {
  private static final MathContext DEFAULT_CONTEXT = MathContext.DECIMAL64;
  private static final int DEFAULT_SCALE = 0;
  private static final BigDecimal INT2_MAX = new BigDecimal(Short.MAX_VALUE);

  @Expose private final BigDecimal value;

	public NumericDatum(long val) {
    super(TajoDataTypes.Type.NUMERIC);
		value = new BigDecimal(val, DEFAULT_CONTEXT);
    value.setScale(DEFAULT_SCALE);
	}

  public NumericDatum(String val) {
    super(TajoDataTypes.Type.NUMERIC);
    value = new BigDecimal(val, DEFAULT_CONTEXT);
    value.setScale(DEFAULT_SCALE);
  }

  public NumericDatum(BigDecimal val) {
    super(TajoDataTypes.Type.NUMERIC);
    this.value = val;
  }

  @Override
	public boolean asBool() {
		throw new InvalidCastException();
	}

  @Override
  public char asChar() {
    return (char) value.byteValue();
  }
	
	@Override
	public short asInt2() {
		return value.shortValue();
	}

  @Override
	public int asInt4() {
		return value.intValue();
	}

  @Override
	public long asInt8() {
		return value.longValue();
	}

  @Override
	public byte asByte() {
		return value.byteValue();
	}

  @Override
	public byte [] asByteArray() {
		return value.unscaledValue().toByteArray();
	}

  @Override
	public float asFloat4() {
		return value.floatValue();
	}

  @Override
	public double asFloat8() {
		return value.doubleValue();
	}

  @Override
	public String asChars() {
		return ""+value.toPlainString();
	}

  @Override
  public byte[] asTextBytes() {
    return value.toPlainString().getBytes();
  }

  @Override
  public int size() {
    return size;
  }
  
  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof NumericDatum) {
      NumericDatum other = (NumericDatum) obj;
      return value.equals(other.value);
    }
    
    return false;
  }

  @Override
  public Datum equalsTo(Datum datum) {
    switch (datum.type()) {
      case INT2:
        return DatumFactory.createBool(asInt2() == datum.asInt2());
      case INT4:
        return DatumFactory.createBool(asInt4() == datum.asInt4());
      case INT8:
        return DatumFactory.createBool(asInt8() == datum.asInt8());
      case FLOAT4:
        return DatumFactory.createBool(asFloat4() == datum.asFloat4());
      case FLOAT8:
        return DatumFactory.createBool(asFloat8() == datum.asFloat8());
      case NUMERIC:
        return DatumFactory.createBool(asFloat8() == datum.asFloat8());
      case NULL_TYPE:
        return datum;
      default:
        throw new InvalidOperationException();
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.isNull()) {
      return -1;
    }

    return value.compareTo(((NumericDatum) datum).value);
  }

  @Override
  public Datum plus(Datum datum) {
    if (datum.isNull()) {
      return datum;
    }

    NumericDatum numeric = (NumericDatum) datum;
    return new NumericDatum(value.add(numeric.value));
  }

  @Override
  public Datum minus(Datum datum) {
    if (datum.isNull()) {
      return datum;
    }

    NumericDatum numeric = (NumericDatum) datum;
    return new NumericDatum(value.subtract(numeric.value));
  }

  @Override
  public Datum multiply(Datum datum) {
    if (datum.isNull()) {
      return datum;
    }

    NumericDatum numeric = (NumericDatum) datum;
    return new NumericDatum(value.multiply(numeric.value));
  }

  @Override
  public Datum divide(Datum datum) {
    if (datum.isNull()) {
      return datum;
    }

    NumericDatum numeric = (NumericDatum) datum;
    return new NumericDatum(value.divide(numeric.value));
  }

  @Override
  public Datum modular(Datum datum) {
    if (datum.isNull()) {
      return datum;
    }

    NumericDatum numeric = (NumericDatum) datum;
    return new NumericDatum(value.remainder(numeric.value));
  }

  @Override
  public NumberDatum inverseSign() {
    return new NumericDatum(value.unscaledValue().modInverse());
  }
}
