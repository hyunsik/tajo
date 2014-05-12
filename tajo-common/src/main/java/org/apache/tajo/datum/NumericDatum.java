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
import org.apache.hadoop.io.WritableUtils;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.ProtoUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

import static org.apache.tajo.common.TajoDataTypes.Type;


public class NumericDatum extends NumberDatum {
  private static final RoundingMode DEFAULT_ROUND_MODE = RoundingMode.HALF_EVEN;
  private static final int DIVIDE_DEFAULT_SCALE = 16;
  public static final int MAX_PRECISION = 38;
  public static final int MAX_SCALE = 38;

  private static final MathContext FLOAT4_CONTEXT = new MathContext(6, DEFAULT_ROUND_MODE);
  private static final MathContext FLOAT8_CONTEXT = new MathContext(15, DEFAULT_ROUND_MODE);

  public static final NumericDatum ZERO = new NumericDatum(BigInteger.ZERO, 0);
  public static final NumericDatum ONE = new NumericDatum(BigInteger.ONE, 0);
  public static final NumericDatum TEN = new NumericDatum(BigInteger.TEN, 0);

  @Expose public final BigDecimal value;

  public NumericDatum(long val) {
    super(Type.NUMERIC);
    value = new BigDecimal(val);
  }

  public NumericDatum(float val) {
    super(Type.NUMERIC);
    value = new BigDecimal(val, FLOAT4_CONTEXT);
  }

  public NumericDatum(double val) {
    super(Type.NUMERIC);
    value = new BigDecimal(val, FLOAT8_CONTEXT);
  }

	public NumericDatum(BigInteger unscaled, int scale) {
    super(Type.NUMERIC);
		value = new BigDecimal(unscaled, scale);
	}

  public NumericDatum(BigDecimal val) {
    super(Type.NUMERIC);
    this.value = val;
  }

  public NumericDatum(BigDecimal val, int scale) {
    super(Type.NUMERIC);
    this.value = val;
    this.value.setScale(scale);
  }

  public NumericDatum(String str, int scale) {
    super(Type.NUMERIC);
    this.value = new BigDecimal(str).setScale(scale, DEFAULT_ROUND_MODE);
  }

  public NumericDatum(String str) {
    super(Type.NUMERIC);
    this.value = new BigDecimal(str);
  }

  public NumericDatum(byte [] bytes) {
    super(Type.NUMERIC);
    int scale = ProtoUtil.readRawVarint32(bytes, 0);
    int scaleBytesLength = ProtoUtil.computeRawVarint32Size(scale);
    byte [] unscaledBytes = new byte[bytes.length - scaleBytesLength];
    System.arraycopy(bytes, scaleBytesLength, unscaledBytes, 0, unscaledBytes.length);
    value = new BigDecimal(new BigInteger(unscaledBytes), scale);
  }

  public int scale() {
    return value.scale();
  }

  public NumericDatum enforceScale(int scale) {
    return new NumericDatum(value.setScale(scale, DEFAULT_ROUND_MODE));
  }

  @Override
	public boolean asBool() {
		throw new InvalidCastException(Type.NUMERIC, Type.BOOLEAN);
	}

  @Override
  public char asChar() {
    throw new InvalidCastException(Type.NUMERIC, Type.CHAR);
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
    throw new InvalidCastException(Type.NUMERIC, Type.BIT);
	}

  @Override
	public byte [] asByteArray() {
    BigInteger unscaledValue = value.unscaledValue();
    int unscaledValueByteLength = unscaledValue.bitLength() / 8 + 1;
    int scaleBytesLength = ProtoUtil.computeRawVarint32Size(scale());
    byte [] bytes = new byte[scaleBytesLength + unscaledValueByteLength];
    ProtoUtil.writeRawVarint32(scale(), bytes, 0);
    System.arraycopy(unscaledValue.toByteArray(), 0, bytes, scaleBytesLength, unscaledValueByteLength);
    return bytes;
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
    int unscaledByteLength = value.unscaledValue().bitLength() / 8 + 1;
    return unscaledByteLength + ProtoUtil.computeRawVarint32Size(unscaledByteLength);
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
    return new NumericDatum(value.divide(numeric.value, DIVIDE_DEFAULT_SCALE, DEFAULT_ROUND_MODE));
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
    return new NumericDatum(value.negate());
  }
}
