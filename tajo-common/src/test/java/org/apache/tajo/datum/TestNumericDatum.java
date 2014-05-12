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

import junit.framework.TestCase;

public class TestNumericDatum extends TestCase {

  public void testStringCtor() throws Exception {
    NumericDatum numeric1 = new NumericDatum("12345.454", 2);
    NumericDatum numeric2 = new NumericDatum("12345.455", 2);
    NumericDatum numeric3 = new NumericDatum("12345.456", 2);
    assertEquals("12345.45", numeric1.asChars());
    assertEquals("12345.46", numeric2.asChars());
    assertEquals("12345.46", numeric3.asChars());
  }

  public void testFloat4Ctor() throws Exception {
    NumericDatum numeric = new NumericDatum(12.3456789f);
    assertEquals("12.3457", numeric.asChars());

    NumericDatum numeric2 = new NumericDatum(123.45678f);
    assertEquals("123.457", numeric2.asChars());
  }

  public void testFloat8Ctor() throws Exception {
    NumericDatum numeric = new NumericDatum(123.456789123457d);
    assertEquals("123.456789123457", numeric.asChars());
  }

  public void testAsInt2() throws Exception {
    NumericDatum numeric = new NumericDatum(12345);
    assertEquals(12345, numeric.asInt2());
    assertEquals("12345", numeric.asChars());
  }

  public void testAsInt4() throws Exception {
    NumericDatum numeric = new NumericDatum(12345);
    assertEquals(12345, numeric.asInt4());
    assertEquals("12345", numeric.asChars());
  }

  public void testAsInt8() throws Exception {
    NumericDatum numeric = new NumericDatum(1234567890);
    assertEquals(1234567890, numeric.asInt4());
  }

  public void testAsByte() throws Exception {
    NumericDatum numeric = new NumericDatum(1234567890);
    assertEquals(1234567890, numeric.asInt4());
  }

  public void testAsByteArray() throws Exception {
    NumericDatum numeric = new NumericDatum("1234567890.12345");
    assertEquals(numeric, new NumericDatum(numeric.asByteArray()));
  }

  public void testAsFloat4() throws Exception {
    NumericDatum numeric = new NumericDatum("12345.123");
    assertEquals(12345.123f, numeric.asFloat4());
    assertEquals("12345.123", numeric.asChars());
  }

  public void testAsFloat8() throws Exception {
    NumericDatum numeric = new NumericDatum("12345.1234567890");
    assertEquals(12345.1234567890d, numeric.asFloat8());
    assertEquals("12345.1234567890", numeric.asChars());
  }

  public void testSize() throws Exception {
    NumericDatum numeric = new NumericDatum("1234567890.12345");
    assertEquals(numeric.size(), numeric.asByteArray().length);
  }

  public void testHashCode() throws Exception {
    NumericDatum numeric1 = new NumericDatum("1234567890.12345");
    NumericDatum numeric2 = new NumericDatum("1234567890.12345");
    NumericDatum numeric3 = new NumericDatum("1234567890.123450");

    assertEquals(numeric1.hashCode(), numeric2.hashCode());
    assertNotSame(numeric1.hashCode(), numeric3.hashCode());
  }

  public void testEquals() throws Exception {
    NumericDatum numeric1 = new NumericDatum("1234567890.12345");
    NumericDatum numeric2 = new NumericDatum("1234567890.12345");
    NumericDatum numeric3 = new NumericDatum("1234567890.123450");

    assertTrue(numeric1.equals(numeric2));
    assertTrue(numeric2.equals(numeric1));
    assertFalse(numeric1.equals(numeric3));
    assertFalse(numeric3.equals(numeric1));
  }

  public void testEqualsTo() throws Exception {
    NumericDatum numeric1 = new NumericDatum("1234567890.12345");
    NumericDatum numeric2 = new NumericDatum("1234567890.12345");
    NumericDatum numeric3 = new NumericDatum("1234567890.123450");
    assertTrue(numeric1.equalsTo(numeric2).asBool());
    assertTrue(numeric1.equalsTo(numeric3).asBool());
  }

  public void testCompareTo() throws Exception {
    NumericDatum numeric1 = new NumericDatum("1234567890.12345");
    NumericDatum numeric2 = new NumericDatum("1234567890.123451");
    NumericDatum numeric3 = new NumericDatum("1234567890.12345");
    assertTrue(numeric1.compareTo(numeric2) < 0);
    assertTrue(numeric2.compareTo(numeric1) > 0);
    assertTrue(numeric1.compareTo(numeric3) == 0);
    assertTrue(numeric3.compareTo(numeric1) == 0);
  }

  public void testPlus() throws Exception {
    NumericDatum numeric1 = new NumericDatum("100.10");
    NumericDatum numeric2 = new NumericDatum("128.200");
    assertEquals(new NumericDatum("228.300"), numeric1.plus(numeric2));
  }

  public void testMinus() throws Exception {
    NumericDatum numeric1 = new NumericDatum("100.10");
    NumericDatum numeric2 = new NumericDatum("128.200");
    assertEquals(new NumericDatum("-28.100"), numeric1.minus(numeric2));
  }

  public void testMultiply() throws Exception {
    NumericDatum numeric1 = new NumericDatum("100.10");
    NumericDatum numeric2 = new NumericDatum("128.200");
    assertEquals(new NumericDatum("12832.82000"), numeric1.multiply(numeric2));
  }

  public void testDivide() throws Exception {
    NumericDatum numeric1 = new NumericDatum("128.110");
    NumericDatum numeric2 = new NumericDatum("2");
    assertEquals(new NumericDatum("64.0550000000000000"), numeric1.divide(numeric2));


    NumericDatum numeric3 = new NumericDatum("36.20");
    NumericDatum numeric4 = new NumericDatum("3");
    assertEquals(new NumericDatum("12.0666666666666667"), numeric3.divide(numeric4));
  }

  public void testModular() throws Exception {
    NumericDatum numeric1 = new NumericDatum("36.20");
    NumericDatum numeric2 = new NumericDatum("3");
    assertEquals(new NumericDatum("0.20"), numeric1.modular(numeric2));
  }

  public void testInverseSign() throws Exception {
    NumericDatum numeric1 = new NumericDatum("36.20");
    NumericDatum numeric2 = new NumericDatum("3");
    assertEquals(new NumericDatum("-36.20"), numeric1.inverseSign());
    assertEquals(new NumericDatum("-3"), numeric2.inverseSign());
  }
}
