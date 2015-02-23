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

package org.apache.tajo.storage.text;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.datetime.DateTimeUtil;

import java.io.IOException;

public class ApacheLogLineSerDe extends TextLineSerDe {
  private static final Log LOG = LogFactory.getLog(ApacheLogLineSerDe.class);

  public final Schema fakeSchema;
  private static final int [] fakeProjectIds = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23};

  public ApacheLogLineSerDe() {
    fakeSchema = new Schema();
    fakeSchema.addColumn("col1", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col2", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col3", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col4", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col5", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col6", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col7", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col8", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col9", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col10", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col11", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col12", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col13", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col14", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col15", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col16", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col17", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col18", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col19", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col20", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col21", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col22", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col23", TajoDataTypes.Type.TEXT);
    fakeSchema.addColumn("col24", TajoDataTypes.Type.TEXT);
  }

  @Override
  public TextLineDeserializer createDeserializer(Schema schema, TableMeta meta, int[] targetColumnIndexes) {
    return new ApacheLogDeserializer(fakeSchema, meta, fakeProjectIds, targetColumnIndexes);
  }

  @Override
  public TextLineSerializer createSerializer(Schema schema, TableMeta meta) {
    return new CSVLineSerializer(schema, meta);
  }

  public static class ApacheLogDeserializer extends CSVLineDeserializer {
    int [] actualTargetColumns;
    public ApacheLogDeserializer(Schema schema, TableMeta meta, int[] targetColumnIndexes, int [] actualColumnIds) {
      super(schema, meta, targetColumnIndexes);
      this.actualTargetColumns = actualColumnIds;
    }

    @Override
    public void init() {
      super.init();
    }

    Tuple tuple = new VTuple(24);

    @Override
    public void deserialize(ByteBuf buf, Tuple output) throws IOException, TextLineParsingError {
      super.deserialize(buf, tuple);

      for (int i = 0; i < actualTargetColumns.length; i++) {
        output.put(i, getField(tuple, actualTargetColumns[i]));
      }
    }

    public static Datum getField(Tuple tuple, int idx) throws TextLineParsingError {
      if (tuple.isNull(idx)) {
        return NullDatum.get();
      }

      switch (idx) {
      case 0:
        return tuple.get(0); // remote address
      case 1:
        return tuple.get(1);
      case 2:
        return tuple.get(2);
      case 3:
        return new TimestampDatum(DateTimeUtil.toJulianTimestamp(tuple.get(3).asChars().substring(1))); // timestamp
      case 4:
        return DatumFactory.createText(tuple.get(5).asChars().substring(1)); // HTTP request method
      case 5:
        return tuple.get(6); // HTTP request path and parameters
      case 6:
        String httpVersion = tuple.getText(7);
        try {
          return DatumFactory.createText(httpVersion.substring(0, httpVersion.length() - 1)); // HTTP version
        } catch (Throwable t) {
          return NullDatum.get();
        }
      case 7:
        try {
          int num = Integer.parseInt(tuple.getText(8));
          return DatumFactory.createInt4(num); // HTTP response code (e.g., 200, 431, ..)
        } catch (Throwable t) {
          return NullDatum.get();
        }
      case 8:
        try {
          int num = Integer.parseInt(tuple.getText(9));
          return DatumFactory.createInt4(num); // HTTP Transferred bytes
        } catch (Throwable t) {
          return NullDatum.get();
        }
      case 9:
        String some = tuple.get(10).asChars();
        try {
          return DatumFactory.createText(some.substring(1, some.length() - 1)); // -
        } catch (Throwable t) {
          return NullDatum.get();
        }
      case 10:
        String agent = mergeRemains(tuple, 11);
        try {
          return DatumFactory.createText(agent.substring(1, agent.length() - 2)); // HTTP Agent
        } catch (Throwable t) {
          return NullDatum.get();
        }
      default:
        throw new RuntimeException("Out of index at ApacheLogLineSerDe");
      }
    }

    @Override
    public void release() {
      super.release();
    }
  }

  public static String mergeRemains(Tuple tuple, int startIndex) {
    StringBuilder builder = new StringBuilder();
    for (int i = startIndex; i < tuple.size() && tuple.isNotNull(i); i++) {
      builder.append(tuple.getText(i)).append(" ");
    }
    return builder.toString();
  }
}
