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

package org.apache.tajo.storage.json;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.net.util.Base64;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.schema.Field;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.storage.text.TextLineParsingError;
import org.apache.tajo.type.Array;
import org.apache.tajo.type.Record;
import org.apache.tajo.type.Type;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.CharsetDecoder;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

public class JsonLineDeserializer extends TextLineDeserializer {
  private JSONParser parser;

  // Full Path -> Type
  private final Map<String, org.apache.tajo.type.Type> types;
  private final String [] projectedPaths;
  private final CharsetDecoder decoder = CharsetUtil.getDecoder(CharsetUtil.UTF_8);

  private final TimeZone timezone;

  public JsonLineDeserializer(Schema schema, TableMeta meta, Column [] projected) {
    super(schema, meta);

    projectedPaths = SchemaUtil.convertColumnsToPaths(Lists.newArrayList(projected), false);
    types = SchemaUtil.buildTypeMap(schema.getAllColumns(), projectedPaths);

    timezone = TimeZone.getTimeZone(meta.getProperty(StorageConstants.TIMEZONE,
        StorageUtil.TAJO_CONF.getSystemTimezone().getID()));
  }

  @Override
  public void init() {
    parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR);
  }

  private Datum extractField(Type type, Object object,
                             @Nullable String fullPath, @Nullable String [] pathElements, @Nullable int depth) {
    if (object == null) {
      return NullDatum.get();
    }

    switch (type.kind()) {
    case BOOLEAN:
      if (object != null) {
        return DatumFactory.createBool(object.equals("true"));
      } else {
        return NullDatum.get();
      }

    case CHAR:
        return DatumFactory.createChar((String) object);

    case INT1:
    case INT2:
      Number int2Num = ((Number)object);
      return DatumFactory.createInt2(int2Num.shortValue());

    case INT4:
      Number int4Num = ((Number)object);
      return DatumFactory.createInt4(int4Num.intValue());

    case INT8:
      Number int8Num = ((Number)object);
      return DatumFactory.createInt8(int8Num.longValue());

    case FLOAT4:
      Number float4Num = ((Number)object);
      return DatumFactory.createFloat4(float4Num.floatValue());

    case FLOAT8:
      Number float8Num = ((Number)object);
      return DatumFactory.createFloat8(float8Num.doubleValue());

    case TEXT:
      String textStr = (String)object;
      return DatumFactory.createText(textStr);

    case TIMESTAMP:
      String timestampStr = (String)object;
      return DatumFactory.createTimestamp(timestampStr, timezone);

    case TIME:
      String timeStr = (String)object;
      return DatumFactory.createTime(timeStr);

    case DATE:
      String dateStr = (String)object;
      return DatumFactory.createDate(dateStr);

    case BIT:
    case BINARY:
    case VARBINARY:
    case BLOB: {
      return DatumFactory.createBlob(Base64.decodeBase64((String) object));
    }

    case ARRAY:
      JSONArray arrayObject = (JSONArray) object;
      Array arrayType = (Array) type;

      // TODO - Array Index operation can be pushed down to here. We need to support it later.
      if (false) {

        return extractField(arrayType.elementType(), arrayObject.get(0), fullPath, null, depth + 1);
      } else {
        ImmutableList.Builder<Datum> builder = ImmutableList.builder();
        for (int i = 0; i < arrayObject.size(); i++) {
          builder.add(extractField(arrayType.elementType(), arrayObject.get(i), fullPath, null, depth + 1));
        }
        return new ArrayDatum(type, builder.build());
      }

    case RECORD:
      JSONObject nestedObject = (JSONObject)object;
      Record recordType = (Record) type;

      if (!isLeaf(pathElements, depth)) {
        final String fieldName = pathElements[depth + 1];
        final String newPath = fullPath + "/" + fieldName;
        final Type fieldType = types.get(newPath);
        return extractField(fieldType, getTypeObject(fieldType, nestedObject, fieldName), newPath, pathElements, depth + 1);
      } else {
        ImmutableList.Builder<Datum> b = ImmutableList.builder();
        for (Field field : recordType.fields()) {
          final String newPath = fullPath + "/" + field.name().interned();
          b.add(extractField(field.type(), nestedObject.get(field.name().interned()), newPath, pathElements, depth + 1));
        }
        return new RecordDatum(type, b.build());
      }

    case NULL_TYPE:
      return NullDatum.get();

    default:
      throw new TajoRuntimeException(new NotImplementedException("" + type + " for json"));
    }
  }

  private static final boolean isLeaf(String [] pathElement, int depth) {
    return pathElement.length - 1 == depth;
  }

  public Object getTypeObject(Type type, JSONObject json, String key) {
    TajoDataTypes.Type kind = type.kind();
    if (INT1.getNumber() <= kind.getNumber() && kind.getNumber() <= NUMERIC.getNumber()) {
      return json.getAsNumber(key);
    } else if (kind == RECORD || kind == ARRAY) {
      return json.get(key);
    } else {
      return json.getAsString(key);
    }
  }

  @Override
  public void deserialize(ByteBuf buf, Tuple output) throws IOException, TextLineParsingError {
    String line = decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString();

    JSONObject object;
    try {
      object = (JSONObject) parser.parse(line);
    } catch (ParseException pe) {
      throw new TextLineParsingError(line, pe);
    } catch (ArrayIndexOutOfBoundsException ae) {
      // truncated value
      throw new TextLineParsingError(line, ae);
    }

    for (int i = 0; i < projectedPaths.length; i++) {
      final String [] paths = projectedPaths[i].split(NestedPathUtil.PATH_DELIMITER);
      final Type type = types.get(paths[0]);
      final Object field = getTypeObject(type, object, paths[0]);
      output.put(i, extractField(type, field, paths[0], paths, 0));
    }
  }

  @Override
  public void release() {
  }
}
