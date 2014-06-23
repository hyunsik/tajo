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


package org.apache.tajo.algebra;


import com.google.gson.*;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.lang.reflect.Type;

public abstract class PartitionMethodDescExpr {
  @Expose
  @SerializedName("PartitionType")
  PartitionType type;

  public PartitionMethodDescExpr(PartitionType type) {
    this.type = type;
  }

  public PartitionType getPartitionType() {
    return type;
  }

  static class JsonSerDer implements JsonSerializer<PartitionMethodDescExpr>,
      JsonDeserializer<PartitionMethodDescExpr> {

    @Override
    public PartitionMethodDescExpr deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      PartitionType type = PartitionType.valueOf(jsonObject.get("PartitionType").getAsString());
      switch (type) {
        case RANGE:
          return context.deserialize(json, CreateTable.RangePartition.class);
        case HASH:
          return context.deserialize(json, CreateTable.HashPartition.class);
        case LIST:
          return context.deserialize(json, CreateTable.ListPartition.class);
        case COLUMN:
          return context.deserialize(json, CreateTable.ColumnPartition.class);
        case COL_PRED:
          return  context.deserialize(json,ColumnPredicatePartition.class);
      }
      return null;
    }

    @Override
    public JsonElement serialize(PartitionMethodDescExpr src, Type typeOfSrc, JsonSerializationContext context) {
      switch (src.getPartitionType()) {
        case RANGE:
          return context.serialize(src, CreateTable.RangePartition.class);
        case HASH:
          return context.serialize(src, CreateTable.HashPartition.class);
        case LIST:
          return context.serialize(src, CreateTable.ListPartition.class);
        case COLUMN:
          return context.serialize(src, CreateTable.ColumnPartition.class);
        case COL_PRED:
          return  context.serialize(src,ColumnPredicatePartition.class);
        default:
          return null;
      }
    }
  }
}
