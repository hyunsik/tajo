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

package org.apache.tajo.catalog.partition;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.PartitionPredicateSchema;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.TUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * <code>PartitionPredicateMethodDesc</code> presents a table description, including partition type, and partition keys.
 */
public class PartitionPredicateMethodDesc implements ProtoObject<CatalogProtos.PartitionMethodProto>, Cloneable, GsonObject {
  private CatalogProtos.PartitionMethodProto.Builder builder;

  @Expose private String databaseName;                         // required
  @Expose private String tableName;                            // required
  @Expose private CatalogProtos.PartitionType partitionType;   // required
  @Expose private String expression;                           // required
  @Expose private PartitionPredicateSchema partitionPredicateSchema; // required
  @Expose private Map<String, String> partitionProperties;     // optional

  public PartitionPredicateMethodDesc() {
    builder = CatalogProtos.PartitionMethodProto.newBuilder();
  }



  public PartitionPredicateMethodDesc(String databaseName, String tableName,
                             CatalogProtos.PartitionType partitionType, String expression,
                             PartitionPredicateSchema partitionPredicateSchema) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionType = partitionType;
    this.expression = expression;
    this.partitionPredicateSchema = partitionPredicateSchema;
  }

  public PartitionPredicateMethodDesc(CatalogProtos.PartitionMethodProto proto) {
    this(proto.getTableIdentifier().getDatabaseName(),
        proto.getTableIdentifier().getTableName(),
        proto.getPartitionType(), proto.getExpression(),
        new PartitionPredicateSchema(proto.getPartitionPredicateSchema()));

    if (null != proto.getPartitionProperties()) {
      final List<PrimitiveProtos.KeyValueProto> keyValueProtos = proto.getPartitionProperties().getKeyvalList();
      if (null != proto.getPartitionProperties() && null != keyValueProtos && keyValueProtos.size() > 0) {
        this.partitionProperties = new HashMap<String, String>();
        for (PrimitiveProtos.KeyValueProto currentKeyValueProto : keyValueProtos) {
          this.partitionProperties.put(currentKeyValueProto.getKey(), currentKeyValueProto.getValue());
        }
      }
    }
  }


  public String getTableName() {
    return tableName;
  }

  public String getExpression() {
    return expression;
  }

  public PartitionPredicateSchema getExpressionSchema() {
    return partitionPredicateSchema;
  }

  public CatalogProtos.PartitionType getPartitionType() {
    return partitionType;
  }

  public void setTableName(String tableId) {
    this.tableName = tableId;
  }

  public void setExpressionSchema(PartitionPredicateSchema expressionSchema) {
    this.partitionPredicateSchema = expressionSchema;
  }

  public void setPartitionType(CatalogProtos.PartitionType partitionsType) {
    this.partitionType = partitionsType;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public Map<String, String> getPartitionProperties() {
    return partitionProperties;
  }

  public void setPartitionProperties(Map<String, String> partitionProperties) {
    this.partitionProperties = partitionProperties;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PartitionPredicateMethodDesc) {
      PartitionPredicateMethodDesc other = (PartitionPredicateMethodDesc) object;
      boolean eq = tableName.equals(other.tableName);
      eq = eq && partitionType.equals(other.partitionType);
      eq = eq && expression.equals(other.expression);
      eq = eq && TUtil.checkEquals(partitionPredicateSchema, other.partitionPredicateSchema);
      eq = eq && TUtil.checkEquals(partitionProperties, other.partitionProperties);
      return eq;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, partitionType, expression, partitionPredicateSchema);
  }

  @Override
  public CatalogProtos.PartitionMethodProto getProto() {
    if (builder == null) {
      builder = CatalogProtos.PartitionMethodProto.newBuilder();
    }

    CatalogProtos.TableIdentifierProto.Builder tableIdentifierBuilder = CatalogProtos.TableIdentifierProto.newBuilder();
    if (databaseName != null) {
      tableIdentifierBuilder.setDatabaseName(databaseName);
    }
    if (tableName != null) {
      tableIdentifierBuilder.setTableName(tableName);
    }

    CatalogProtos.PartitionMethodProto.Builder builder = CatalogProtos.PartitionMethodProto.newBuilder();
    builder.setTableIdentifier(tableIdentifierBuilder.build());
    builder.setPartitionType(partitionType);
    builder.setExpression(expression);
    builder.setPartitionPredicateSchema(partitionPredicateSchema.getProto());
    if (null != partitionProperties && partitionProperties.size() > 0) {
      builder.setPartitionProperties(hashMapToKeyValueSetProto(partitionProperties));
    }
    return builder.build();
  }

  private PrimitiveProtos.KeyValueSetProto hashMapToKeyValueSetProto(final Map<String, String> partitionProperties) {
    PrimitiveProtos.KeyValueSetProto.Builder setBuilder = PrimitiveProtos.KeyValueSetProto.newBuilder();
    PrimitiveProtos.KeyValueProto.Builder builder = PrimitiveProtos.KeyValueProto.newBuilder();
    for (Map.Entry<String, String> entry : partitionProperties.entrySet()) {
      builder.setKey(entry.getKey());
      builder.setValue(entry.getValue());
      setBuilder.addKeyval(builder.build());
    }
    return setBuilder.build();
  }

  public Object clone() throws CloneNotSupportedException {
    PartitionPredicateMethodDesc desc = (PartitionPredicateMethodDesc) super.clone();
    desc.builder = builder;
    desc.tableName = tableName;
    desc.partitionType = partitionType;
    desc.expression = expression;
    desc.partitionPredicateSchema = (PartitionPredicateSchema) partitionPredicateSchema.clone();
    desc.partitionProperties = partitionProperties;
    return desc;
  }

  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().
        excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, PartitionPredicateMethodDesc.class);
  }

  public static PartitionPredicateMethodDesc fromJson(String strVal) {
    return strVal != null ? CatalogGsonHelper.fromJson(strVal, PartitionPredicateMethodDesc.class) : null;
  }
}
