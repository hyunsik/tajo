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
package org.apache.tajo.catalog;


import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.OpType;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionPredicateProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.json.GsonObject;

public class PartitionPredicate implements ProtoObject<PartitionPredicateProto>, Cloneable, GsonObject {

  @Expose
  protected String columnName;
  @Expose
  String partitionValue;
  @Expose
  protected TajoDataTypes.DataType dataType;
  @Expose
  protected OpType operationType;

  public PartitionPredicate(String columnName, String partitionValue, TajoDataTypes.DataType dataType, OpType operationType) {
    this.columnName = columnName;
    this.partitionValue = partitionValue;
    this.dataType = dataType;
    this.operationType = operationType;
  }

  public PartitionPredicate(PartitionPredicateProto partitionPredicateProto) {
    columnName =   partitionPredicateProto.getColumnName();
    partitionValue = partitionPredicateProto.getPartitionValue();
    dataType = partitionPredicateProto.getDataType();
    operationType = partitionPredicateProto.getPartitionOpType();

  }
  @Override
  public PartitionPredicateProto getProto() {
    return PartitionPredicateProto.newBuilder().setColumnName(this.columnName)
        .setPartitionValue(partitionValue)
        .setDataType(this.dataType)
        .setPartitionOpType(operationType)
        .build();
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, PartitionPredicate.class);
  }

  @Override
  public String toString() {
    return columnName + " " + operationType + " " + partitionValue;
  }
}
