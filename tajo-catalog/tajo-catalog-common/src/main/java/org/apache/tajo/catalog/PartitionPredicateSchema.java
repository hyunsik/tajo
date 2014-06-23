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
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;

import java.util.ArrayList;
import java.util.List;

public class PartitionPredicateSchema implements ProtoObject<CatalogProtos.PartitionPredicateSchemaProto>, Cloneable, GsonObject {

  private CatalogProtos.PartitionPredicateSchemaProto.Builder builder = CatalogProtos.PartitionPredicateSchemaProto.newBuilder();

  @Expose
  protected List<PartitionPredicate> partitionPredicates = null;

  public PartitionPredicateSchema() {
    init();
  }

  public PartitionPredicateSchema (CatalogProtos.PartitionPredicateSchemaProto proto ) {
    this.partitionPredicates = new ArrayList<PartitionPredicate>();
    for( CatalogProtos.PartitionPredicateProto partitionPredicateProto : proto.getPredicatesList()) {
      partitionPredicates.add(new PartitionPredicate(partitionPredicateProto));
    }

  }


  private void init() {
    partitionPredicates = new ArrayList<PartitionPredicate>();
  }

  public List<PartitionPredicate> getPartitionPredicates() {
    return partitionPredicates;
  }

  public void addPredicate(PartitionPredicate predicate) {
    partitionPredicates.add(predicate);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    PartitionPredicateSchema schema = null;
    schema = (PartitionPredicateSchema) super.clone();
    schema.builder = CatalogProtos.PartitionPredicateSchemaProto.newBuilder();
    schema.init();
    for (PartitionPredicate partitionPredicate : schema.getPartitionPredicates()) {
        schema.addPredicate(partitionPredicate);
    }
    return schema;
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, PartitionPredicateSchema.class);
  }

  @Override
  public CatalogProtos.PartitionPredicateSchemaProto getProto() {
    builder.clearPredicates();
    if (this.partitionPredicates != null) {
      for (PartitionPredicate partitionPredicate : partitionPredicates) {
        builder.addPredicates(partitionPredicate.getProto());
      }
    }
    return builder.build();
  }
}
