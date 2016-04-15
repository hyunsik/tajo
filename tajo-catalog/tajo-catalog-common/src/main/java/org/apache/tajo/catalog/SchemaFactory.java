/*
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

import org.apache.tajo.catalog.proto.CatalogProtos;

public class SchemaFactory {

  public static SchemaBuilder builder() {
    return SchemaBuilder.builder();
  }

  public static Schema newV1() {
    return new SchemaLegacy();
  }

  public static Schema newV1(CatalogProtos.SchemaProto proto) {
    return new SchemaLegacy(proto);
  }

  public static Schema newV1(Schema schema) {
    return builder().addAll(schema.getRootColumns()).build();
  }

  public static Schema newV1(Column [] columns) {
    SchemaBuilder builder = builder();
    for (Column c :columns) {
      builder.add(c);
    }
    return builder.build();
  }

  public static Schema newV1(Iterable<Column> columns) {
    return builder().addAll(columns).build();
  }
}