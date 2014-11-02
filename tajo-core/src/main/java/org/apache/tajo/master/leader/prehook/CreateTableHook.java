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

package org.apache.tajo.master.leader.prehook;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.physical.PhysicalPlanUtil;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.GlobalEngine;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.CreateTableNode;
import org.apache.tajo.plan.logical.LogicalRootNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.KeyValueSet;

public class CreateTableHook implements DistributedQueryHook {

  @Override
  public boolean isEligible(QueryContext queryContext, LogicalPlan plan) {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
    return rootNode.getChild().getType() == NodeType.CREATE_TABLE;
  }

  @Override
  public void hook(TajoMaster.MasterContext context, QueryContext queryContext, LogicalPlan plan) throws Exception {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
    CreateTableNode createTableNode = rootNode.getChild();

    TableMeta meta = new TableMeta(createTableNode.getStorageType(), new KeyValueSet());
    PhysicalPlanUtil.setNullCharIfNecessary(queryContext, createTableNode, meta);
    meta.getOptions().putAll(createTableNode.getOptions());
    createTableNode.setOptions(meta.getOptions());

    String [] splitted  = CatalogUtil.splitFQTableName(createTableNode.getTableName());
    String databaseName = splitted[0];
    String tableName = splitted[1];
    queryContext.setOutputTable(tableName);
    queryContext.setOutputPath(
        StorageUtil.concatPath(TajoConf.getWarehouseDir(context.getConf()), databaseName, tableName));
    if(createTableNode.getPartitionMethod() != null) {
      queryContext.setPartitionMethod(createTableNode.getPartitionMethod());
    }
    queryContext.setCreateTable();
  }
}
