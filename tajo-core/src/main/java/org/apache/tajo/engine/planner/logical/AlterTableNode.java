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

package org.apache.tajo.engine.planner.logical;


import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.algebra.AlterTableOpType;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.partition.PartitionPredicateMethodDesc;
import org.apache.tajo.engine.planner.PlanString;

public class AlterTableNode extends LogicalNode {

  @Expose
  private String tableName;
  @Expose
  private String newTableName;
  @Expose
  private String columnName;
  @Expose
  private String newColumnName;
  @Expose
  private Column column; //Renamed it to be generic based on the OpType the role it plays changes.
  @Expose
  private PartitionPredicateMethodDesc partition;
  @Expose
  private AlterTableOpType alterTableOpType;
  @Expose
  private Path path;

  public AlterTableNode(int pid) {
    super(pid, NodeType.ALTER_TABLE);
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getNewTableName() {
    return newTableName;
  }

  public void setNewTableName(String newTableName) {
    this.newTableName = newTableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getNewColumnName() {
    return newColumnName;
  }

  public void setNewColumnName(String newColumnName) {
    this.newColumnName = newColumnName;
  }

  public Column getColumn() {
    return column;
  }

  public void setColumn(Column column) {
    this.column = column;
  }

  public PartitionPredicateMethodDesc getPartition() {
    return partition;
  }

  public void setPartition(PartitionPredicateMethodDesc partitions) {
    this.partition = partitions;
  }

  public AlterTableOpType getAlterTableOpType() {
    return alterTableOpType;
  }

  public void setAlterTableOpType(AlterTableOpType alterTableOpType) {
    this.alterTableOpType = alterTableOpType;
  }

  public boolean hasPath() {
    return this.path != null;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public Path getPath() {
    return this.path;
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AlterTableNode) {
      AlterTableNode other = (AlterTableNode) obj;
      return super.equals(other);
    } else {
      return false;
    }
  }

    /*@Override
    public Object clone() throws CloneNotSupportedException {
        AlterTableNode alterTableNode = (AlterTableNode) super.clone();
        alterTableNode.tableName = tableName;
        alterTableNode.newTableName = newTableName;
        alterTableNode.columnName = columnName;
        alterTableNode.newColumnName=newColumnName;
        alterTableNode.column =(Column) column.clone();
        return alterTableNode;
    }*/

  @Override
  public String toString() {
    return "AlterTable (table=" + tableName + ")";
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
}
