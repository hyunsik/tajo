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

package org.apache.tajo.engine.query;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;

import java.sql.ResultSet;

public class TestLargeNumbers extends QueryTestCaseBase {

  public TestLargeNumbers() {
  }

  @Test
  public final void testSelect1() throws Exception {
    executeDDL("table1_ddl.sql", "table1.tbl", null);
    ResultSet res = executeString("select * from table1");
    assertResultSet(res);
    cleanupQuery(res);
    client.dropTable("table1");
  }

  @Test
  public final void testSelect2() throws Exception {
    executeDDL("table1_ddl.sql", "table1.tbl", null);
    ResultSet res = executeString("select round(num1, 3) from table1");
    assertResultSet(res);
    cleanupQuery(res);
    client.dropTable("table1");
  }
}
