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

package org.apache.tajo.master;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.JsonHelper;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.leader.DDLExecutor;
import org.apache.tajo.master.leader.QueryExecutor;
import org.apache.tajo.master.leader.prehook.CreateTableHook;
import org.apache.tajo.master.leader.prehook.DistributedQueryHookManager;
import org.apache.tajo.master.leader.prehook.InsertHook;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.plan.*;
import org.apache.tajo.plan.logical.LogicalRootNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.verifier.LogicalPlanVerifier;
import org.apache.tajo.plan.verifier.PreLogicalPlanVerifier;
import org.apache.tajo.plan.verifier.VerificationState;
import org.apache.tajo.plan.verifier.VerifyException;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.util.CommonTestingUtil;

import java.io.IOException;
import java.sql.SQLException;

import static org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;

public class GlobalEngine extends AbstractService {
  /** Class Logger */
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final MasterContext context;
  private final StorageManager sm;

  private SQLAnalyzer analyzer;
  private CatalogService catalog;
  private PreLogicalPlanVerifier preVerifier;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private LogicalPlanVerifier annotatedPlanVerifier;
  private DistributedQueryHookManager hookManager;

  private DDLExecutor ddlExecutor;
  private QueryExecutor queryExecutor;

  public GlobalEngine(final MasterContext context) {
    super(GlobalEngine.class.getName());
    this.context = context;
    this.catalog = context.getCatalog();
    this.sm = context.getStorageManager();
    this.ddlExecutor = new DDLExecutor(context);
  }

  public void start() {
    try  {
      analyzer = new SQLAnalyzer();
      preVerifier = new PreLogicalPlanVerifier(context.getCatalog());
      planner = new LogicalPlanner(context.getCatalog());
      optimizer = new LogicalOptimizer(context.getConf());
      annotatedPlanVerifier = new LogicalPlanVerifier(context.getConf(), context.getCatalog());

      hookManager = new DistributedQueryHookManager();
      hookManager.addHook(new CreateTableHook(this));
      hookManager.addHook(new InsertHook());
      this.queryExecutor = new QueryExecutor(context, hookManager);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    super.start();
  }

  public void stop() {
    super.stop();
  }

  @VisibleForTesting
  public SQLAnalyzer getAnalyzer() {
    return analyzer;
  }

  @VisibleForTesting
  public PreLogicalPlanVerifier getPreLogicalPlanVerifier() {
    return preVerifier;
  }

  @VisibleForTesting
  public LogicalPlanner getLogicalPlanner() {
    return planner;
  }

  @VisibleForTesting
  public LogicalOptimizer getLogicalOptimizer() {
    return optimizer;
  }

  @VisibleForTesting
  public LogicalPlanVerifier getLogicalPlanVerifier() {
    return annotatedPlanVerifier;
  }

  @VisibleForTesting
  public DDLExecutor getDDLExecutor() {
    return ddlExecutor;
  }

  private QueryContext createQueryContext(Session session) {
    QueryContext newQueryContext =  new QueryContext(context.getConf(), session);

    String tajoTest = System.getProperty(CommonTestingUtil.TAJO_TEST_KEY);
    if (tajoTest != null && tajoTest.equalsIgnoreCase(CommonTestingUtil.TAJO_TEST_TRUE)) {
      newQueryContext.putAll(CommonTestingUtil.getSessionVarsForTest());
    }

    return newQueryContext;
  }

  public SubmitQueryResponse executeQuery(Session session, String query, boolean isJson) {
    LOG.info("Query: " + query);
    QueryContext queryContext = createQueryContext(session);
    Expr planningContext;

    try {
      if (isJson) {
        planningContext = buildExpressionFromJson(query);
      } else {
        planningContext = buildExpressionFromSql(queryContext, query);
      }

      String jsonExpr = planningContext.toJson();
      LogicalPlan plan = createLogicalPlan(queryContext, planningContext);
      SubmitQueryResponse response = executeQueryInternal(queryContext, session, plan, query, jsonExpr);
      return response;
    } catch (Throwable t) {
      context.getSystemMetrics().counter("Query", "errorQuery").inc();
      LOG.error("\nStack Trace:\n" + StringUtils.stringifyException(t));
      SubmitQueryResponse.Builder responseBuilder = SubmitQueryResponse.newBuilder();
      responseBuilder.setUserName(queryContext.get(SessionVars.USERNAME));
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setIsForwarded(true);
      responseBuilder.setResultCode(ClientProtos.ResultCode.ERROR);
      String errorMessage = t.getMessage();
      if (t.getMessage() == null) {
        errorMessage = t.getClass().getName();
      }
      responseBuilder.setErrorMessage(errorMessage);
      responseBuilder.setErrorTrace(StringUtils.stringifyException(t));
      return responseBuilder.build();
    }
  }

  public Expr buildExpressionFromJson(String json) {
    return JsonHelper.fromJson(json, Expr.class);
  }

  public Expr buildExpressionFromSql(QueryContext queryContext, String sql)
      throws InterruptedException, IOException, IllegalQueryStatusException {
    context.getSystemMetrics().counter("Query", "totalQuery").inc();
    return analyzer.parse(sql);
  }

  private SubmitQueryResponse executeQueryInternal(QueryContext queryContext,
                                                      Session session,
                                                      LogicalPlan plan,
                                                      String sql,
                                                      String jsonExpr) throws Exception {

    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    SubmitQueryResponse.Builder responseBuilder = SubmitQueryResponse.newBuilder();
    responseBuilder.setIsForwarded(false);
    responseBuilder.setUserName(queryContext.get(SessionVars.USERNAME));


    if (PlannerUtil.checkIfDDLPlan(rootNode)) {
      context.getSystemMetrics().counter("Query", "numDDLQuery").inc();
      ddlExecutor.execute(queryContext, session, plan);
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setResultCode(ClientProtos.ResultCode.OK);


    } else if (plan.isExplain()) { // explain query
      queryExecutor.explainQuery(queryContext, session, plan, responseBuilder);


    } else if (PlannerUtil.checkIfSimpleQuery(plan)) {
      // Simple query indicates a form of 'select * from tb_name [LIMIT X];'.
      queryExecutor.executeSimpleQuery(queryContext, session, plan, responseBuilder);


    } else if (PlannerUtil.checkIfNonFromQuery(plan)) {
      // NonFromQuery indicates a form of 'select a, x+y;'
      queryExecutor.execNonFromQuery(queryContext, session, plan, responseBuilder);


    } else { // it requires distributed execution. So, the query is forwarded to a query master.
      queryExecutor.executeDistributedQuery(queryContext, session, plan, sql, jsonExpr, responseBuilder);
    }
    SubmitQueryResponse response = responseBuilder.build();
    return response;
  }

  public QueryId updateQuery(QueryContext queryContext, Session session, String sql, boolean isJson)
      throws IOException, SQLException, PlanningException {
    try {
      LOG.info("SQL: " + sql);

      Expr expr;
      if (isJson) {
        expr = JsonHelper.fromJson(sql, Expr.class);
      } else {
        // parse the query
        expr = analyzer.parse(sql);
      }

      LogicalPlan plan = createLogicalPlan(queryContext, expr);
      LogicalRootNode rootNode = plan.getRootBlock().getRoot();

      if (!PlannerUtil.checkIfDDLPlan(rootNode)) {
        throw new SQLException("This is not update query:\n" + sql);
      } else {
        ddlExecutor.execute(queryContext, session, plan);
        return QueryIdFactory.NULL_QUERY_ID;
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  private LogicalPlan createLogicalPlan(QueryContext queryContext, Expr expression) throws PlanningException {

    VerificationState state = new VerificationState();
    preVerifier.verify(queryContext, state, expression);
    if (!state.verified()) {
      StringBuilder sb = new StringBuilder();
      for (String error : state.getErrorMessages()) {
        sb.append(error).append("\n");
      }
      throw new VerifyException(sb.toString());
    }

    LogicalPlan plan = planner.createPlan(queryContext, expression);
    if (LOG.isDebugEnabled()) {
      LOG.debug("=============================================");
      LOG.debug("Non Optimized Query: \n" + plan.toString());
      LOG.debug("=============================================");
    }
    LOG.info("Non Optimized Query: \n" + plan.toString());
    optimizer.optimize(queryContext, plan);
    LOG.info("=============================================");
    LOG.info("Optimized Query: \n" + plan.toString());
    LOG.info("=============================================");

    annotatedPlanVerifier.verify(queryContext, state, plan);

    if (!state.verified()) {
      StringBuilder sb = new StringBuilder();
      for (String error : state.getErrorMessages()) {
        sb.append(error).append("\n");
      }
      throw new VerifyException(sb.toString());
    }

    return plan;
  }
}
