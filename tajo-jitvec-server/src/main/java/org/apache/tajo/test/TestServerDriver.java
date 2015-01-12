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

package org.apache.tajo.test;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.CreateSessionRequest;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.master.GlobalEngine;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.TajoMasterClientService;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.serder.LogicalNodeSerializer;
import org.apache.tajo.plan.verifier.LogicalPlanVerifier;
import org.apache.tajo.plan.verifier.PreLogicalPlanVerifier;
import org.apache.tajo.plan.verifier.VerificationState;
import org.apache.tajo.plan.verifier.VerifyException;
import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.session.InvalidSessionException;
import org.apache.tajo.session.Session;
import org.apache.tajo.test.TestServerProtocol.*;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.ProtoUtil;

import java.net.InetSocketAddress;

public class TestServerDriver extends AbstractService {
  private static final Log LOG = LogFactory.getLog(TestServerDriver.class);

  TpchTestBase tpchTestBase;
  TajoTestingCluster testingCluster;

  private TajoConf conf;
  private SQLAnalyzer analyzer;
  private CatalogService catalog;
  private PreLogicalPlanVerifier preVerifier;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private LogicalPlanVerifier annotatedPlanVerifier;

  private BlockingRpcServer rpcServer;
  private TestServerProtocolService.BlockingInterface handler;

  public TestServerDriver() throws Exception {
    super(TestServerDriver.class.getSimpleName());

    tpchTestBase = new TpchTestBase();
    tpchTestBase.setUp();

    testingCluster = tpchTestBase.getTestingCluster();
    this.conf = testingCluster.getConfiguration();
    catalog = testingCluster.getMaster().getCatalog();

    analyzer = new SQLAnalyzer();
    preVerifier = new PreLogicalPlanVerifier(catalog);
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);
    annotatedPlanVerifier = new LogicalPlanVerifier(conf, catalog);

    handler = new ProtocolHandler();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    rpcServer = new BlockingRpcServer(TestServerProtocol.class, handler,
        new InetSocketAddress("0.0.0.0", 30060), 1);

    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    rpcServer.start();

    super.serviceStart();

    InetSocketAddress bindAddr = rpcServer.getListenAddress();
    System.out.println("\n=============================================================================");
    System.out.println("Test Server Addr: " + NetUtils.normalizeInetSocketAddress(bindAddr));
    System.out.println("HDFS Namenode Addr: " + testingCluster.getMiniDFSCluster().getFileSystem().getUri());
    System.out.println("Catalog Server Addr: " +
        NetUtils.normalizeInetSocketAddress(testingCluster.getMaster().getCatalogServer().getBindAddress()));
    System.out.println("=============================================================================\n");
  }

  public void serviceStop() throws Exception {
    if (rpcServer != null) {
      rpcServer.shutdown();
    }

    super.serviceStop();
  }

  public class ProtocolHandler implements TestServerProtocolService.BlockingInterface {
    @Override
    public ClientProtos.CreateSessionResponse createSession(RpcController controller, CreateSessionRequest request)
        throws ServiceException {
      TajoMaster.MasterContext context = testingCluster.getMaster().getContext();
      TajoMasterClientService clientService = context.getClientService();
      return clientService.getServiceHandler().createSession(controller, request);
    }

    @Override
    public PrimitiveProtos.BoolProto removeSession(RpcController controller, TajoIdProtos.SessionIdProto request)
        throws ServiceException {
      TajoMaster.MasterContext context = testingCluster.getMaster().getContext();
      TajoMasterClientService clientService = context.getClientService();
      return clientService.getServiceHandler().removeSession(controller, request);
    }

    @Override
    public ExecuteSQLResponse executeSQL(RpcController controller, TestServerProtocol
        .ExecuteSQLRequest request) throws ServiceException {
      TajoMaster.MasterContext context = testingCluster.getMaster().getContext();

      GlobalEngine engine = context.getGlobalEngine();

      Session session;

      ExecuteSQLResponse.Builder responseBuilder = ExecuteSQLResponse.newBuilder();
      try {
        session = context.getSessionManager().getSession(request.getSessionId().getId());

        LOG.debug("Submitted Query [\n" + request.getSql() + "\n]");
        engine.executeQuery(session, request.getSql(), false);

        responseBuilder.setStatus(Status.OK_PROTO);
      } catch (Throwable t) {
        responseBuilder.setStatus(Status.convertStatus(Status.UNKNOWN, t.getMessage()));
      }

      return responseBuilder.build();
    }

    @Override
    public PlanResponse requestPlan(RpcController controller, RequestPlan request) throws ServiceException {
      TajoMaster.MasterContext context = testingCluster.getMaster().getContext();

      PlanResponse.Builder builder = PlanResponse.newBuilder();

      Session session = null;
      try {
        session = context.getSessionManager().getSession(request.getSessionId().getId());
      } catch (InvalidSessionException e) {
        LOG.error(e.getMessage());
        builder.setStatus(Status.convertStatus(Status.UNKNOWN, e.getMessage()));
      }

      QueryContext queryContext = new QueryContext(conf, session);

      try {
        LOG.info("Request is received: " + request.getSql());
        Expr expr = analyzer.parse(request.getSql());
        VerificationState state = new VerificationState();
        preVerifier.verify(queryContext, state, expr);
        if (!state.verified()) {
          StringBuilder sb = new StringBuilder();
          for (String error : state.getErrorMessages()) {
            sb.append(error).append("\n");
          }
          throw new VerifyException(sb.toString());
        }

        LogicalPlan plan = planner.createPlan(queryContext, expr);
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

        builder.setPlan(LogicalNodeSerializer.serialize(plan.getRootBlock().getRoot()));
        builder.setStatus(Status.OK_PROTO);

      } catch (PlanningException e) {
        if (e.getMessage() != null) {
          LOG.error(e.getMessage());
          builder.setStatus(Status.convertStatus(Status.UNKNOWN, e.getMessage()));
        } else {
          e.printStackTrace();
          builder.setStatus(Status.convertStatus(Status.UNKNOWN, "Unknown"));
        }
      }

      return builder.build();
    }

    @Override
    public SumResponse sum(RpcController controller, SumRequest request) throws ServiceException {
      return SumResponse.newBuilder().setResult(
          request.getX1()+request.getX2()+request.getX3()+request.getX4()
      ).build();
    }

    @Override
    public EchoMessage echo(RpcController controller, EchoMessage request) throws ServiceException {
      return EchoMessage.newBuilder().setMessage(request.getMessage()).build();
    }

    public boolean getNullCalled = false;
    public boolean getErrorCalled = false;

    @Override
    public EchoMessage getError(RpcController controller, EchoMessage request)
        throws ServiceException {
      getErrorCalled = true;
      controller.setFailed(request.getMessage());
      return request;
    }

    @Override
    public EchoMessage getNull(RpcController controller, EchoMessage request)
        throws ServiceException {
      getNullCalled = true;
      LOG.info("noCallback is called");
      return null;
    }

    @Override
    public EchoMessage deley(RpcController controller, EchoMessage request) throws ServiceException {
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
      }
      return request;
    }
  }

  public static void startServer(String [] args) throws Exception {
    TajoConf conf = new TajoConf();
    TestServerDriver server = new TestServerDriver();
    server.init(conf);
    server.start();
  }

  public static void main(String [] args) throws Exception {
    startServer(args);
  }
}
