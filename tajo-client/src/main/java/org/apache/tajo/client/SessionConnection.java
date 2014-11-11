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

package org.apache.tajo.client;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.discovery.ServiceTracker;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.ServerCallable;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.NetUtils;
import org.jboss.netty.channel.ConnectTimeoutException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.ipc.ClientProtos.CreateSessionRequest;
import static org.apache.tajo.ipc.ClientProtos.CreateSessionResponse;
import static org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;

public class SessionConnection implements Closeable {

  private final Log LOG = LogFactory.getLog(TajoClientImpl.class);

  final ServiceTracker serviceTracker;

  final Map<QueryId, InetSocketAddress> queryMasterMap = new ConcurrentHashMap<QueryId, InetSocketAddress>();

  final KeyValueSet properties;

  final RpcConnectionPool connPool;

  private final String baseDatabase;

  private final UserRoleInfo userInfo;

  volatile TajoIdProtos.SessionIdProto sessionId;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private static final int RPC_WORKER_THREAD_NUM = 4;

  public SessionConnection(String hostname, int port, String baseDatabase) throws IOException {
    this(new DummyClientServiceTracker(NetUtils.createSocketAddr(hostname, port)), baseDatabase, new KeyValueSet());
  }

  /**
   * Connect to TajoMaster
   *
   * @param tracker service tracker
   * @param baseDatabase The base database name. It is case sensitive. If it is null,
   *                     the 'default' database will be used.
   * @throws java.io.IOException
   */
  public SessionConnection(ServiceTracker tracker, @Nullable String baseDatabase, @Nullable KeyValueSet properties)
      throws IOException {
    this.serviceTracker = tracker;
    this.properties = properties == null ? new KeyValueSet() : properties;

    // Don't share connection pool per client
    connPool = RpcConnectionPool.newPool(getClass().getSimpleName(), RPC_WORKER_THREAD_NUM);
    userInfo = UserRoleInfo.getCurrentUser();
    this.baseDatabase = baseDatabase != null ? baseDatabase : null;
  }

  public NettyClientBase getTajoMasterConnection(boolean asyncMode) throws NoSuchMethodException,
      ConnectTimeoutException, ClassNotFoundException {
    return connPool.getConnection(getTajoMasterAddr(), TajoMasterClientProtocol.class, asyncMode);
  }

  public NettyClientBase getConnection(QueryId queryId, Class protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ConnectTimeoutException, ClassNotFoundException {
    InetSocketAddress addr = queryMasterMap.get(queryId);
    return connPool.getConnection(addr, protocolClass, asyncMode);
  }

  public NettyClientBase getConnection(InetSocketAddress addr, Class protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ConnectTimeoutException, ClassNotFoundException {
    return connPool.getConnection(addr, protocolClass, asyncMode);
  }

  @SuppressWarnings("unused")
  public void setSessionId(TajoIdProtos.SessionIdProto sessionId) {
    this.sessionId = sessionId;
  }

  public TajoIdProtos.SessionIdProto getSessionId() {
    return sessionId;
  }

  public String getBaseDatabase() {
    return baseDatabase;
  }

  public boolean isConnected() {
    if(!closed.get()){
      try {
        return connPool.getConnection(getTajoMasterAddr(), TajoMasterClientProtocol.class, false).isConnected();
      } catch (Throwable e) {
        return false;
      }
    }
    return false;
  }

  public UserRoleInfo getUserInfo() {
    return userInfo;
  }

  public String getCurrentDatabase() throws ServiceException {
    return new ServerCallable<String>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public String call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.getCurrentDatabase(null, sessionId).getValue();
      }
    }.withRetries();
  }

  public Boolean updateSessionVariables(final Map<String, String> variables) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        KeyValueSet keyValueSet = new KeyValueSet();
        keyValueSet.putAll(variables);
        ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
            .setSessionId(sessionId)
            .setSetVariables(keyValueSet.getProto()).build();

        return tajoMasterService.updateSessionVariables(null, request).getValue();
      }
    }.withRetries();
  }

  public Boolean unsetSessionVariables(final List<String> variables)  throws ServiceException {
    return new ServerCallable<Boolean>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
            .setSessionId(sessionId)
            .addAllUnsetVariables(variables).build();
        return tajoMasterService.updateSessionVariables(null, request).getValue();
      }
    }.withRetries();
  }

  public String getSessionVariable(final String varname) throws ServiceException {
    return new ServerCallable<String>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public String call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.getSessionVariable(null, convertSessionedString(varname)).getValue();
      }
    }.withRetries();
  }

  public Boolean existSessionVariable(final String varname) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.existSessionVariable(null, convertSessionedString(varname)).getValue();
      }
    }.withRetries();
  }

  public Map<String, String> getAllSessionVariables() throws ServiceException {
    return new ServerCallable<Map<String, String>>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class,
        false, true) {

      public Map<String, String> call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        KeyValueSet keyValueSet = new KeyValueSet(tajoMasterService.getAllSessionVariables(null, sessionId));
        return keyValueSet.getAllKeyValus();
      }
    }.withRetries();
  }

  public Boolean selectDatabase(final String databaseName) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.selectDatabase(null, convertSessionedString(databaseName)).getValue();
      }
    }.withRetries();
  }

  @Override
  public void close() {
    if(closed.getAndSet(true)){
      return;
    }

    // remove session
    try {

      NettyClientBase client = connPool.getConnection(getTajoMasterAddr(), TajoMasterClientProtocol.class, false);
      TajoMasterClientProtocolService.BlockingInterface tajoMaster = client.getStub();
      tajoMaster.removeSession(null, sessionId);

    } catch (Throwable e) {
    }

    if(connPool != null) {
      connPool.shutdown();
    }

    queryMasterMap.clear();
  }

  protected InetSocketAddress getTajoMasterAddr() {
    return serviceTracker.getMasterClientAddress();
  }

  protected void checkSessionAndGet(NettyClientBase client) throws ServiceException {

    if (sessionId == null) {

      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
      CreateSessionRequest.Builder builder = CreateSessionRequest.newBuilder();
      builder.setUsername(userInfo.getUserName()).build();

      if (baseDatabase != null) {
        builder.setBaseDatabaseName(baseDatabase);
      }

      CreateSessionResponse response = tajoMasterService.createSession(null, builder.build());

      if (response.getState() == CreateSessionResponse.ResultState.SUCCESS) {

        sessionId = response.getSessionId();
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Got session %s as a user '%s'.", sessionId.getId(), userInfo.getUserName()));
        }

      } else {
        throw new InvalidClientSessionException(response.getMessage());
      }
    }
  }

  ClientProtos.SessionedStringProto convertSessionedString(String str) {
    ClientProtos.SessionedStringProto.Builder builder = ClientProtos.SessionedStringProto.newBuilder();
    builder.setSessionId(sessionId);
    builder.setValue(str);
    return builder.build();
  }

}
