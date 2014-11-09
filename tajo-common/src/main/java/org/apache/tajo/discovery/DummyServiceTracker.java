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

package org.apache.tajo.discovery;

import org.apache.tajo.conf.TajoConf;

import java.net.InetSocketAddress;

public class DummyServiceTracker extends ServiceTracker {
  private final TajoConf conf;

  @SuppressWarnings("unused")
  public DummyServiceTracker(TajoConf conf) {
    this.conf = conf;
  }

  @Override
  public boolean isHighAvailable() {
    return false;
  }

  @Override
  public InetSocketAddress getMasterUmbilicalAddress() {
    return conf.getSocketAddrVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
  }

  @Override
  public InetSocketAddress getMasterClientAddress() {
    return conf.getSocketAddrVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS);
  }

  @Override
  public InetSocketAddress getResourceTrackerAddress() {
    return conf.getSocketAddrVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS);
  }

  @Override
  public InetSocketAddress getCatalogAddress() {
    return conf.getSocketAddrVar(TajoConf.ConfVars.CATALOG_ADDRESS);
  }

  @Override
  public InetSocketAddress getMasterInfoAddress() {
    return conf.getSocketAddrVar(TajoConf.ConfVars.TAJO_MASTER_INFO_ADDRESS);
  }
}
