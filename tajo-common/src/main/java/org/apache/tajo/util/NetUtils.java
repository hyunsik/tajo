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

package org.apache.tajo.util;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.tajo.net.SocketIOWithTimeout;

import java.io.IOException;
import java.net.*;
import java.nio.channels.SocketChannel;

public class NetUtils {
  private static final Log LOG = LogFactory.getLog(NetUtils.class);

  public static String normalizeInetSocketAddress(InetSocketAddress addr) {
    return addr.getAddress().getHostAddress() + ":" + addr.getPort();
  }

  public static InetSocketAddress createSocketAddr(String addr) {
    String [] splitted = addr.split(":");
    return new InetSocketAddress(splitted[0], Integer.parseInt(splitted[1]));
  }

  /**
   * Util method to build socket addr from either:
   *   <host>
   *   <host>:<port>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String host, int port) {
    return new InetSocketAddress(host, port);
  }

  public static InetSocketAddress createUnresolved(String addr) {
    String [] splitted = addr.split(":");
    return InetSocketAddress.createUnresolved(splitted[0], Integer.parseInt(splitted[1]));
  }

  /**
   * Returns InetSocketAddress that a client can use to
   * connect to the server. NettyServerBase.getListenerAddress() is not correct when
   * the server binds to "0.0.0.0". This returns "hostname:port" of the server,
   * or "127.0.0.1:port" when the getListenerAddress() returns "0.0.0.0:port".
   *
   * @param addr of a listener
   * @return socket address that a client can use to connect to the server.
   */
  public static InetSocketAddress getConnectAddress(InetSocketAddress addr) {
    if (!addr.isUnresolved() && addr.getAddress().isAnyLocalAddress()) {
      try {
        addr = new InetSocketAddress(InetAddress.getLocalHost(), addr.getPort());
      } catch (UnknownHostException uhe) {
        // shouldn't get here unless the host doesn't have a loopback iface
        addr = new InetSocketAddress("127.0.0.1", addr.getPort());
      }
    }
    InetSocketAddress canonicalAddress =
        new InetSocketAddress(addr.getAddress().getCanonicalHostName(), addr.getPort());
    return canonicalAddress;
  }

  /**
   * Given an InetAddress, checks to see if the address is a local address, by
   * comparing the address with all the interfaces on the node.
   * @param addr address to check if it is local node's address
   * @return true if the address corresponds to the local node
   */
  public static boolean isLocalAddress(InetAddress addr) {
    // Check if the address is any local or loop back
    boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

    // Check if the address is defined on any interface
    if (!local) {
      try {
        local = NetworkInterface.getByInetAddress(addr) != null;
      } catch (SocketException e) {
        local = false;
      }
    }
    return local;
  }

  public static String normalizeHost(String host) {
    try {
      InetAddress address = InetAddress.getByName(host);
      if (isLocalAddress(address)) {
        return InetAddress.getLocalHost().getHostAddress();
      } else {
        return address.getHostAddress();
      }
    } catch (UnknownHostException e) {
    }
    return host;
  }

  /**
   * This is a drop-in replacement for
   * {@link Socket#connect(java.net.SocketAddress, int)}.
   * In the case of normal sockets that don't have associated channels, this
   * just invokes <code>socket.connect(endpoint, timeout)</code>. If
   * <code>socket.getChannel()</code> returns a non-null channel,
   * connect is implemented using Hadoop's selectors. This is done mainly
   * to avoid Sun's connect implementation from creating thread-local
   * selectors, since Hadoop does not have control on when these are closed
   * and could end up taking all the available file descriptors.
   *
   * @see java.net.Socket#connect(java.net.SocketAddress, int)
   *
   * @param socket
   * @param address the remote address
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket,
                             SocketAddress address,
                             int timeout) throws IOException {
    connect(socket, address, null, timeout);
  }

  /**
   * Like {@link NetUtils#connect(Socket, SocketAddress, int)} but
   * also takes a local address and port to bind the socket to.
   *
   * @param socket
   * @param endpoint the remote address
   * @param localAddr the local address to bind the socket to
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket,
                             SocketAddress endpoint,
                             SocketAddress localAddr,
                             int timeout) throws IOException {
    if (socket == null || endpoint == null || timeout < 0) {
      throw new IllegalArgumentException("Illegal argument for connect()");
    }

    SocketChannel ch = socket.getChannel();

    if (localAddr != null) {
      Class localClass = localAddr.getClass();
      Class remoteClass = endpoint.getClass();
      Preconditions.checkArgument(localClass.equals(remoteClass),
          "Local address %s must be of same family as remote address %s.",
          localAddr, endpoint);
      socket.bind(localAddr);
    }

    try {
      if (ch == null) {
        // let the default implementation handle it.
        socket.connect(endpoint, timeout);
      } else {
        SocketIOWithTimeout.connect(ch, endpoint, timeout);
      }
    } catch (SocketTimeoutException ste) {
      throw new ConnectTimeoutException(ste.getMessage());
    }

    // There is a very rare case allowed by the TCP specification, such that
    // if we are trying to connect to an endpoint on the local machine,
    // and we end up choosing an ephemeral port equal to the destination port,
    // we will actually end up getting connected to ourself (ie any data we
    // send just comes right back). This is only possible if the target
    // daemon is down, so we'll treat it like connection refused.
    if (socket.getLocalPort() == socket.getPort() &&
        socket.getLocalAddress().equals(socket.getInetAddress())) {
      LOG.info("Detected a loopback TCP socket, disconnecting it");
      socket.close();
      throw new ConnectException(
          "Localhost targeted connection resulted in a loopback. " +
              "No daemon is listening on the target port.");
    }
  }
}