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

import com.google.common.collect.Maps;
import org.apache.tajo.conf.TajoConf;

import java.lang.reflect.Constructor;
import java.util.Map;

public class ServiceTrackerFactory {
  private static final Class [] DEFAULT_PARAM = new Class [] {TajoConf.class};
  private static final Map<String, Constructor<ServiceTracker>> CONSTRUCTOR_CACHE = Maps.newConcurrentMap();

  public static final String DUMMY_SERVICE_TRACKER = DummyServiceTracker.class.getName();
  public static final String DEFAULT_HA_SERVICE_TRACKER = HAHdfsServiceTracker.class.getName();

  public static ServiceTracker getServiceTracker(TajoConf conf) {
    if (conf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
      return getHAServiceTracker(DEFAULT_HA_SERVICE_TRACKER, conf);
    } else {
      return getHAServiceTracker(DUMMY_SERVICE_TRACKER, conf);
    }
  }

  public static ServiceTracker getHAServiceTracker(String className, TajoConf conf) {
    Class<ServiceTracker> trackerClass;
    Constructor<ServiceTracker> constructor;

    try {
      trackerClass = (Class<ServiceTracker>) Class.forName(className);

      if (CONSTRUCTOR_CACHE.containsKey(className)) {
        constructor = CONSTRUCTOR_CACHE.get(className);
      } else {
        constructor = trackerClass.getConstructor(DEFAULT_PARAM);
        CONSTRUCTOR_CACHE.put(className, constructor);
      }

      return constructor.newInstance(conf);

    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
