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

import com.google.common.collect.Maps;

import java.lang.reflect.Constructor;
import java.util.Map;

public class ReflectionUtil {
  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
  private static final Object [] EMPTY_PARAM = new Object[]{};

  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = Maps.newConcurrentMap();

	public static <T> T newInstance(Class<T> clazz)  {
    Constructor<T> constructor;
    try {
      if (CONSTRUCTOR_CACHE.containsKey(clazz)) {
        constructor = (Constructor<T>) CONSTRUCTOR_CACHE.get(clazz);
      } else {
        constructor = clazz.getConstructor(EMPTY_ARRAY);
        CONSTRUCTOR_CACHE.put(clazz, constructor);
      }

      return constructor.newInstance(EMPTY_PARAM);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
	}
}
