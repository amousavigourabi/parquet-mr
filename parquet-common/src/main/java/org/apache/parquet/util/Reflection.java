/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.util;

import org.apache.parquet.compression.codec.CompressionCodec;
import org.apache.parquet.conf.ParquetConfiguration;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reflection utility, largely lifted from Apache Hadoop.
 */
public class Reflection {

  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
    new ConcurrentHashMap<Class<?>, Constructor<?>>();

  /**
   * Initialise an object for the given class, configured using the passed {@link ParquetConfiguration} if possible.
   *
   * @param <T> generic type of the {@link Class} to instantiate
   * @param clazz class of which an object is created
   * @param configuration {@link ParquetConfiguration} used to configure the instantiated object
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> clazz, ParquetConfiguration configuration) {
    T result;
    try {
      Constructor<T> constructor = (Constructor<T>) CONSTRUCTOR_CACHE.get(clazz);
      if (constructor == null) {
        constructor = clazz.getDeclaredConstructor(EMPTY_ARRAY);
        constructor.setAccessible(true);
        CONSTRUCTOR_CACHE.put(clazz, constructor);
      }
      result = constructor.newInstance();
      if (result instanceof CompressionCodec) {
        ((CompressionCodec) result).configureCodec(configuration);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
