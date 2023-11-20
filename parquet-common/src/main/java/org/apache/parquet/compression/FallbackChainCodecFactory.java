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
package org.apache.parquet.compression;

import org.apache.parquet.compression.codec.CodecNotYetImplementedException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Arrays;
import java.util.List;

/**
 * Simple delegating implementation of the {@link CompressionCodecFactory} interface.
 */
public class FallbackChainCodecFactory implements CompressionCodecFactory {

  private final List<CompressionCodecFactory> codecFactories;

  /**
   * Create a new delegating {@link FallbackChainCodecFactory}.
   *
   * @param codecFactories the {@link CompressionCodecFactory} implementations to fall back on, in order
   */
  public FallbackChainCodecFactory(CompressionCodecFactory... codecFactories) {
    if (codecFactories.length < 1) {
      throw new IllegalArgumentException("org.apache.parquet.compression.FallbackChainCodecFactory needs to be constructed with at least one CompressionCodecFactory implementation to fall back on.");
    }
    this.codecFactories = Arrays.asList(codecFactories);
  }

  @Override
  public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
    BytesInputCompressor compressor = null;
    int i = 0;
    while (compressor == null) {
      try {
        compressor = codecFactories.get(i).getCompressor(codecName);
      } catch (CodecNotYetImplementedException ignored) {
        i++;
      }
      if (i >= codecFactories.size()) {
        throw new CodecNotYetImplementedException("None of the fallback codec factories have implemented the " + codecName + " compressor.");
      }
    }
    return compressor;
  }

  @Override
  public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
    BytesInputDecompressor decompressor = null;
    int i = 0;
    while (decompressor == null) {
      try {
        decompressor = codecFactories.get(i).getDecompressor(codecName);
      } catch (CodecNotYetImplementedException ignored) {
        i++;
      }
      if (i >= codecFactories.size()) {
        throw new CodecNotYetImplementedException("None of the fallback codec factories have implemented the " + codecName + " decompressor.");
      }
    }
    return decompressor;
  }

  @Override
  public void release() {
    codecFactories.forEach(CompressionCodecFactory::release);
  }
}
