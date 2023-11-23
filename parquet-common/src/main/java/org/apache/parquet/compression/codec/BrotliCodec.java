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
package org.apache.parquet.compression.codec;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.conf.ParquetConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Simple streaming Brotli codec implementation.
 */
public class BrotliCodec implements CompressionCodec {

  @Override
  public InputStream createInputStream(ByteBufferInputStream inputStream) throws IOException {
    throw new CodecNotYetImplementedException("Brotli decompression is not yet supported by this CompressionCodecFactory implementation.");
  }

  @Override
  public OutputStream createOutputStream(ByteArrayOutputStream compressedOutBuffer) throws IOException {
    throw new CodecNotYetImplementedException("Brotli compression is not yet supported by this CompressionCodecFactory implementation.");
  }

  @Override
  public void configureCodec(ParquetConfiguration configuration) {
    //todo
  }
}
