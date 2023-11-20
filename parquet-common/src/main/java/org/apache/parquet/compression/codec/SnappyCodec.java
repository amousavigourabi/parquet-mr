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

import org.apache.commons.compress.compressors.lz77support.Parameters;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorOutputStream;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.conf.ParquetConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SnappyCodec implements CompressionCodec {

  private final static String BUFFER_SIZE_CONFIG = "io.file.buffer.size";

  int bufferSize = 4*1024;

  @Override
  public InputStream createInputStream(ByteBufferInputStream inputStream) throws IOException {
    return new SnappyCompressorInputStream(inputStream);
  }

  @Override
  public OutputStream createOutputStream(ByteArrayOutputStream compressedOutBuffer) throws IOException {
    Parameters parameters = SnappyCompressorOutputStream.createParameterBuilder(bufferSize).build();
    return new SnappyCompressorOutputStream(compressedOutBuffer, compressedOutBuffer.size(), parameters);
  }

  @Override
  public void configureCodec(ParquetConfiguration configuration) {
    bufferSize = configuration.getInt(BUFFER_SIZE_CONFIG, bufferSize);
  }
}
