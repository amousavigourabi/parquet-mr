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

import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.conf.ParquetConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Lz4RawCodec implements CompressionCodec {

  @Override
  public InputStream createInputStream(ByteBufferInputStream inputStream) throws IOException {
    return new BlockLZ4CompressorInputStream(inputStream);
  }

  @Override
  public OutputStream createOutputStream(ByteArrayOutputStream compressedOutBuffer) throws IOException {
    return new BlockLZ4CompressorOutputStream(compressedOutBuffer);
  }

  @Override
  public void configureCodec(ParquetConfiguration configuration) {
  }
}
