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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.compression.codec.CompressionCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.util.Reflection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple implementation of the {@link CompressionCodecFactory}, without any Hadoop dependencies.
 * Does NOT yet support the full LZO and Brotli codecs and only part of LZ4, GZIP, and ZSTD.
 * When configured in a setup encountering the LZO or Brotli codecs, or reading LZ4, GZIP, or ZSTD
 * data, a {@link org.apache.parquet.compression.codec.CodecNotYetImplementedException} will be thrown.
 * <p>
 * Not recommended for use without {@link FallbackChainCodecFactory} as encounters with the LZO or
 * Brotli codecs cause it to fail.
 */
public class SimpleCompressionCodecFactory implements CompressionCodecFactory {

  private final Map<CompressionCodecName, BytesInputCompressor> compressors = new HashMap<>();
  private final Map<CompressionCodecName, BytesInputDecompressor> decompressors = new HashMap<>();

  protected final ParquetConfiguration configuration;
  protected final int pageSize;

  /**
   * Create a new codec factory.
   *
   * @param configuration used to pass compression codec configuration information
   * @param pageSize the expected page size, does not set a hard limit, currently just
   *                 used to set the initial size of the output stream used when
   *                 compressing a buffer. If this factory is only used to construct
   *                 decompressors this parameter has no impact on the function of the factory
   */
  public SimpleCompressionCodecFactory(ParquetConfiguration configuration, int pageSize) {
    this.configuration = configuration;
    this.pageSize = pageSize;
  }

  class SimpleHeapBytesDecompressor implements BytesInputDecompressor {

    private final CompressionCodec codec;

    public SimpleHeapBytesDecompressor(CompressionCodecName codecName) {
      this.codec = getCodec(codecName);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      final BytesInput decompressed;
      if (codec != null) {
        try (InputStream is = codec.createInputStream(bytes.toInputStream())) {
          decompressed = BytesInput.from(is, uncompressedSize);
        }
      } else {
        decompressed = bytes;
      }
      return decompressed;
    }

    @Override
    public void release() { }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
      ByteBuffer decompressed = decompress(BytesInput.from(input), uncompressedSize).toByteBuffer();
      output.put(decompressed);
    }
  }

  /**
   * Encapsulates the logic around compression
   */
  class SimpleHeapBytesCompressor implements BytesInputCompressor {

    private final CompressionCodec codec;
    private final CompressionCodecName codecName;

    public SimpleHeapBytesCompressor(CompressionCodecName codecName) {
      this.codecName = codecName;
      this.codec = getCodec(codecName);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      final BytesInput compressedBytes;
      if (codec != null) {
        try (ByteArrayOutputStream compressedOutBuffer = new ByteArrayOutputStream(pageSize);
             OutputStream os = codec.createOutputStream(compressedOutBuffer)) {
          bytes.writeAllTo(os);
          compressedBytes = BytesInput.from(compressedOutBuffer);
        }
      } else {
        compressedBytes = bytes;
      }
      return compressedBytes;
    }

    @Override
    public CompressionCodecName getCodecName() {
      return codecName;
    }

    @Override
    public void release() { }

  }

  protected BytesInputCompressor createCompressor(CompressionCodecName codecName) {
    return new SimpleHeapBytesCompressor(codecName);
  }

  protected BytesInputDecompressor createDecompressor(CompressionCodecName codecName) {
    return new SimpleHeapBytesDecompressor(codecName);
  }

  /**
   *
   * @param codecName the requested codec
   * @return the corresponding simple codec. null if UNCOMPRESSED
   */
  protected CompressionCodec getCodec(CompressionCodecName codecName) {
    Class<?> codecClass = codecName.getSimpleCompressionCodecClass();
    if (codecClass == null) {
      return null;
    }

    try {
      return (CompressionCodec) Reflection.newInstance(codecClass, configuration);
    } catch (RuntimeException e) {
      throw new RuntimeException("Class " + codecClass.getCanonicalName() + " could not be instantiated", e);
    }
  }

  @Override
  public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
    BytesInputCompressor comp = compressors.get(codecName);
    if (comp == null) {
      comp = createCompressor(codecName);
      compressors.put(codecName, comp);
    }
    return comp;
  }

  @Override
  public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
    BytesInputDecompressor decomp = decompressors.get(codecName);
    if (decomp == null) {
      decomp = createDecompressor(codecName);
      decompressors.put(codecName, decomp);
    }
    return decomp;
  }

  @Override
  public void release() {
    for (BytesInputCompressor compressor : compressors.values()) {
      compressor.release();
    }
    compressors.clear();
    for (BytesInputDecompressor decompressor : decompressors.values()) {
      decompressor.release();
    }
    decompressors.clear();
  }
}
