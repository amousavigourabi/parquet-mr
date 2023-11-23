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
package org.apache.parquet.hadoop.metadata;


import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.codec.CompressionCodecNotSupportedException;
import java.util.Locale;

public enum CompressionCodecName {
  UNCOMPRESSED(null, null, CompressionCodec.UNCOMPRESSED, ""),
  SNAPPY("org.apache.parquet.hadoop.codec.SnappyCodec", org.apache.parquet.compression.codec.SnappyCodec.class, CompressionCodec.SNAPPY, ".snappy"),
  GZIP("org.apache.hadoop.io.compress.GzipCodec", org.apache.parquet.compression.codec.GzipCodec.class, CompressionCodec.GZIP, ".gz"),
  LZO("com.hadoop.compression.lzo.LzoCodec", org.apache.parquet.compression.codec.LzoCodec.class, CompressionCodec.LZO, ".lzo"),
  BROTLI("org.apache.hadoop.io.compress.BrotliCodec", org.apache.parquet.compression.codec.BrotliCodec.class, CompressionCodec.BROTLI, ".br"),
  LZ4("org.apache.hadoop.io.compress.Lz4Codec", org.apache.parquet.compression.codec.Lz4Codec.class, CompressionCodec.LZ4, ".lz4hadoop"),
  ZSTD("org.apache.parquet.hadoop.codec.ZstandardCodec", org.apache.parquet.compression.codec.ZstdCodec.class, CompressionCodec.ZSTD, ".zstd"),
  LZ4_RAW("org.apache.parquet.hadoop.codec.Lz4RawCodec", org.apache.parquet.compression.codec.Lz4RawCodec.class, CompressionCodec.LZ4_RAW, ".lz4raw");

  public static CompressionCodecName fromConf(String name) {
     if (name == null) {
       return UNCOMPRESSED;
     }
     return valueOf(name.toUpperCase(Locale.ENGLISH));
  }

  public static CompressionCodecName fromCompressionCodec(Class<?> clazz) {
    if (clazz == null) {
      return UNCOMPRESSED;
    }
    String name = clazz.getName();
    for (CompressionCodecName codec : CompressionCodecName.values()) {
      if (name.equals(codec.getHadoopCompressionCodecClassName())) {
        return codec;
      }
    }
    throw new CompressionCodecNotSupportedException(clazz);
  }

  public static CompressionCodecName fromParquet(CompressionCodec codec) {
    for (CompressionCodecName codecName : CompressionCodecName.values()) {
      if (codec.equals(codecName.parquetCompressionCodec)) {
        return codecName;
      }
    }
    throw new IllegalArgumentException("Unknown compression codec " + codec);
  }

  private final String hadoopCompressionCodecClass;
  private final Class<?> simpleCompressionCodecClass;
  private final CompressionCodec parquetCompressionCodec;
  private final String extension;

  private CompressionCodecName(String hadoopCompressionCodecClass, Class<?> simpleCompressionCodecClass, CompressionCodec parquetCompressionCodec, String extension) {
    this.hadoopCompressionCodecClass = hadoopCompressionCodecClass;
    this.simpleCompressionCodecClass = simpleCompressionCodecClass;
    this.parquetCompressionCodec = parquetCompressionCodec;
    this.extension = extension;
  }

  public String getHadoopCompressionCodecClassName() {
    return hadoopCompressionCodecClass;
  }

  public Class<?> getSimpleCompressionCodecClass() {
    return simpleCompressionCodecClass;
  }

  public Class<?> getHadoopCompressionCodecClass() {
    String codecClassName = getHadoopCompressionCodecClassName();
    if (codecClassName==null) {
      return null;
    }
    try {
      return Class.forName(codecClassName);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  public CompressionCodec getParquetCompressionCodec() {
    return parquetCompressionCodec;
  }

  public String getExtension() {
    return extension;
  }

}
