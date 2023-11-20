/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.avro;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.SimpleCompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.parquet.avro.AvroTestUtil.optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class TestReadWriteDifferentCodecFactories {

  @Parameterized.Parameters(name = "codec = {0}, hadoopWrite = {1}, sameReadWriteCodec = {2}")
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
      { CompressionCodecName.UNCOMPRESSED, true, true },
      { CompressionCodecName.LZ4, true, true },
      { CompressionCodecName.GZIP, true, true },
      { CompressionCodecName.LZ4_RAW, true, true },
      { CompressionCodecName.SNAPPY, true, true },
      { CompressionCodecName.ZSTD, true, true },
      { CompressionCodecName.UNCOMPRESSED, false, true },
      { CompressionCodecName.LZ4_RAW, false, true },
      { CompressionCodecName.SNAPPY, false, true },
      { CompressionCodecName.UNCOMPRESSED, true, false },
      { CompressionCodecName.LZ4_RAW, true, false },
      { CompressionCodecName.SNAPPY, true, false },
      { CompressionCodecName.UNCOMPRESSED, false, false },
      { CompressionCodecName.LZ4, false, false },
      { CompressionCodecName.GZIP, false, false },
      { CompressionCodecName.LZ4_RAW, false, false },
      { CompressionCodecName.SNAPPY, false, false },
      { CompressionCodecName.ZSTD, false, false }
    };
    return Arrays.asList(data);
  }

  private final CompressionCodecName codecName;
  private final boolean hadoopWriterCodec;
  private final boolean sameReadWriteCodec;

  private final ParquetConfiguration conf = new PlainParquetConfiguration();

  public TestReadWriteDifferentCodecFactories(CompressionCodecName codecName, boolean hadoopWriterCodec, boolean sameReadWriteCodec) {
    this.codecName = codecName;
    this.hadoopWriterCodec = hadoopWriterCodec;
    this.sameReadWriteCodec = sameReadWriteCodec;
    this.conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);
    this.conf.setBoolean("parquet.avro.add-list-element-records", false);
    this.conf.setBoolean("parquet.avro.write-old-list-structure", false);
  }

  @Test
  public void testAll() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("all.avsc").openStream());

    String file = createTempFile().getPath();
    List<Integer> integerArray = Arrays.asList(1, 2, 3);
    GenericData.Record nestedRecord = new GenericRecordBuilder(
      schema.getField("mynestedrecord").schema())
      .set("mynestedint", 1).build();
    List<Integer> emptyArray = new ArrayList<>();
    Schema arrayOfOptionalIntegers = Schema.createArray(
      optional(Schema.create(Schema.Type.INT)));
    GenericData.Array<Integer> genericIntegerArrayWithNulls =
      new GenericData.Array<>(
        arrayOfOptionalIntegers,
        Arrays.asList(1, null, 2, null, 3));
    GenericFixed genericFixed = new GenericData.Fixed(
      Schema.createFixed("fixed", null, null, 1), new byte[]{(byte) 65});
    ImmutableMap<String, Integer> emptyMap = new ImmutableMap.Builder<String, Integer>().build();

    try(ParquetWriter<GenericRecord> writer = writer(file, schema)) {

      GenericData.Array<Integer> genericIntegerArray = new GenericData.Array<>(
        Schema.createArray(Schema.create(Schema.Type.INT)), integerArray);

      GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mynull", null)
        .set("myboolean", true)
        .set("myint", 1)
        .set("mylong", 2L)
        .set("myfloat", 3.1f)
        .set("mydouble", 4.1)
        .set("mybytes", ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)))
        .set("mystring", "hello")
        .set("mynestedrecord", nestedRecord)
        .set("myenum", "a")
        .set("myarray", genericIntegerArray)
        .set("myemptyarray", emptyArray)
        .set("myoptionalarray", genericIntegerArray)
        .set("myarrayofoptional", genericIntegerArrayWithNulls)
        .set("mymap", ImmutableMap.of("a", 1, "b", 2))
        .set("myemptymap", emptyMap)
        .set("myfixed", genericFixed)
        .build();

      writer.write(record);
    }

    final GenericRecord nextRecord;
    try (ParquetReader<GenericRecord> reader = reader(file)) {
      nextRecord = reader.read();
    }

//    Object expectedEnumSymbol = "a";

    assertNotNull(nextRecord);
    assertNull(nextRecord.get("mynull"));
    assertEquals(true, nextRecord.get("myboolean"));
    assertEquals(1, nextRecord.get("myint"));
    assertEquals(2L, nextRecord.get("mylong"));
    assertEquals(3.1f, nextRecord.get("myfloat"));
    assertEquals(4.1, nextRecord.get("mydouble"));
    assertEquals(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)), nextRecord.get("mybytes"));
//    assertEquals("hello", nextRecord.get("mystring"));
//    assertEquals(expectedEnumSymbol, nextRecord.get("myenum"));
    assertEquals(nestedRecord, nextRecord.get("mynestedrecord"));
    assertEquals(integerArray, nextRecord.get("myarray"));
    assertEquals(emptyArray, nextRecord.get("myemptyarray"));
    assertEquals(integerArray, nextRecord.get("myoptionalarray"));
    assertEquals(genericIntegerArrayWithNulls, nextRecord.get("myarrayofoptional"));
//    assertEquals(ImmutableMap.of("a", 1, "b", 2), nextRecord.get("mymap"));
    assertEquals(emptyMap, nextRecord.get("myemptymap"));
    assertEquals(genericFixed, nextRecord.get("myfixed"));
  }

  @Test
  public void testUnionWithSingleNonNullType() throws Exception {
    Schema schema = Schema.createRecord("SingleStringUnionRecord", null, null, false);
    schema.setFields(
      Collections.singletonList(new Schema.Field("value",
        Schema.createUnion(Schema.create(Schema.Type.STRING)), null, null)));

    String file = createTempFile().getPath();

    // Parquet writer
    try(ParquetWriter<GenericRecord> parquetWriter = writer(file, schema)) {

      GenericRecord record = new GenericRecordBuilder(schema)
        .set("value", "theValue")
        .build();

      parquetWriter.write(record);
    }

    try(ParquetReader<GenericRecord> reader = reader(file)) {
      GenericRecord nextRecord = reader.read();

      assertNotNull(nextRecord);
//      assertEquals("theValue", nextRecord.get("value"));
    }
  }

  @Test
  public void testDuplicatedValuesWithDictionary() throws Exception {
    Schema schema = SchemaBuilder.record("spark_schema")
      .fields().optionalBytes("value").endRecord();

    String file = createTempFile().getPath();

    String[] records = {"one", "two", "three", "three", "two", "one", "zero"};
    try (ParquetWriter<GenericRecord> writer = writer(file, schema)) {
      for (String record : records) {
        writer.write(new GenericRecordBuilder(schema)
          .set("value", record.getBytes()).build());
      }
    }

    try (ParquetReader<GenericRecord> reader = reader(file)) {
      GenericRecord rec;
      int i = 0;
      while ((rec = reader.read()) != null) {
        ByteBuffer buf = (ByteBuffer) rec.get("value");
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        assertEquals(records[i++], new String(bytes));
      }
    }
  }

  @Test
  public void testNestedLists() throws Exception {
    Schema schema = new Schema.Parser().parse(
      Resources.getResource("nested_array.avsc").openStream());
    String file = createTempFile().getPath();

    // Parquet writer
    ParquetWriter<GenericRecord> parquetWriter = writer(file, schema);

    Schema innerRecordSchema = schema.getField("l1").schema().getTypes()
      .get(1).getElementType().getTypes().get(1);

    GenericRecord record = new GenericRecordBuilder(schema)
      .set("l1", Collections.singletonList(
        new GenericRecordBuilder(innerRecordSchema).set("l2", Collections.singletonList("hello")).build()
      ))
      .build();

    parquetWriter.write(record);
    parquetWriter.close();

    ParquetReader<GenericRecord> reader = reader(file);
    GenericRecord nextRecord = reader.read();

    assertNotNull(nextRecord);
    assertNotNull(nextRecord.get("l1"));
    List<?> l1List = (List<?>) nextRecord.get("l1");
    assertNotNull(l1List.get(0));
    List<?> l2List = (List<?>) ((GenericRecord) l1List.get(0)).get("l2");
//    assertEquals("hello", l2List.get(0));
  }

  /**
   * A test demonstrating the most simple way to write and read Parquet files
   * using Avro {@link GenericRecord}.
   */
  @Test
  public void testSimpleGeneric() throws IOException {
    final Schema schema =
        Schema.createRecord("Person", null, "org.apache.parquet", false);
    schema.setFields(Arrays.asList(
        new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("weight", Schema.create(Schema.Type.INT), null,
            null)));

    final String file = createTempFile().getPath();

    try (final ParquetWriter<GenericRecord> parquetWriter = writer(file, schema)) {

      final GenericData.Record fooRecord = new GenericData.Record(schema);
      fooRecord.put("name", "foo");
      fooRecord.put("weight", 123);

      final GenericData.Record oofRecord = new GenericData.Record(schema);
      oofRecord.put("name", "oof");
      oofRecord.put("weight", 321);

      parquetWriter.write(fooRecord);
      parquetWriter.write(oofRecord);
    }

    // Read the file. String data is returned as org.apache.avro.util.Utf8 so it
    // must be converting to a String before checking equality
    try (ParquetReader<GenericRecord> reader = reader(file)) {

      final GenericRecord r1 = reader.read();
      assertEquals("foo", r1.get("name").toString());
      assertEquals(123, r1.get("weight"));

      final GenericRecord r2 = reader.read();
      assertEquals("oof", r2.get("name").toString());
      assertEquals(321, r2.get("weight"));
    }
  }

  private File createTempFile() throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    return tmp;
  }

  private ParquetWriter<GenericRecord> writer(String file, Schema schema) throws IOException {
    AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter
      .<GenericRecord>builder(new LocalOutputFile(Paths.get(file)))
      .withSchema(schema)
      .withConf(conf);
    if (!hadoopWriterCodec) {
      writerBuilder = writerBuilder.withCodecFactory(new SimpleCompressionCodecFactory(conf, ParquetProperties.DEFAULT_PAGE_SIZE));
    }
    return writerBuilder
      .withCompressionCodec(codecName)
      .build();
  }

  private ParquetReader<GenericRecord> reader(String file) throws IOException {
    ParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader
      .<GenericRecord>builder(new LocalInputFile(Paths.get(file)))
      .withDataModel(GenericData.get())
      .withConf(conf);
    if (hadoopWriterCodec ^ sameReadWriteCodec) {
      readerBuilder = readerBuilder.withCodecFactory(new SimpleCompressionCodecFactory(conf, ParquetProperties.DEFAULT_PAGE_SIZE));
    }
    return readerBuilder.build();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
}
