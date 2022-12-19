/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.parquet.reader.flat;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.ColumnReader;
import io.trino.parquet.reader.ColumnReaderFactory;
import io.trino.parquet.reader.PageReader;
import io.trino.parquet.reader.TestingColumnReader;
import io.trino.parquet.reader.TestingColumnReader.ColumnReaderFormat;
import io.trino.parquet.reader.TestingColumnReader.DataPageVersion;
import io.trino.parquet.reader.decoders.ValueDecoders;
import io.trino.spi.block.Block;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.parquet.ParquetEncoding.BIT_PACKED;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.TestingColumnReader.DataPageVersion.V1;
import static io.trino.parquet.reader.TestingColumnReader.PLAIN_WRITER;
import static io.trino.parquet.reader.TestingColumnReader.getDictionaryPage;
import static io.trino.parquet.reader.flat.IntColumnAdapter.INT_ADAPTER;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;

/**
 * The purpose of this class is to cover all code paths of FlatColumnReader with all supported data types.
 * Many methods have the same signature, even though they do not use some arguments. This is for the
 * purpose of reusing the testng data providers.
 */
public class TestFlatColumnReader
{
    private static final PrimitiveType TYPE = new PrimitiveType(REQUIRED, INT32, "");
    private static final PrimitiveField NULLABLE_FIELD = new PrimitiveField(INTEGER, false, new ColumnDescriptor(new String[] {}, TYPE, 0, 0), 0);
    private static final PrimitiveField FIELD = new PrimitiveField(INTEGER, true, new ColumnDescriptor(new String[] {}, TYPE, 0, 0), 0);

    @Test
    public void testReadPageV1BitPacked()
            throws IOException
    {
        FlatColumnReader<int[]> reader = new FlatColumnReader<>(NULLABLE_FIELD, ValueDecoders::getIntDecoder, INT_ADAPTER);
        reader.setPageReader(getSimplePageReaderMock(BIT_PACKED), Optional.empty());
        reader.prepareNextRead(1);

        assertThatThrownBy(reader::readNullable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid definition level encoding: BIT_PACKED");
    }

    @Test
    public void testReadPageV1BitPackedNoNulls()
            throws IOException
    {
        FlatColumnReader<int[]> reader = new FlatColumnReader<>(FIELD, ValueDecoders::getIntDecoder, INT_ADAPTER);
        reader.setPageReader(getSimplePageReaderMock(BIT_PACKED), Optional.empty());
        reader.prepareNextRead(1);

        reader.readNoNull();
    }

    @Test
    public void testReadPageV1RleNoNulls()
            throws IOException
    {
        FlatColumnReader<int[]> reader = new FlatColumnReader<>(FIELD, ValueDecoders::getIntDecoder, INT_ADAPTER);
        assertThat(FIELD.isRequired()).isTrue();
        assertThat(FIELD.getDescriptor().getMaxDefinitionLevel()).isEqualTo(0);
        reader.setPageReader(getSimplePageReaderMock(RLE), Optional.empty());
        reader.prepareNextRead(1);

        Block block = reader.readNoNull().getBlock();
        assertThat(block.getPositionCount()).isEqualTo(1);
        assertThat(block.getInt(0, 0)).isEqualTo(42);
    }

    @Test
    public void testReadPageV1RleOnlyNulls()
            throws IOException
    {
        FlatColumnReader<int[]> reader = new FlatColumnReader<>(NULLABLE_FIELD, ValueDecoders::getIntDecoder, INT_ADAPTER);
        assertThat(NULLABLE_FIELD.isRequired()).isFalse();
        assertThat(NULLABLE_FIELD.getDescriptor().getMaxDefinitionLevel()).isEqualTo(0);
        reader.setPageReader(getNullOnlyPageReaderMock(), Optional.empty());
        reader.prepareNextRead(1);

        Block block = reader.readNullable().getBlock();
        assertThat(block.getPositionCount()).isEqualTo(1);
        assertThat(block.isNull(0)).isTrue();
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionary(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values = format.write(dictionaryWriter, new Integer[] {1, 1});
        DataPage page = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 2);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page)), dictionaryPage), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        format.assertBlock(values, actual);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionaryNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values = format.write(dictionaryWriter, new Integer[] {1, null});
        DataPage page = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, false, true);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page)), dictionaryPage), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        format.assertBlock(values, actual);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionaryNullableWithNoNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values = format.write(dictionaryWriter, new Integer[] {1, 1});
        DataPage page = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, false, false);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page)), dictionaryPage), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        format.assertBlock(values, actual);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionaryNullableWithNoNullsUsingColumnStats(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values = format.write(dictionaryWriter, new Integer[] {1, 1});
        DataPage page = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, false, false);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page)), dictionaryPage, false), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        format.assertBlock(values, actual);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionaryNullableWithOnlyNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        // Write dummy value. Without it dictionary would end up null.
        format.write(dictionaryWriter, new Integer[] {42});
        T[] values = format.resetAndWrite(dictionaryWriter, new Integer[] {null, null});
        DataPage page = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, true, true);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page)), dictionaryPage), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        format.assertBlock(values, actual);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadNoNull(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1});
        DataPage page1 = createDataPage(version, PLAIN, writer, 1);
        T[] values2 = format.resetAndWrite(writer, new Integer[] {2, 3});
        DataPage page2 = createDataPage(version, PLAIN, writer, 2);
        T[] values3 = format.resetAndWrite(writer, new Integer[] {4});
        DataPage page3 = createDataPage(version, PLAIN, writer, 1);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page1, page2, page3)), null), Optional.empty());
        Block actual1 = readBlock(reader, 1); // Parquet/Trino page size the same
        Block actual2 = readBlock(reader, 1); // Parquet page bigger than Trino
        Block actual3 = readBlock(reader, 2); // Parquet page smaller than Trino

        format.assertBlock(values1, actual1);
        format.assertBlock(values2, actual2, 0, 0, 1);
        format.assertBlock(values2, actual3, 1, 0, 1);
        format.assertBlock(values3, actual3, 0, 1, 1);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionaryAndNonDictionaryInASingleChunk(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(dictionaryWriter, new Integer[] {1, 1});
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 2);
        T[] values2 = format.write(writer, new Integer[] {2});
        DataPage page2 = createDataPage(version, PLAIN, writer, 1);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page1, page2)), dictionaryPage), Optional.empty());
        reader.prepareNextRead(3);
        Block actual = reader.readPrimitive().getBlock();
        format.assertBlock(values1, actual, 0, 0, 2);
        format.assertBlock(values2, actual, 0, 2, 1);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadOnlyNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {null});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, true);
        T[] values2 = format.write(writer, new Integer[] {null, null});
        DataPage page2 = createNullableDataPage(version, PLAIN, writer, true, true);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page1, page2)), null), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2);
        Block actual2 = readBlock(reader, 1);

        format.assertBlock(values1, actual1, 0, 0, 1);
        format.assertBlock(values2, actual1, 0, 1, 1);
        format.assertBlock(values2, actual2, 1, 0, 1);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {null, 1});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, true, false);
        T[] values2 = format.resetAndWrite(writer, new Integer[] {null, 2, null});
        DataPage page2 = createNullableDataPage(version, PLAIN, writer, true, false, true);
        T[] values3 = format.resetAndWrite(writer, new Integer[] {3, null, 4});
        DataPage page3 = createNullableDataPage(version, PLAIN, writer, false, true, false);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page1, page2, page3)), null), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2);
        Block actual2 = readBlock(reader, 2);
        Block actual3 = readBlock(reader, 4);

        format.assertBlock(values1, actual1);
        format.assertBlock(values2, actual2, 0, 0, 2);
        format.assertBlock(values2, actual3, 2, 0, 1);
        format.assertBlock(values3, actual3, 0, 1, 3);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadNullableWithNoNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1, 2});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, false, false);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page1)), null), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2);

        format.assertBlock(values1, actual1);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadNullableWithNoNullsUsingColumnStats(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1, 2});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, false, false);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page1)), null, true), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2);
        assertThat(actual1.mayHaveNull()).isFalse();
        format.assertBlock(values1, actual1);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testMixedDictionaryAndOrdinary(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values1 = format.write(dictionaryWriter, new Integer[] {1, 2, 3});
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 3);
        ValuesWriter writer = format.getPlainWriter();
        T[] values2 = format.resetAndWrite(writer, new Integer[] {4, 5, 6});
        DataPage page2 = createDataPage(version, PLAIN, writer, 3);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page1, page2)), dictionaryPage), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2); // Only dictionary
        Block actual2 = readBlock(reader, 2); // Mixed
        Block actual3 = readBlock(reader, 2); // Only non-dictionary

        format.assertBlock(values1, actual1, 0, 0, 2);
        format.assertBlock(values1, actual2, 2, 0, 1);
        format.assertBlock(values2, actual2, 0, 1, 1);
        format.assertBlock(values2, actual3, 1, 0, 2);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testOnlyNullParquetPage(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, false);
        T[] values2 = format.resetAndWrite(writer, new Integer[] {null});
        DataPage page2 = createNullableDataPage(version, PLAIN, writer, true);
        T[] values3 = format.resetAndWrite(writer, new Integer[] {2});
        DataPage page3 = createNullableDataPage(version, PLAIN, writer, false);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page1, page2, page3)), null), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual = readBlock(reader, 3);

        format.assertBlock(values1, actual, 0, 0, 1);
        format.assertBlock(values2, actual, 0, 1, 1);
        format.assertBlock(values3, actual, 0, 2, 1);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSkip(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values1 = format.write(writer, new Integer[] {1, 2, 3});
        DataPage page1 = createDataPage(version, PLAIN, writer, 3);
        T[] values2 = format.resetAndWrite(dictionaryWriter, new Integer[] {4, 5, 6});
        DataPage page2 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 3);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(new LinkedList<>(List.of(page1, page2)), dictionaryPage), Optional.empty());
        reader.prepareNextRead(1); // skip
        Block actual = readBlock(reader, 2);
        reader.prepareNextRead(1); // skip
        Block actual2 = readBlock(reader, 2);

        format.assertBlock(values1, actual, 1, 0, 2);
        format.assertBlock(values2, actual2, 1, 0, 2);
    }

    private Block readBlock(ColumnReader reader, int valueCount)
    {
        reader.prepareNextRead(valueCount);
        Block block = reader.readPrimitive().getBlock();
        assertThat(block.getPositionCount()).isEqualTo(valueCount);
        return block;
    }

    private DataPage createDataPage(DataPageVersion version, ParquetEncoding encoding, ValuesWriter writer, int valueCount)
            throws IOException
    {
        Slice slice = Slices.wrappedBuffer(writer.getBytes().toByteArray());
        if (version == V1) {
            return new DataPageV1(slice, valueCount, slice.length(), OptionalLong.empty(), RLE, BIT_PACKED, encoding);
        }
        else {
            return new DataPageV2(
                    valueCount,
                    0,
                    valueCount,
                    EMPTY_SLICE,
                    EMPTY_SLICE,
                    encoding,
                    slice,
                    slice.length(),
                    OptionalLong.empty(),
                    null,
                    false);
        }
    }

    private DataPage createNullableDataPage(DataPageVersion version, ParquetEncoding encoding, ValuesWriter writer, boolean... isNull)
            throws IOException
    {
        int valueCount = isNull.length;
        RunLengthBitPackingHybridValuesWriter nullsWriter = new RunLengthBitPackingHybridValuesWriter(1, valueCount, valueCount, HeapByteBufferAllocator.getInstance());
        int nullCount = 0;
        for (int i = 0; i < valueCount; i++) {
            nullsWriter.writeInteger(isNull[i] ? 0 : 1);
            nullCount += isNull[i] ? 1 : 0;
        }
        byte[] nullBytes = nullsWriter.getBytes().toByteArray();
        byte[] valueBytes = writer.getBytes().toByteArray();

        if (version == V1) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            bytes.write(nullBytes);
            bytes.write(valueBytes);

            return new DataPageV1(Slices.wrappedBuffer(bytes.toByteArray()), valueCount, 4, OptionalLong.empty(), RLE, RLE, encoding);
        }
        else {
            return new DataPageV2(
                    valueCount,
                    nullCount,
                    valueCount,
                    EMPTY_SLICE,
                    Slices.wrappedBuffer(nullBytes, Integer.BYTES, nullBytes.length - Integer.BYTES), // Skip length
                    encoding,
                    Slices.wrappedBuffer(valueBytes),
                    nullBytes.length + valueBytes.length,
                    OptionalLong.empty(),
                    null,
                    false);
        }
    }

    private PrimitiveField createField(ColumnReaderFormat<?> format, boolean required)
    {
        PrimitiveBuilder<PrimitiveType> builder;
        if (required) {
            builder = Types.required(format.getTypeName());
        }
        else {
            builder = Types.optional(format.getTypeName());
        }
        builder = builder.length(format.getTypeLengthInBytes());
        if (format.getLogicalTypeAnnotation() != null) {
            builder = builder.as(format.getLogicalTypeAnnotation());
        }
        PrimitiveType primitiveType = builder.named("dummy");
        return new PrimitiveField(
                format.getTrinoType(),
                required,
                new ColumnDescriptor(new String[] {"dummy"}, primitiveType, 0, required ? 0 : 1),
                0);
    }

    private static PageReader getSimplePageReaderMock(ParquetEncoding encoding)
            throws IOException
    {
        LinkedList<DataPage> pages = new LinkedList<>();
        ValuesWriter writer = PLAIN_WRITER.apply(1);
        writer.writeInteger(42);
        byte[] valueBytes = writer.getBytes().toByteArray();
        pages.add(new DataPageV1(
                Slices.wrappedBuffer(valueBytes),
                1,
                valueBytes.length,
                OptionalLong.empty(),
                encoding,
                encoding,
                PLAIN));
        return new PageReader(UNCOMPRESSED, pages, null, 1, false);
    }

    private static PageReader getNullOnlyPageReaderMock()
            throws IOException
    {
        LinkedList<DataPage> pages = new LinkedList<>();
        RunLengthBitPackingHybridValuesWriter nullsWriter = new RunLengthBitPackingHybridValuesWriter(1, 1, 1, HeapByteBufferAllocator.getInstance());
        nullsWriter.writeInteger(0);
        byte[] nullBytes = nullsWriter.getBytes().toByteArray();
        pages.add(new DataPageV1(
                Slices.wrappedBuffer(nullBytes),
                1,
                nullBytes.length,
                OptionalLong.empty(),
                RLE,
                RLE,
                PLAIN));
        return new PageReader(UNCOMPRESSED, pages, null, 1, false);
    }

    private static PageReader getPageReaderMock(LinkedList<DataPage> dataPages, @Nullable DictionaryPage dictionaryPage)
    {
        return getPageReaderMock(dataPages, dictionaryPage, false);
    }

    private static PageReader getPageReaderMock(LinkedList<DataPage> dataPages, @Nullable DictionaryPage dictionaryPage, boolean hasNoNulls)
    {
        return new PageReader(
                UNCOMPRESSED,
                dataPages,
                dictionaryPage,
                dataPages.stream()
                        .mapToInt(DataPage::getValueCount)
                        .sum(),
                hasNoNulls);
    }

    private static ColumnReader createColumnReader(PrimitiveField field)
    {
        return ColumnReaderFactory.create(field, UTC, true);
    }
}
