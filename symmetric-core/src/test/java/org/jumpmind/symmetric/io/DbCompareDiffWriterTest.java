package org.jumpmind.symmetric.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;

import org.jumpmind.db.DdlReaderTestConstants;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

public class DbCompareDiffWriterTest {
    protected OutputStream stream;
    protected ISymmetricEngine targetEngine;
    protected DbCompareTables tables;
    protected DbCompareDiffWriter test;

    @BeforeEach
    public void setUp() throws Exception {
        this.stream = mock(OutputStream.class);
        this.targetEngine = mock(ISymmetricEngine.class);
        this.tables = mock(DbCompareTables.class);
        this.test = new DbCompareDiffWriter(targetEngine, tables, stream);
    }
    
    @Test
    void dbCompareDiffWriterWriteDeleteTest() throws Exception {
        // Mocks
        DbCompareRow targetCompareRow = mock(DbCompareRow.class);
        Table mockTable = mock(Table.class);
        Table mockTable2 = mock(Table.class);
        DmlStatement mockDmlStatement = mock(DmlStatement.class);
        Column mockColumn = mock(Column.class);
        Column mockColumn2 = mock(Column.class);
        IDatabasePlatform dbPlatform = mock(IDatabasePlatform.class);
        // Whens
        when(targetCompareRow.getTable()).thenReturn(mockTable);
        when(targetEngine.getDatabasePlatform()).thenReturn(dbPlatform);
        when(dbPlatform.createDmlStatement(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(mockDmlStatement);
        when(targetCompareRow.getTable().getPrimaryKeyColumnCount()).thenReturn(1);
        when(mockTable.getColumn(ArgumentMatchers.anyInt())).thenReturn(mockColumn);
        when(mockColumn.getName()).thenReturn("test");
        when(targetCompareRow.getTable()).thenReturn(mockTable2);
        when(mockTable2.getColumn(ArgumentMatchers.anyInt())).thenReturn(mockColumn2);
        when(mockColumn2.getName()).thenReturn("test2");
        when(mockDmlStatement.buildDynamicDeleteSql(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn("testing");
        ArgumentCaptor<byte[]> valueCapture = ArgumentCaptor.forClass(byte[].class);
        doNothing().when(stream).write(valueCapture.capture());
        // Test
        test.writeDelete(targetCompareRow);
        String expectedString1 = "testing";
        String expectedString2 = "\r\n";
        byte[] expectedByte1 = expectedString1.getBytes();
        byte[] expectedByte2 = expectedString2.getBytes();
        List<byte[]> byteList = valueCapture.getAllValues();
        assertArrayEquals(expectedByte1, byteList.get(0));
        assertArrayEquals(expectedByte2, byteList.get(1));
    }

    @Test
    void dbCompareDiffWriterWriteInsertTest() throws Exception {
        // Mocks
        DbCompareRow sourceCompareRow = mock(DbCompareRow.class);
        LinkedHashMap<Column, Column> mockColumnMapping = mock(LinkedHashMap.class);
        IDatabasePlatform dbPlatform = mock(IDatabasePlatform.class);
        DmlStatement mockDmlStatement = mock(DmlStatement.class);
        // Tables
        Table targetTable = new Table();
        targetTable.setName(DdlReaderTestConstants.TESTNAME);
        targetTable.setType(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        targetTable.setCatalog(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        targetTable.setSchema(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        targetTable.setDescription(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        Column testTargetColumn = new Column();
        testTargetColumn.setDefaultValue("test");
        testTargetColumn.setName(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        testTargetColumn.setJdbcTypeName("test");
        testTargetColumn.setSize("1");
        testTargetColumn.setAutoIncrement(false);
        testTargetColumn.setJdbcTypeCode(1);
        testTargetColumn.setMappedType(TypeMap.VARCHAR);
        testTargetColumn.setPrecisionRadix(10);
        testTargetColumn.setPrimaryKeySequence(1);
        testTargetColumn.setPrimaryKey(true);
        targetTable.addColumn(testTargetColumn);
        Table sourceTable = new Table();
        sourceTable.setName(DdlReaderTestConstants.TESTNAME);
        sourceTable.setType(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        sourceTable.setCatalog(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        sourceTable.setSchema(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        sourceTable.setDescription(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        Column testSourceColumn = new Column();
        testSourceColumn.setDefaultValue("test");
        testSourceColumn.setName(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        testSourceColumn.setJdbcTypeName("test");
        testSourceColumn.setSize("1");
        testSourceColumn.setAutoIncrement(false);
        testSourceColumn.setJdbcTypeCode(1);
        testSourceColumn.setMappedType(TypeMap.VARCHAR);
        testSourceColumn.setPrecisionRadix(10);
        testSourceColumn.setPrimaryKeySequence(1);
        testSourceColumn.setPrimaryKey(true);
        sourceTable.addColumn(testSourceColumn);
        // Whens
        when(tables.getTargetTable()).thenReturn(targetTable);
        when(tables.getColumnMapping()).thenReturn(mockColumnMapping);
        when(mockColumnMapping.containsValue(ArgumentMatchers.any(Column.class))).thenReturn(true);
        when(targetEngine.getDatabasePlatform()).thenReturn(dbPlatform);
        when(dbPlatform.createDmlStatement(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(mockDmlStatement);
        when(tables.getSourceTable()).thenReturn(sourceTable);
        when(mockDmlStatement.buildDynamicSql(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn("testing");
        ArgumentCaptor<byte[]> valueCapture = ArgumentCaptor.forClass(byte[].class);
        doNothing().when(stream).write(valueCapture.capture());
        // Test
        test.writeInsert(sourceCompareRow);
        String expectedString1 = "testing";
        String expectedString2 = "\r\n";
        byte[] expectedByte1 = expectedString1.getBytes();
        byte[] expectedByte2 = expectedString2.getBytes();
        List<byte[]> byteList = valueCapture.getAllValues();
        assertArrayEquals(expectedByte1, byteList.get(0));
        assertArrayEquals(expectedByte2, byteList.get(1));
    }

    @Test
    void dbCompareDiffWriterWriteUpdateTest() throws Exception {
        // Mocks
        DbCompareRow targetCompareRow = mock(DbCompareRow.class);
        IDatabasePlatform dbPlatform = mock(IDatabasePlatform.class);
        DmlStatement mockDmlStatement = mock(DmlStatement.class);
        Row mockRow = mock(Row.class);
        // Variables
        LinkedHashMap<Column, String> deltas = new LinkedHashMap<Column, String>();
        Column column = new Column();
        column.setDefaultValue("test");
        column.setName(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        column.setJdbcTypeName("test");
        column.setSize("1");
        column.setAutoIncrement(false);
        column.setJdbcTypeCode(1);
        column.setMappedType(TypeMap.VARCHAR);
        column.setPrecisionRadix(10);
        column.setPrimaryKeySequence(1);
        column.setPrimaryKey(true);
        deltas.put(column, "test");
        // Tables
        Table targetTable = new Table();
        targetTable.setName(DdlReaderTestConstants.TESTNAME);
        targetTable.setType(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        targetTable.setCatalog(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        targetTable.setSchema(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        targetTable.setDescription(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        Column testTargetColumn = new Column();
        testTargetColumn.setDefaultValue("test");
        testTargetColumn.setName(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        testTargetColumn.setJdbcTypeName("test");
        testTargetColumn.setSize("1");
        testTargetColumn.setAutoIncrement(false);
        testTargetColumn.setJdbcTypeCode(1);
        testTargetColumn.setMappedType(TypeMap.VARCHAR);
        testTargetColumn.setPrecisionRadix(10);
        testTargetColumn.setPrimaryKeySequence(1);
        testTargetColumn.setPrimaryKey(true);
        targetTable.addColumn(testTargetColumn);
        // Whens
        when(targetCompareRow.getTable()).thenReturn(targetTable);
        when(targetEngine.getDatabasePlatform()).thenReturn(dbPlatform);
        when(dbPlatform.createDmlStatement(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(mockDmlStatement);
        when(targetCompareRow.getRow()).thenReturn(mockRow);
        when(mockRow.getString(ArgumentMatchers.anyString())).thenReturn("test");
        when(mockDmlStatement.buildDynamicSql(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn("testing");
        ArgumentCaptor<byte[]> valueCapture = ArgumentCaptor.forClass(byte[].class);
        doNothing().when(stream).write(valueCapture.capture());
        // Test
        test.writeUpdate(targetCompareRow, deltas);
        String expectedString1 = "testing";
        String expectedString2 = "\r\n";
        byte[] expectedByte1 = expectedString1.getBytes();
        byte[] expectedByte2 = expectedString2.getBytes();
        List<byte[]> byteList = valueCapture.getAllValues();
        assertArrayEquals(expectedByte1, byteList.get(0));
        assertArrayEquals(expectedByte2, byteList.get(1));
    }

    @Test
    void testSettersAndGettersMethods() throws Exception {
        test.setContinueAfterError(true);
        test.setError(true);
        test.setThrowable(new Throwable("test"));
        assertTrue(test.isContinueAfterError());
        assertTrue(test.isError());
        assertEquals(new Throwable("test").getMessage(), test.getThrowable().getMessage());
    }

    @Test
    void testWriteLineException() throws Exception {
        Exception exception = assertThrows(Exception.class, () -> {
            test.writeLine(null);
        });
        String expectedMessage = "failed to write to stream";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    void testWriteDeleteException() throws Exception {
        assertThrows(Exception.class, () -> {
            test.writeDelete(null);
        });
    }

    @Test
    void testWriteInsertException() throws Exception {
        assertThrows(Exception.class, () -> {
            test.writeInsert(null);
        });
    }

    @Test
    void testWriteUpdateException() throws Exception {
        assertThrows(Exception.class, () -> {
            test.writeUpdate(null, null);
        });
    }
}
