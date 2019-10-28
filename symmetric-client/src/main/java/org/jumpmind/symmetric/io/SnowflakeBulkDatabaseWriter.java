package org.jumpmind.symmetric.io;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;

public class SnowflakeBulkDatabaseWriter extends AbstractBulkDatabaseWriter {

    protected Table table = null;
    protected Table databaseTable = null;
    protected boolean needsBinaryConversion;
    protected boolean needsColumnsReordered;
    protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected String rowTerminator = "\r\n";
    protected String fieldTerminator = "|";
    protected int loadedRows = 0;
    protected int maxRowsBeforeFlush = 10000;
    protected String fileName = null;
    
    public SnowflakeBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, 
            String tablePrefix, IStagingManager stagingManager) {
        super(symmetricPlatform, targetPlatform, tablePrefix);
        this.stagingManager = stagingManager;
    }

    public boolean start(Table table) {
        this.table = table;
        if (super.start(table)) {
            if (sourceTable != null && targetTable == null) {
                String qualifiedName = sourceTable.getFullyQualifiedTableName();
                if (writerSettings.isIgnoreMissingTables()) {                    
                    if (!missingTables.contains(qualifiedName)) {
                        log.info("Did not find the {} table in the target database. This might have been part of a sql "
                                + "command (truncate) but will work if the fully qualified name was in the sql provided", qualifiedName);
                        missingTables.add(qualifiedName);
                    }
                } else {
                    throw new SymmetricException("Could not load the %s table.  It is not in the target database", qualifiedName);
                }
            }
            
            needsBinaryConversion = false;
            if (! batch.getBinaryEncoding().equals(BinaryEncoding.HEX) && targetTable != null) {
                for (Column column : targetTable.getColumns()) {
                    if (column.isOfBinaryType()) {
                        needsBinaryConversion = true;
                        break;
                    }
                }
            }
            databaseTable = getPlatform(table).getTableFromCache(sourceTable.getCatalog(), sourceTable.getSchema(),
                    sourceTable.getName(), false);
            if (targetTable != null && databaseTable != null) {
                String[] csvNames = targetTable.getColumnNames();
                String[] columnNames = databaseTable.getColumnNames();
                needsColumnsReordered = false;
                for (int i = 0; i < csvNames.length; i++) {
                    if (! csvNames[i].equals(columnNames[i])) {
                        needsColumnsReordered = true;
                        break;
                    }
                }
            }
            if (this.stagedInputFile == null) {
                createStagingFile();
            }
            return true;
        } else {
            return false;
        }
    }
    @Override
    protected void bulkWrite(CsvData data) {
        DataEventType dataEventType = data.getDataEventType();

        switch (dataEventType) {
            case INSERT:
                statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
                try {
                    String[] parsedData = data.getParsedData(CsvData.ROW_DATA);
                    if (needsBinaryConversion) {
                        Column[] columns = targetTable.getColumns();
                        for (int i = 0; i < columns.length; i++) {
                            if (columns[i].isOfBinaryType()) {
                                if (batch.getBinaryEncoding().equals(BinaryEncoding.BASE64) && parsedData[i] != null) {
                                    parsedData[i] = new String(Hex.encodeHex(Base64.decodeBase64(parsedData[i].getBytes())));
                                }
                            }
                        }
                    }
                    OutputStream out =  this.stagedInputFile.getOutputStream();
                    if (needsColumnsReordered) {
                        Map<String, String> mapData = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
                        String[] columnNames = databaseTable.getColumnNames();
                        for (int i = 0; i < columnNames.length; i++) {
                            String columnData = mapData.get(columnNames[i]);
                            if (columnData != null) {
                                out.write(columnData.getBytes());
                            }
                            if (i + 1 < columnNames.length) {
                                out.write(fieldTerminator.getBytes());
                            }
                        }
                    } else {
                        for (int i = 0; i < parsedData.length; i++) {
                            if (parsedData[i] != null) {
                                out.write(parsedData[i].getBytes());
                            }
                            if (i + 1 < parsedData.length) {
                                out.write(fieldTerminator.getBytes());
                            }
                        }
                    }
                    out.write(rowTerminator.getBytes());
                    loadedRows++;
                } catch (Exception ex) {
                    throw getPlatform(table).getSqlTemplate().translate(ex);
                } finally {
                    statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
                    statistics.get(batch).increment(DataWriterStatisticConstants.ROWCOUNT);
                    statistics.get(batch).increment(DataWriterStatisticConstants.LINENUMBER);
                }
                break;
            case UPDATE:
            case DELETE:
            default:
                flush();
                writeDefault(data);
                break;
        }

        if (loadedRows >= maxRowsBeforeFlush) {
            flush();
        }
    }

    protected void flush() {
        if (loadedRows > 0) {
                this.stagedInputFile.close();
                
            statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
            String filename = stagedInputFile.getFile().getAbsolutePath();
            
            try {
                String fileFormat = String.format("create or replace file format public.symds_csv_format type = csv "
                        + "field_delimiter = '%s' ", fieldTerminator);
                
                String putCommand = String.format("put file://%s @public."
                        + "symds_stage;", filename);
                
                String copyCommand = String.format("copy into public.%s from '@public.symds_stage/%s' file_format = (format_name='public.symds_csv_format')", 
                        targetTable.getName(), fileName);
                
                
                JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) getTargetTransaction();
                Connection c = jdbcTransaction.getConnection();
                
                Statement stmt = c.createStatement();
                
                stmt.execute(fileFormat);
                stmt.execute(putCommand);
                stmt.execute(copyCommand);
                
                stmt.close();
                
                loadedRows = 0;
            } catch (SQLException ex) {
                throw getPlatform().getSqlTemplate().translate(ex);
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
                this.stagedInputFile.delete();
            }
        }
    }
    
    @Override
    public void end(Table table) {

        try {
            flush();
            this.stagedInputFile.close();
            this.stagedInputFile.delete();
        } finally {
            super.end(table);
        }
    }
    protected void createStagingFile() {
        this.fileName = table.getName() + this.getBatch().getBatchId() + ".csv";
        this.stagedInputFile = stagingManager.create("bulkloaddir",fileName);
    }
}
