package org.jumpmind.symmetric.io.data.writer;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class MsSqlBulkDatabaseWriter extends DatabaseWriter {

    protected static final byte[] DELIMITER = "||".getBytes();
    protected NativeJdbcExtractor jdbcExtractor;
    protected int maxRowsBeforeFlush;
    protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected int loadedRows = 0;
    protected boolean fireTriggers;
    protected String uncPath;
    protected boolean needsBinaryConversion;
    protected boolean needsColumnsReordered;
    protected Table table = null;
    protected Table databaseTable = null;
    
	public MsSqlBulkDatabaseWriter(IDatabasePlatform platform,
			IStagingManager stagingManager, NativeJdbcExtractor jdbcExtractor,
			int maxRowsBeforeFlush, boolean fireTriggers, String uncPath) {
		super(platform);
		this.jdbcExtractor = jdbcExtractor;
		this.maxRowsBeforeFlush = maxRowsBeforeFlush;
		this.stagingManager = stagingManager;
		this.fireTriggers = fireTriggers;
		this.uncPath = uncPath;
	}

    public boolean start(Table table) {
        this.table = table;
        if (super.start(table)) {
            needsBinaryConversion = false;
            if (! batch.getBinaryEncoding().equals(BinaryEncoding.HEX)) {
	            for (Column column : targetTable.getColumns()) {
	                if (column.isOfBinaryType()) {
	                    needsBinaryConversion = true;
	                    break;
	                }
	            }
            }
            databaseTable = platform.getTableFromCache(sourceTable.getCatalog(), sourceTable.getSchema(),
                    sourceTable.getName(), false);
            String[] csvNames = targetTable.getColumnNames();
            String[] columnNames = databaseTable.getColumnNames();
            needsColumnsReordered = false;
            for (int i = 0; i < csvNames.length; i++) {
                if (! csvNames[i].equals(columnNames[i])) {
                    needsColumnsReordered = true;
                    break;
                }
            }
        	//TODO: Did this because start is getting called multiple times
        	//      for the same table in a single batch before end is being called
        	if (this.stagedInputFile == null) {
        		createStagingFile();
        	}
            return true;
        } else {
            return false;
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

    public void write(CsvData data) {
        DataEventType dataEventType = data.getDataEventType();

        switch (dataEventType) {
            case INSERT:
                statistics.get(batch).increment(DataWriterStatisticConstants.STATEMENTCOUNT);
                statistics.get(batch).increment(DataWriterStatisticConstants.LINENUMBER);
                statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
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
                                out.write(DELIMITER);
                            }
                        }
                    } else {
                        for (int i = 0; i < parsedData.length; i++) {
                            if (parsedData[i] != null) {
                                out.write(parsedData[i].getBytes());
                            }
                            if (i + 1 < parsedData.length) {
                                out.write(DELIMITER);
                            }
                        }
                    }
                    out.write('\r');
                    out.write('\n');
                    loadedRows++;
                } catch (Exception ex) {
                    throw getPlatform().getSqlTemplate().translate(ex);
                } finally {
                    statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                }
                break;
            case UPDATE:
            case DELETE:
            default:
                flush();
                super.write(data);
                break;
        }

        if (loadedRows >= maxRowsBeforeFlush) {
            flush();
        }
    }
    
    protected void flush() {
        if (loadedRows > 0) {
        	this.stagedInputFile.close();
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String filename;
            if (StringUtils.isEmpty(uncPath)) {
                filename = stagedInputFile.getFile().getAbsolutePath();
            } else {
                filename = uncPath + "\\" + stagedInputFile.getFile().getName();
            }
	        try {
	            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) transaction;
	            Connection c = jdbcTransaction.getConnection();
	            String sql = String.format("BULK INSERT " + 
	            		this.getTargetTable().getFullyQualifiedTableName() + 
	            		" FROM '" + filename) + "'" +
                        " WITH ( FIELDTERMINATOR='||', KEEPIDENTITY " + (fireTriggers ? ", FIRE_TRIGGERS" : "") + ");";
	            Statement stmt = c.createStatement();
	
	            //TODO:  clean this up, deal with errors, etc.?
	            stmt.execute(sql);
	            stmt.close();
	
	        } catch (SQLException ex) {
	            throw platform.getSqlTemplate().translate(ex);
	        } finally {
	            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
	        }
	        this.stagedInputFile.delete();
	        createStagingFile();
            loadedRows = 0;
        }
    }
    
    protected void createStagingFile() {
    	//TODO: We should use constants for dir structure path, 
    	//      but we don't want to depend on symmetric core.
        this.stagedInputFile = stagingManager.create(0, "bulkloaddir",
                table.getName() + this.getBatch().getBatchId() + ".csv");
    }
        
}
