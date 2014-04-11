package org.jumpmind.symmetric.io.data.writer;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
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

public class MsSqlBulkDatabaseWriter extends DefaultDatabaseWriter {

    protected NativeJdbcExtractor jdbcExtractor;
    protected int maxRowsBeforeFlush;
    protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected int loadedRows = 0;
    protected boolean fireTriggers;
    protected boolean needsBinaryConversion;
    protected Table table = null;

	public MsSqlBulkDatabaseWriter(IDatabasePlatform platform,
			IStagingManager stagingManager, NativeJdbcExtractor jdbcExtractor,
			int maxRowsBeforeFlush, boolean fireTriggers) {
		super(platform);
		this.jdbcExtractor = jdbcExtractor;
		this.maxRowsBeforeFlush = maxRowsBeforeFlush;
		this.stagingManager = stagingManager;
		this.fireTriggers = fireTriggers;
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
                    for (int i = 0; i < parsedData.length; i++) {
                        if (parsedData[i] != null) {
                            out.write(parsedData[i].getBytes());
                        }
                        if (i + 1 < parsedData.length) {
                            out.write("||".getBytes());
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
	        try {
	            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) transaction;
	            Connection c = jdbcTransaction.getConnection();
	            String sql = String.format("BULK INSERT " + 
	            		this.getTargetTable().getFullyQualifiedTableName() + 
	            		" FROM '" + stagedInputFile.getFile().getAbsolutePath()) + "'" +
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
