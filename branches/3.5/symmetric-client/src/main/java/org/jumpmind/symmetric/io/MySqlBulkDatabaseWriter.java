package org.jumpmind.symmetric.io;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class MySqlBulkDatabaseWriter extends DatabaseWriter {


    protected NativeJdbcExtractor jdbcExtractor;
    protected int maxRowsBeforeFlush;
    protected boolean isLocal;
    protected boolean isReplace;
    protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected int loadedRows = 0;

    public MySqlBulkDatabaseWriter(IDatabasePlatform platform,
            IStagingManager stagingManager, NativeJdbcExtractor jdbcExtractor,
            int maxRowsBeforeFlush, boolean isLocal, boolean isReplace) {
        super(platform);
        this.jdbcExtractor = jdbcExtractor;
        this.maxRowsBeforeFlush = maxRowsBeforeFlush;
        this.isLocal = isLocal;
        this.isReplace = isReplace;
        this.stagingManager = stagingManager;
    }

    public boolean start(Table table) {
        if (super.start(table)) {
        	//TODO: Did this because start is getting called multiple times
        	//      for the same table in a single batch before end is being called
        	if (this.stagedInputFile == null) {
        		createStagingFile(table);
        	}
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void end(Table table) {
        try {
            this.stagedInputFile.close();
        	flush();
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
                    String formattedData = CsvUtils.escapeCsvData(
                            data.getParsedData(CsvData.ROW_DATA), '\n', '"', 
                            CsvWriter.ESCAPE_MODE_DOUBLED);
                    this.stagedInputFile.getOutputStream().write(formattedData.getBytes());                   
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
            loadedRows = 0;
        }
    }
    
    protected void flush() {
        statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        if (loadedRows > 0) {
	        try {
	            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) transaction;
	            Connection c = jdbcTransaction.getConnection();
	            String sql = String.format("LOAD DATA " + (isLocal ? "LOCAL " : "") + 
	            		"INFILE '" + stagedInputFile.getFile().getAbsolutePath()).replace('\\', '/') + "' " + 
	            		(isReplace ? "REPLACE " : "IGNORE ") + "INTO TABLE " +
	            		this.getTargetTable().getFullyQualifiedTableName() +
	            		" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' STARTING BY ''";
	            Statement stmt = c.createStatement();
	
	            //TODO:  clean this up, deal with errors, etc.?
	            log.debug(sql);
	            stmt.execute(sql);
	            stmt.close();
	
	        } catch (SQLException ex) {
	            throw platform.getSqlTemplate().translate(ex);
	        } finally {
	            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
	        }
        }
    }
    
    protected void createStagingFile(Table table) {
    	//TODO: We should use constants for dir structure path, 
    	//      but we don't want to depend on symmetric core.
        this.stagedInputFile = stagingManager.create(0, "bulkloaddir",
                table.getName() + this.getBatch().getBatchId() + ".csv");
    }

}
