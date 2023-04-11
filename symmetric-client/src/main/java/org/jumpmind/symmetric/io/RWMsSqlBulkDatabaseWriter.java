package org.jumpmind.symmetric.io;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class RWMsSqlBulkDatabaseWriter extends MsSqlBulkDatabaseWriter {
	private String fieldDelimiter;
	private String rowDelimiter;

	public RWMsSqlBulkDatabaseWriter(IDatabasePlatform platform, IStagingManager stagingManager,
			NativeJdbcExtractor jdbcExtractor, int maxRowsBeforeFlush, boolean fireTriggers, String uncPath,
			String fieldDelimiter, String rowDelimiter) {
		super(platform, stagingManager, jdbcExtractor, maxRowsBeforeFlush, fireTriggers, uncPath);
		this.fieldDelimiter = fieldDelimiter;
		this.rowDelimiter = rowDelimiter;
	}
	
	@Override
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
                            	if (fieldDelimiter != null) {
                            		out.write(fieldDelimiter.getBytes());
                            	} else {
                            		out.write(DELIMITER);
                            	}
                            }
                        }
                    } else {
                        for (int i = 0; i < parsedData.length; i++) {
                            if (parsedData[i] != null) {
                                out.write(parsedData[i].getBytes());
                            }
                            if (i + 1 < parsedData.length) {
                            	if (fieldDelimiter != null) {
                            		out.write(fieldDelimiter.getBytes());
                            	} else {
                            		out.write(DELIMITER);
                            	}
                            }
                        }
                    }
                    if (rowDelimiter != null) {
                    	out.write(rowDelimiter.getBytes());
                    } else {
	                    out.write('\r');
	                    out.write('\n');
                    }
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
	
	@Override
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
	            DatabaseInfo dbInfo = platform.getDatabaseInfo();
	            String quote = dbInfo.getDelimiterToken();
	            String catalogSeparator = dbInfo.getCatalogSeparator();
	            String schemaSeparator = dbInfo.getSchemaSeparator();
	            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) transaction;
	            Connection c = jdbcTransaction.getConnection();
	            String fieldTerminator = (fieldDelimiter == null ? "||" : fieldDelimiter);
	            String rowTerminator = (rowDelimiter == null ? "\r\n" : rowDelimiter);
	            String sql = String.format("BULK INSERT " + 
	            		this.getTargetTable().getQualifiedTableName(quote, catalogSeparator, schemaSeparator) + 
	            		" FROM '" + filename) + "'" +
	            		" WITH ( FIELDTERMINATOR='" + fieldTerminator + "', ROWTERMINATOR='" + rowTerminator + "', KEEPIDENTITY " + (fireTriggers ? ", FIRE_TRIGGERS" : "") + ");";
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

}
