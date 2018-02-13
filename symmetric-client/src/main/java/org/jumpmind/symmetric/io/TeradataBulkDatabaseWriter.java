package org.jumpmind.symmetric.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.db.util.ResettableBasicDataSource;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;

public class TeradataBulkDatabaseWriter extends AbstractBulkDatabaseWriter {
	protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected String rowTerminator = "\r\n";
    protected String fieldTerminator = ",";
    protected int loadedRows = 0;
    protected boolean needsBinaryConversion;
    protected boolean columnHeaderWritten;
    protected Table table = null;
    protected int totalBytes;
    
	public TeradataBulkDatabaseWriter(IDatabasePlatform symmetricPlatform,
			IDatabasePlatform tar, String tablePrefix,
			IStagingManager stagingManager) {
		super(symmetricPlatform, tar, tablePrefix);
		this.stagingManager = stagingManager;
	}
	
	public boolean start(Table table) {
        this.table = table;
        if (super.start(table)) {
            if (sourceTable != null && targetTable == null) {
                String qualifiedName = sourceTable.getFullyQualifiedTableName();
                if (writerSettings.isIgnoreMissingTables()) {                    
                    if (!missingTables.contains(qualifiedName)) {
                        log.warn("Did not find the {} table in the target database", qualifiedName);
                        missingTables.add(qualifiedName);
                    }
                } else {
                    throw new SymmetricException("Could not load the %s table.  It is not in the target database", qualifiedName);
                }
            }
            
            needsBinaryConversion = false;
            if (! batch.getBinaryEncoding().equals(BinaryEncoding.HEX)) {
                for (Column column : targetTable.getColumns()) {
                    if (column.isOfBinaryType()) {
                        needsBinaryConversion = true;
                        break;
                    }
                }
            }

            columnHeaderWritten=false;
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
                    if (!columnHeaderWritten) {
                        String[] columnNames = targetTable.getColumnNames();
                        for (int i = 0; i < columnNames.length; i++) {
                            if (columnNames[i] != null) {
                                out.write(columnNames[i].getBytes());
                                totalBytes+=columnNames[i].getBytes().length;
                            }
                            if (i + 1 < columnNames.length) {
                                out.write(fieldTerminator.getBytes());
                                totalBytes+=fieldTerminator.getBytes().length;
                            }
                        }
                        out.write(rowTerminator.getBytes());
                        totalBytes+=rowTerminator.getBytes().length;
                        
                        columnHeaderWritten=true;
                    } 
                    
                    for (int i = 0; i < parsedData.length; i++) {
                        if (parsedData[i] != null) {
                            out.write(parsedData[i].getBytes());
                            totalBytes+=parsedData[i].getBytes().length;
                        }
                        if (i + 1 < parsedData.length) {
                            out.write(fieldTerminator.getBytes());
                            totalBytes+=fieldTerminator.getBytes().length;
                        }
                    }
                    
                    out.write(rowTerminator.getBytes());
                    totalBytes+=(rowTerminator.getBytes().length);
                    
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
    }
    
    protected void flush() {
        if (loadedRows > 0) {
        		
        		this.stagedInputFile.close();
            statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
            Connection c = null;
            ResettableBasicDataSource ds = null;
            PreparedStatement ps = null;
            String fastLoadConnectionString = "TMODE=ANSI,CHARSET=UTF8,TYPE=FASTLOADCSV";
            DatabaseInfo dbInfo = getPlatform().getDatabaseInfo();
            String quote = dbInfo.getDelimiterToken();
            String catalogSeparator = dbInfo.getCatalogSeparator();
            String schemaSeparator = dbInfo.getSchemaSeparator();
            String tableName = this.getTargetTable().getQualifiedTableName(quote, catalogSeparator, schemaSeparator);
            
	        try {
	        		cleanUpFastLoadTables(tableName, false);
	        	
	        		IDatabasePlatform platform = getPlatform();
	            ds = ((ResettableBasicDataSource) platform.getDataSource());
	            
	            boolean containsCommas = ds.getUrl().indexOf(",") > 0;
	            boolean lastCharSlash = ds.getUrl().charAt(ds.getUrl().length() - 1) == '/';
	            
	            fastLoadConnectionString = (lastCharSlash && containsCommas ? "," : lastCharSlash ? "" : "/") + fastLoadConnectionString;
	            
	            if (ds.getUrl().indexOf(fastLoadConnectionString) < 0) {
	            		ds.setUrl(ds.getUrl() + fastLoadConnectionString);
	            }
	            
	            c = DriverManager.getConnection(ds.getUrl(), ds.getUsername(), ds.getPassword());
	            
	            String sql = String.format("INSERT INTO " + tableName + " VALUES " + buildSql());
	            
	            ps = c.prepareStatement(sql);
	            
	            InputStream dataStream = new FileInputStream(this.stagedInputFile.getFile());
	            ps.setAsciiStream(1, dataStream, -1);
	            ps.executeUpdate();
	            
	            log.info("Fast load complete.");
	        } catch (SQLException e) {
	           while (e != null)
	            {
	                log.error("SQL State = "
	                    + e.getSQLState()
	                    + ", Error Code = "
	                    + e.getErrorCode());
	                e = e.getNextException();
	                if (e.getErrorCode() == 2636) {
	                		log.warn("In order to use the teradata bulk loader the target table " + tableName + " must me empty");
	                }
	            }

	            throw getPlatform().getSqlTemplate().translate(e);
	        } catch (Exception ex) {
	        		
	        } finally {
	            statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
	            totalBytes=0;
	            
	            cleanUpFastLoadTables(tableName, false);

	            if (c != null) {
	            		try {
	            			c.close();
	            		}
	            		catch (SQLException sqle) {
	            			log.error("Unable to close teradata connection", sqle);
	            		}
	            }
	            if (ps != null) {
	            		try {
	            			ps.close();
	            		}
	            		catch (SQLException sqle) {
	            			log.error("Unable to close teradata prepared statement", sqle);
	            		}
	            }
	        }
        }
    }
    
    protected void cleanUpFastLoadTables(String tableName, boolean clearTargetTable) {
    		JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) getTargetTransaction();
        Connection normalConnection = jdbcTransaction.getConnection();
        
        if (clearTargetTable) {
	        try {
	        		Statement stmt = normalConnection.createStatement();
		    		stmt.execute("delete from " + tableName);
		    		stmt.close();
	        }
	        catch (Exception e) {
	        		log.info("Unable to delete from teradata table + " + tableName + " before fast load.");
	        }
        }
        
        try {
        		normalConnection.setAutoCommit(true);
    			Statement stmt = normalConnection.createStatement();
        		stmt.execute("drop table " + tableName + "_ERR_1");
        		stmt.close();
        }
        catch (Exception e) { }
        
        try {
        		normalConnection.setAutoCommit(true);
    			Statement stmt = normalConnection.createStatement();
        		stmt.execute("drop table " + tableName + "_ERR_2");
        		stmt.close();
        }
        catch (Exception e) { }
    }
    
    protected String buildSql() {
    		StringBuffer sql = new StringBuffer("(");
    		for (int i=0; i < this.getTargetTable().getColumnCount(); i++) {
    			if (i>0) {
    				sql.append(",");
    			}
    			sql.append("?");
    		}
    		sql.append(")");
    		return sql.toString();
    }
    
    protected void createStagingFile() {
        this.stagedInputFile = stagingManager.create("bulkloaddir",
                table.getName() + this.getBatch().getBatchId() + ".csv");
    }

}
