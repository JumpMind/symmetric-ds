/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.io;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DefaultDatabaseWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class MySqlBulkDatabaseWriter extends DefaultDatabaseWriter {


    protected NativeJdbcExtractor jdbcExtractor;
    protected int maxRowsBeforeFlush;
    protected long maxBytesBeforeFlush;
    protected boolean isLocal;
    protected boolean isReplace;
    protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected int loadedRows = 0;
    protected long loadedBytes = 0;
    protected boolean needsBinaryConversion;    
    protected Table table = null;

    public MySqlBulkDatabaseWriter(IDatabasePlatform platform,
            IStagingManager stagingManager, NativeJdbcExtractor jdbcExtractor,
            int maxRowsBeforeFlush, long maxBytesBeforeFlush, boolean isLocal, boolean isReplace) {
        super(platform);
        this.jdbcExtractor = jdbcExtractor;
        this.maxRowsBeforeFlush = maxRowsBeforeFlush;
        this.maxBytesBeforeFlush = maxBytesBeforeFlush;
        this.isLocal = isLocal;
        this.isReplace = isReplace;
        this.stagingManager = stagingManager;
    }

    public boolean start(Table table) {
    	this.table = table;
        if (super.start(table)) {
            needsBinaryConversion = false;
            if (! batch.getBinaryEncoding().equals(BinaryEncoding.NONE)) {
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
                    byte[] byteData = null;
                    if (needsBinaryConversion) {
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
                        writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
                        writer.setRecordDelimiter('\n');
                        writer.setTextQualifier('"');
                        writer.setUseTextQualifier(true);
                        writer.setForceQualifier(true);
                        writer.setNullString("\\N");
                        Column[] columns = targetTable.getColumns();
                        boolean lastWasBinary = false;
                        for (int i = 0; i < columns.length; i++) {
                            if (columns[i].isOfBinaryType() && parsedData[i] != null) {
                                if (i > 0) {
                                    out.write(',');
                                }
                                out.write('"');
                                if (batch.getBinaryEncoding().equals(BinaryEncoding.HEX)) {
                                    out.write(escape(Hex.decodeHex(parsedData[i].toCharArray())));
//                                    out.write(parsedData[i].getBytes());
                                } else if (batch.getBinaryEncoding().equals(BinaryEncoding.BASE64)) {
//                                    out.write(escape(Base64.decodeBase64(parsedData[i].getBytes())));
                                    out.write(new String(Hex.encodeHex(Base64.decodeBase64(parsedData[i].getBytes()))).getBytes());
                                }
                                out.write('"');
                                lastWasBinary = true;
                            } else {
//                                if (lastWasBinary) {
//                                    out.write(',');
//                                } 
                                writer.write(parsedData[i], true);
                                writer.flush();
                                lastWasBinary = false;
                            }
                        }
                        writer.endRecord();
                        writer.close();
                        byteData = out.toByteArray();
                    } else {
                            String formattedData = CsvUtils.escapeCsvData(parsedData, '\n', '"', CsvWriter.ESCAPE_MODE_BACKSLASH, "\\N");
                            byteData = formattedData.getBytes();
                    }
                    this.stagedInputFile.getOutputStream().write(byteData);
                    loadedRows++;
                    loadedBytes += byteData.length;
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

        if (loadedRows >= maxRowsBeforeFlush || loadedBytes >= maxBytesBeforeFlush) {
            flush();
        }
    }
    
    protected void flush() {
        if (loadedRows > 0) {
                this.stagedInputFile.close();
                statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
	        try {
	            DatabaseInfo dbInfo = platform.getDatabaseInfo();
	            String quote = dbInfo.getDelimiterToken();
	            String catalogSeparator = dbInfo.getCatalogSeparator();
	            String schemaSeparator = dbInfo.getSchemaSeparator();
	            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) transaction;
	            Connection c = jdbcTransaction.getConnection();
	            String sql = String.format("LOAD DATA " + (isLocal ? "LOCAL " : "") + 
	            		"INFILE '" + stagedInputFile.getFile().getAbsolutePath()).replace('\\', '/') + "' " + 
	            		(isReplace ? "REPLACE " : "IGNORE ") + "INTO TABLE " +
	            		this.getTargetTable().getQualifiedTableName(quote, catalogSeparator, schemaSeparator) +
                                " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' STARTING BY '' " +
                                getCommaDeliminatedColumns(table.getColumns());
	            Statement stmt = c.createStatement();
	
	            //TODO:  clean this up, deal with errors, etc.?
	            log.debug(sql);
	            stmt.execute(sql);
	            stmt.close();
	            transaction.commit();
	        } catch (SQLException ex) {
	            throw platform.getSqlTemplate().translate(ex);
	        } finally {
	            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
	        }
	        this.stagedInputFile.delete();
	        createStagingFile();
	        loadedRows = 0;
	        loadedBytes = 0;
        }
    }
    
    protected String getCommaDeliminatedColumns(Column[] cols) {
        DatabaseInfo dbInfo = platform.getDatabaseInfo();
        String quote = dbInfo.getDelimiterToken();
        StringBuilder columns = new StringBuilder();
        columns.append("(");
        
        // The target syntax is (where BLOB is involved):
        // ('column1', 'column2', @hexColumn3) SET 'column3'=UNHEX(@hexColumn3);
        
        Map<String, String> blobColumns = new LinkedHashMap<String, String>();
        
        if (cols != null && cols.length > 0) {
            for (Column column : cols) {
                boolean blob = column.isOfBinaryType(); 
                if (blob) {
                    String hexVariable = String.format("@%s_hex", column.getName());
                    blobColumns.put(column.getName(), hexVariable);
                    columns.append(hexVariable);
                } else {
                    columns.append(quote);
                    columns.append(column.getName());
                    columns.append(quote);                                    
                }
                
                columns.append(",");
            }
            columns.replace(columns.length() - 1, columns.length(), "");
            
            columns.append(")");
            
            // Build this (optional) clause:
            // SET 'column3'=UNHEX(@hexColumn3);
            
            StringBuilder setClause = new StringBuilder();
            
            for (String columnName : blobColumns.keySet()) {
                if (setClause.length() == 0) {
                    setClause.append(" SET ");
                }
                
                setClause.append(quote);
                setClause.append(columnName);
                setClause.append(quote);
                setClause.append("=UNHEX(");
                setClause.append(blobColumns.get(columnName));
                setClause.append("),");
            }
            
            if (setClause.length() > 0) {
                setClause.setLength(setClause.length()-1);
                columns.append(setClause);
            }
            
            return columns.toString();
        } else {
            return " ";
        }
    }

    protected byte[] escape(byte[] byteData) {
        ArrayList<Integer> indexes = new ArrayList<Integer>();
        for (int i = 0; i < byteData.length; i++) {
            if (byteData[i] == '"' || byteData[i] == '\\') {
                        indexes.add(i + indexes.size());
                }
        }
        for (Integer index : indexes) {
                byteData = ArrayUtils.add(byteData, index, (byte) '\\');
        }
        return byteData;
    }

    protected void createStagingFile() {
    	//TODO: We should use constants for dir structure path, 
    	//      but we don't want to depend on symmetric core.
        this.stagedInputFile = stagingManager.create(0, "bulkloaddir",
                table.getName() + this.getBatch().getBatchId() + ".csv");
    }

}
