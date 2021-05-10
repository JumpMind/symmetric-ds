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

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsSqlBulkDatabaseWriter extends AbstractBulkDatabaseWriter {

	private static final Logger log = LoggerFactory.getLogger(MsSqlBulkDatabaseWriter.class);
    protected int maxRowsBeforeFlush;
    protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected String rowTerminator = "\r\n";
    protected String fieldTerminator = "||";
    protected int loadedRows = 0;
    protected boolean fireTriggers;
    protected String uncPath;
    protected boolean needsBinaryConversion;
    protected boolean needsColumnsReordered;
    protected Table table = null;
    protected Table databaseTable = null;

    public MsSqlBulkDatabaseWriter(IDatabasePlatform symmetricPlatform,
            IDatabasePlatform tar, String tablePrefix,
            IStagingManager stagingManager,
            int maxRowsBeforeFlush, boolean fireTriggers, String uncPath, String fieldTerminator, String rowTerminator, 
            DatabaseWriterSettings writerSettings) {
        super(symmetricPlatform, tar, tablePrefix, writerSettings);
        this.maxRowsBeforeFlush = maxRowsBeforeFlush;
        this.stagingManager = stagingManager;
        this.fireTriggers = fireTriggers;
        if (fieldTerminator != null && fieldTerminator.length() > 0) {
           this.fieldTerminator = fieldTerminator;
        }
        if (rowTerminator != null && rowTerminator.length() > 0) {
           this.rowTerminator = rowTerminator;
        }
        this.uncPath = uncPath;
        log.debug("Initialized with maxRowsBeforeFlush={}, fireTriggers={}, fieldTerminator={}, rowTerminator={}, uncPath={}",
                maxRowsBeforeFlush, fireTriggers, fieldTerminator, rowTerminator, uncPath);
    }

    public boolean start(Table table) {
        this.table = table;
        if (super.start(table)) {
            if (isFallBackToDefault()) { return true; }
            needsBinaryConversion = false;
            if (batch.getBinaryEncoding() != BinaryEncoding.HEX && targetTable != null) {
                for (Column column : targetTable.getColumns()) {
                    if (column.isOfBinaryType()) {
                        needsBinaryConversion = true;
                        break;
                    }
                }
            }
            if (sourceTable != null && targetTable != null) {
                databaseTable = getPlatform(table).getTableFromCache(sourceTable.getCatalog(), sourceTable.getSchema(),
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
            }
            // TODO: Did this because start is getting called multiple times
            // for the same table in a single batch before end is being called
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
            if (isFallBackToDefault()) { return; }
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
                                if (batch.getBinaryEncoding() == BinaryEncoding.BASE64 && parsedData[i] != null) {
                                    parsedData[i] = new String(Hex.encodeHex(Base64.decodeBase64(parsedData[i].getBytes(Charset.defaultCharset()))));
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
                                out.write(columnData.getBytes(Charset.defaultCharset()));
                            }
                            if (i + 1 < columnNames.length) {
                                out.write(fieldTerminator.getBytes(Charset.defaultCharset()));
                            }
                        }
                    } else {
                        for (int i = 0; i < parsedData.length; i++) {
                            if (parsedData[i] != null) {
                                out.write(parsedData[i].getBytes(Charset.defaultCharset()));
                            }
                            if (i + 1 < parsedData.length) {
                                out.write(fieldTerminator.getBytes(Charset.defaultCharset()));
                            }
                        }
                    }
                    out.write(rowTerminator.getBytes(Charset.defaultCharset()));
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
            String filename;
            if (StringUtils.isEmpty(uncPath)) {
                filename = stagedInputFile.getFile().getAbsolutePath();
            } else {
                filename = uncPath + "\\" + stagedInputFile.getFile().getName();
            }
            try {
                DatabaseInfo dbInfo = getPlatform().getDatabaseInfo();
                String quote = dbInfo.getDelimiterToken();
                String catalogSeparator = dbInfo.getCatalogSeparator();
                String schemaSeparator = dbInfo.getSchemaSeparator();
                JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) getTargetTransaction();
                Connection c = jdbcTransaction.getConnection();
                String rowTerminatorString = "";
                /*
                 * There seems to be a bug with the SQL server bulk insert when
                 * you have one row with binary data at the end using \n as the
                 * row terminator. It works when you leave the row terminator
                 * out of the bulk insert statement.
                 */
                if (!(rowTerminator.equals("\n") || rowTerminator.equals("\r\n"))) {
                    rowTerminatorString = ", ROWTERMINATOR='" + StringEscapeUtils.escapeJava(rowTerminator) + "'";
                }
                final String sql = String.format("BULK INSERT " + 
                        this.getTargetTable().getQualifiedTableName(quote, catalogSeparator, schemaSeparator) + 
                        " FROM '" + filename) + "'" +
                        " WITH (DATAFILETYPE='widechar', FIELDTERMINATOR='"+StringEscapeUtils.escapeJava(fieldTerminator)+"', KEEPIDENTITY" + 
                        (fireTriggers ? ", FIRE_TRIGGERS" : "") + rowTerminatorString +");";
                Statement stmt = c.createStatement();
                log.debug("Running {}", sql);

                //TODO:  clean this up, deal with errors, etc.?
                stmt.execute(sql);
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
    
    protected void createStagingFile() {
        this.stagedInputFile = stagingManager.create(Constants.STAGING_CATEGORY_BULK_LOAD,
                table.getName() + this.getBatch().getBatchId() + ".csv");
        log.debug("Using staging file {}", stagedInputFile.getFile().getPath());
    }
}