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
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Types;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleBulkDatabaseWriter extends AbstractBulkDatabaseWriter {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected IStagingManager stagingManager;

    protected IStagedResource stagedInputFile;

    protected Table table = null;

    protected boolean hasBinaryType;

    protected int maxRowsBeforeFlush;
    
    protected String sqlLoader;
    
    protected String dbUser;
    
    protected String dbPassword;
    
    protected int rows = 0;

    public OracleBulkDatabaseWriter(ISymmetricEngine engine, DatabaseWriterSettings settings) {
        super(engine.getSymmetricDialect().getPlatform(), engine.getSymmetricDialect().getTargetPlatform(), engine.getTablePrefix(), settings);
        stagingManager = engine.getStagingManager();
        maxRowsBeforeFlush = engine.getParameterService().getInt("oracle.bulk.load.max.rows.before.flush", 100000);
        sqlLoader = engine.getParameterService().getString("oracle.bulk.load.oracle.home", System.getenv("ORACLE_HOME"));
        if (sqlLoader == null) {
    		sqlLoader = "";
        }
        sqlLoader += File.separator + "bin" + File.separator + "sqlldr"; 
        dbUser = engine.getParameterService().getString(BasicDataSourcePropertyConstants.DB_POOL_USER);
		if (dbUser != null && dbUser.startsWith(SecurityConstants.PREFIX_ENC)) {
			dbUser = engine.getSecurityService().decrypt(dbUser.substring(SecurityConstants.PREFIX_ENC.length()));
		}
		dbPassword = engine.getParameterService().getString(BasicDataSourcePropertyConstants.DB_POOL_PASSWORD);
		if (dbPassword != null && dbPassword.startsWith(SecurityConstants.PREFIX_ENC)) {
			dbPassword = engine.getSecurityService().decrypt(dbPassword.substring(SecurityConstants.PREFIX_ENC.length()));
		}
    }

    public boolean start(Table table) {
        this.table = table;
        if (super.start(table) && targetTable != null) {
            hasBinaryType = false;
            if (!batch.getBinaryEncoding().equals(BinaryEncoding.NONE)) {
                for (Column column : targetTable.getColumns()) {
                    if (column.isOfBinaryType()) {
                        hasBinaryType = true;
                        break;
                    }
                }
            }
            if (stagedInputFile == null) {
                createStagingFile(table);
            }
            return true;
        } else {
            return false;
        }
    }

    protected void createStagingFile(Table table) {
        stagedInputFile = stagingManager.create("bulkloaddir", getBatch().getBatchId());
        try {
            OutputStream out = stagedInputFile.getOutputStream();
            out.write(("LOAD DATA\nINFILE *\nINSERT INTO TABLE " + table.getName() + "\n").getBytes());
            out.write("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'\nTRAILING NULLCOLS\n".getBytes());
            
            StringBuilder columns = new StringBuilder("(");
            int index = 0;
            for (Column column : table.getColumns()) {
                if (index++ > 0) {
                	columns.append(", ");
                }
                columns.append(column.getName());
                int type = column.getJdbcTypeCode();
                if (type == Types.TIMESTAMP || type == Types.DATE) {
                	columns.append(" TIMESTAMP 'YYYY-MM-DD HH24:MI:SS.FF9'");
                } else if (column.isTimestampWithTimezone()) {
                	columns.append(" TIMESTAMP 'YYYY-MM-DD HH24:MI:SS.FF9' TZH:TZM");
                } else if (column.isOfBinaryType()) {
                	columns.append(" ENCLOSED BY '<sym_blob>' AND '</sym_blob>'");
                }
            }
            columns.append(")\n");

            out.write(columns.toString().getBytes());
            out.write("BEGINDATA\n".getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void end(Table table) {
        try {
            flush();
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
                byte[] byteData = null;
                if (hasBinaryType) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
                    writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
                    writer.setRecordDelimiter('\n');
                    writer.setTextQualifier('"');
                    writer.setUseTextQualifier(true);
                    writer.setForceQualifier(false);
                    Column[] columns = targetTable.getColumns();
                    for (int i = 0; i < columns.length; i++) {
                        if (columns[i].isOfBinaryType() && parsedData[i] != null) {
                            if (i > 0) {
                                out.write(',');
                            }
                            out.write("<sym_blob>".getBytes());
                            if (batch.getBinaryEncoding().equals(BinaryEncoding.HEX)) {
                                out.write(Hex.decodeHex(parsedData[i].toCharArray()));
                            } else if (batch.getBinaryEncoding().equals(BinaryEncoding.BASE64)) {
                                out.write(Base64.decodeBase64(parsedData[i].getBytes()));
                            }
                            out.write("</sym_blob>".getBytes());
                        } else {
                            writer.write(parsedData[i], true);
                            writer.flush();
                        }
                    }
                    writer.endRecord();
                    writer.close();
                    byteData = out.toByteArray();
                } else {
                    String formattedData = CsvUtils.escapeCsvData(parsedData, '\n', '"');
                    byteData = formattedData.getBytes();
                }
                stagedInputFile.getOutputStream().write(byteData);
                rows++;
            } catch (Exception ex) {
                throw getPlatform().getSqlTemplate().translate(ex);
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
                statistics.get(batch).increment(DataWriterStatisticConstants.ROWCOUNT);
                statistics.get(batch).increment(DataWriterStatisticConstants.LINENUMBER);
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
            }
            break;
        case UPDATE:
        case DELETE:
        default:
            flush();
            writeDefault(data);
            break;
        }

        if (rows >= maxRowsBeforeFlush) {
            flush();
        }
    }

    protected void flush() {
        if (rows > 0) {
            stagedInputFile.close();
            statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
            try {
            	// TODO: add options for direct=true rows=10000
            	String path = stagedInputFile.getFile().getParent();
            	String[] cmd = { sqlLoader, dbUser + "/" + dbPassword,
            			"control=" + stagedInputFile.getFile().getPath(), "silent=header" };
            	if (logger.isDebugEnabled()) {
            		logger.debug("Running: {} ", ArrayUtils.toString(cmd));
            	}
            	ProcessBuilder pb = new ProcessBuilder(cmd);
            	pb.directory(new File(path));
            	pb.redirectErrorStream(true);
            	Process process = null;
                try {
                	process = pb.start();
                    int rc = process.waitFor();
                    if (rc != 0) {
                    	throw new RuntimeException("Process builder returned " + rc);
                    }
                } catch (IOException e) {
                	throw new RuntimeException(e);
                }

                stagedInputFile.delete();
                new File(path.replace(".create", ".bad")).delete();
                new File(path.replace(".create", ".log")).delete();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
                stagedInputFile = null;
                rows = 0;
            }
        }
    }

}
