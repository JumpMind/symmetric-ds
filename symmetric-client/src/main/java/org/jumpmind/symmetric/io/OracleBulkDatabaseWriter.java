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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Types;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
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

    protected int commitSize;
    
    protected boolean useDirectPath;
    
    protected String sqlLoaderCommand;
    
    protected String dbUser;
    
    protected String dbPassword;
    
    protected String dbUrl;
    
    protected String ezConnectString;
    
    protected int rows = 0;

    public OracleBulkDatabaseWriter(IDatabasePlatform symmetricPlatform,
			IDatabasePlatform targetPlatform, IStagingManager stagingManager, String tablePrefix, 
            int commitSize, boolean useDirectPath, 
            String sqlLoaderCommand, String dbUser, String dbPassword, String dbUrl, String ezConnectString,  
            DatabaseWriterSettings settings) {
        super(symmetricPlatform, targetPlatform, tablePrefix, settings);
        this.stagingManager = stagingManager;
        this.commitSize = commitSize;
        this.useDirectPath = useDirectPath;
        this.sqlLoaderCommand = sqlLoaderCommand;
        this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		this.dbUrl = dbUrl;
		this.ezConnectString = StringUtils.defaultIfBlank(ezConnectString, getEzConnectString(dbUrl));

        if (StringUtils.isBlank(this.sqlLoaderCommand)) {
            String oracleHome = System.getenv("ORACLE_HOME");
            if (StringUtils.isNotBlank(oracleHome)) {
            	this.sqlLoaderCommand = oracleHome + File.separator + "bin" + File.separator + "sqlldr";
            } else {
            	this.sqlLoaderCommand = "sqlldr";
            }
        }
        // TODO: options for readsize and bindsize?
        // TODO: separate control file from data file for higher readsize?
        // TODO: specify type and size for columns if CHAR(255) default is too small
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
                	String scale = column.getScale() > 0 ? "(" + column.getScale() + ")" : "";
                	String local = column.getMappedTypeCode() == ColumnTypes.ORACLE_TIMESTAMPLTZ ? "LOCAL " : "";
                	columns.append(" TIMESTAMP" + scale + " WITH " + local + "TIME ZONE 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'");
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
    }

    protected void flush() {
        if (rows > 0) {
            stagedInputFile.close();
            statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
            try {
            	File absFile = stagedInputFile.getFile().getAbsoluteFile();
            	String path = absFile.getParent();
            	String[] cmd = { sqlLoaderCommand, dbUser + "/" + dbPassword + ezConnectString,
            			"control=" + stagedInputFile.getFile().getName(), "silent=header",
            			"direct=" + (useDirectPath ? "true" : "false") };
            	if (!useDirectPath) {
            		cmd = (String[]) ArrayUtils.add(cmd, "rows=" + commitSize);
            	}
            	if (logger.isDebugEnabled()) {
            		logger.debug("Working dir: {} ", path);
            		logger.debug("Running: {} ", ArrayUtils.toString(cmd));
            	}
            	ProcessBuilder pb = new ProcessBuilder(cmd);
            	pb.directory(new File(path));
            	pb.redirectErrorStream(true);
            	Process process = null;
                try {
                	process = pb.start();
                	
                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                    	if (!line.equals("")) {
                    		logger.info("SQL*Loader: {}", line);
                    	}
                    }
                    reader.close();

                    int rc = process.waitFor();
                    if (rc != 0) {
                    	throw new RuntimeException("Process builder returned " + rc);
                    }
                } catch (IOException e) {
                	throw new RuntimeException(e);
                }

                stagedInputFile.delete();
                new File(absFile.getPath().replace(".create", ".bad")).delete();
                new File(absFile.getPath().replace(".create", ".log")).delete();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
                stagedInputFile = null;
                rows = 0;
            }
        }
    }

    protected String getEzConnectString(String dbUrl) {
    	String ezConnect = null;
		int index = dbUrl.indexOf("@//");
		if (index != -1) {
			ezConnect = dbUrl.substring(index);
		} else {
			index = dbUrl.toUpperCase().indexOf("HOST=");
			if (index != -1) {
				String database = StringUtils.defaultIfBlank(getTnsVariable(dbUrl, "SERVICE_NAME"),
						getTnsVariable(dbUrl, "SID"));
				ezConnect = "@//" + getTnsVariable(dbUrl, "HOST") + ":" + getTnsVariable(dbUrl, "PORT") + "/" + database;
			} else {
				index = dbUrl.indexOf("@");
				if (index != -1) {
					ezConnect = dbUrl.substring(index).replace("@", "@//");
					index = ezConnect.lastIndexOf(":");
					if (index != -1) {
						ezConnect = ezConnect.substring(0, index) + "/" + ezConnect.substring(index + 1);
					}				
				}
			}
		}
		return ezConnect;
    }

    protected String getTnsVariable(String dbUrl, String name) {
    	String value = "";
    	int startIndex = dbUrl.toUpperCase().indexOf(name + "=");
    	if (startIndex != -1) {
    		int endIndex = dbUrl.indexOf(")", startIndex);
    		value = dbUrl.substring(startIndex + name.length() + 1, endIndex);
    	}
    	return value;
    }

}
