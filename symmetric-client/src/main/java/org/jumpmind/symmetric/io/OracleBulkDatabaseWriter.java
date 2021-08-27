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
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
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

public class OracleBulkDatabaseWriter extends AbstractBulkDatabaseWriter {
    
    private IDatabasePlatform targetPlatform;

    protected Logger logger;

    protected IStagingManager stagingManager;

    protected IStagedResource dataResource;

    protected IStagedResource controlResource;

    protected Table table = null;

    protected boolean hasBinaryType;

    protected String sqlLoaderCommand;

    protected ArrayList<String> sqlLoaderOptions;

    protected String dbUser;

    protected String dbPassword;

    protected String dbUrl;

    protected String ezConnectString;
    
    protected String sqlLoaderInfileCharset;
    
    protected String fieldTerminator;
    
    protected String lineTerminator;

    protected int rows = 0;
    
    protected Map<String, Integer> columnLengthsMap = new HashMap<String, Integer>();
    
    protected boolean delimitTokens = true;

    public OracleBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform,
            IStagingManager stagingManager, String tablePrefix, String sqlLoaderCommand, String sqlLoaderOptions,
            String dbUser, String dbPassword, String dbUrl, String ezConnectString, String sqlLoaderInfileCharset,
            String fieldTerminator, String lineTerminator,
            DatabaseWriterSettings settings, boolean delimitTokens) {
        super(symmetricPlatform, targetPlatform, tablePrefix, settings);
        this.targetPlatform = targetPlatform;
        logger = LoggerFactory.getLogger(getClass());
        this.stagingManager = stagingManager;
        this.sqlLoaderCommand = sqlLoaderCommand;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.dbUrl = dbUrl;
        this.ezConnectString = StringUtils.defaultIfBlank(ezConnectString, getConnectString(dbUrl));
        this.sqlLoaderInfileCharset = StringUtils.defaultIfBlank(sqlLoaderInfileCharset, null);
        this.fieldTerminator = fieldTerminator;
        this.lineTerminator = lineTerminator;
        this.delimitTokens = delimitTokens;

        this.sqlLoaderOptions = new ArrayList<String>();
        if (StringUtils.isNotBlank(sqlLoaderOptions)) {
            for (String option : sqlLoaderOptions.split(" ")) {
                this.sqlLoaderOptions.add(option);
            }
        }
        init();
    }
    
    protected void init() {
        if (StringUtils.isBlank(this.sqlLoaderCommand)) {
            String oracleHome = System.getenv("ORACLE_HOME");
            if (StringUtils.isNotBlank(oracleHome)) {
                this.sqlLoaderCommand = oracleHome + File.separator + "bin" + File.separator + "sqlldr";
            } else {
                this.sqlLoaderCommand = "sqlldr";
            }
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
            if (dataResource == null && !isFallBackToDefault()) {
                dataResource = stagingManager.create(Constants.STAGING_CATEGORY_BULK_LOAD, getBatch().getBatchId());
            }
            columnLengthsMap.clear();
            return true;
        } else {
            return false;
        }
    }

    protected void createStagingFile() {
        long batchId = getBatch().getBatchId();
        controlResource = stagingManager.create(Constants.STAGING_CATEGORY_BULK_LOAD, StringUtils.leftPad(batchId + "-ctl", 14, "0"));
        try {
            OutputStream out = controlResource.getOutputStream();
            out.write(("LOAD DATA\n").getBytes());
            if(StringUtils.isNotEmpty(sqlLoaderInfileCharset)) {
                out.write(("CHARACTERSET " + sqlLoaderInfileCharset + "\n").getBytes());
            }
            out.write(getInfileControl().getBytes());
            
            String quote = "";
            if (delimitTokens) {
                quote = targetPlatform.getDdlBuilder().getDatabaseInfo().getDelimiterToken();
            }

            out.write(("APPEND INTO TABLE " + targetTable.getQualifiedTableName(quote, ".", ".") + "\n").getBytes());

            out.write(("FIELDS TERMINATED BY '" + fieldTerminator + "'\n").getBytes());
            out.write(getLineTerminatedByControl().getBytes());
            
            out.write("TRAILING NULLCOLS\n".getBytes());

            StringBuilder columns = new StringBuilder("(");
            int index = 0;
            for (Column column : targetTable.getColumns()) {
                if (index++ > 0) {
                    columns.append(", ");
                }
                columns.append(quote).append(column.getName()).append(quote);
                int type = column.getMappedTypeCode();
                
                if (targetPlatform.isLob(type)) {
                    columns.append(" CHAR(" + columnLengthsMap.get(column.getName()) + ")");
                } else if (column.isOfTextType() && column.getSizeAsInt() > 0) {
                    columns.append(" CHAR(" + column.getSize() + ")");
                } else if (type == Types.TIMESTAMP || type == Types.DATE) {
                    columns.append(" TIMESTAMP 'YYYY-MM-DD HH24:MI:SS.FF9'");
                } else if (column.isTimestampWithTimezone()) {
                    String scale = column.getScale() > 0 ? "(" + column.getScale() + ")" : "";
                    String local = column.getMappedTypeCode() == ColumnTypes.ORACLE_TIMESTAMPLTZ ? "LOCAL " : "";
                    columns.append(
                            " TIMESTAMP" + scale + " WITH " + local + "TIME ZONE 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'");
                }
            }
            columns.append(")\n");
            out.write(columns.toString().getBytes());
            controlResource.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected String getInfileControl() {
        return "INFILE '" + dataResource.getFile().getName() + "' \"str '" + lineTerminator + "'\"\n";
    }

    protected String getLineTerminatedByControl() {
        return "";
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
                OutputStream out = dataResource.getOutputStream();
                String[] parsedData = data.getParsedData(CsvData.ROW_DATA);
                Column[] columns = targetTable.getColumns();

                for (int i = 0; i < parsedData.length; i++) {
                    if (parsedData[i] != null) {
                        byte[] bytesToWrite = null;
                        if (hasBinaryType && columns[i].isOfBinaryType()) {
                            if (batch.getBinaryEncoding().equals(BinaryEncoding.BASE64)) {
                                bytesToWrite = Base64.decodeBase64(parsedData[i].getBytes());
                            } else if (batch.getBinaryEncoding().equals(BinaryEncoding.HEX)) {
                                bytesToWrite = Hex.decodeHex(parsedData[i].toCharArray());
                            }
                        } else {
                            bytesToWrite = parsedData[i].getBytes();
                        }
                        if(bytesToWrite != null) {
                            out.write(bytesToWrite);
                            int newLength = bytesToWrite.length;
                            Integer o = columnLengthsMap.get(columns[i].getName());
                            int oldLength = (o == null ? 0 : o.intValue());
                            if(newLength > oldLength) {
                                this.columnLengthsMap.put(columns[i].getName(), newLength);
                            }
                        }
                    }
                    if (i + 1 < parsedData.length) {
                        out.write(fieldTerminator.getBytes());
                    }
                }

                out.write(lineTerminator.getBytes());
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
        boolean inError = false;
        if (rows > 0) {
            dataResource.close();
            createStagingFile();
            statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
            try {
                File parentDir = controlResource.getFile().getParentFile();
                ArrayList<String> cmd = new ArrayList<String>();
                cmd.add(sqlLoaderCommand);
                cmd.add("userid=" + dbUser + "/" + dbPassword + ezConnectString);
                cmd.add("control=" + controlResource.getFile().getName());
                cmd.addAll(sqlLoaderOptions);
                if (logger.isDebugEnabled()) {
                    logger.debug("Working dir: {} ", parentDir.getAbsolutePath());
                    logger.debug("Running: {} ", cmd.toString());
                }
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.directory(parentDir);
                pb.redirectErrorStream(true);
                Process process = pb.start();
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    if (!line.equals("")) {
                        logger.info("{}: {}", getLoaderName(), line);
                    }
                }

                int rc = process.waitFor();
                if (rc == 2) {
                    throw new RuntimeException("All or some rows were rejected.");
                } else if (rc != 0) {
                    throw new RuntimeException("Process builder returned " + rc);
                }
            } catch (Exception e) {
                inError = true;
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException(e);
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
                cleanup(inError);
                rows = 0;
            }
        } else {
            cleanup(inError);
        }
    }

    protected void cleanup(boolean inError) {
        if (dataResource != null) {
            dataResource.delete();
            dataResource = null;
        }
        if (controlResource != null) {
            if (!inError) {
                File absFile = controlResource.getFile().getAbsoluteFile();
                try {
                    new File(absFile.getPath().replace(".create", ".bad")).delete();
                    new File(absFile.getPath().replace(".create", ".log")).delete();
                } catch (Exception e) {
                }
                controlResource.delete();
            }
            controlResource = null;
        }
    }

    protected String getConnectString(String dbUrl) {
        String ezConnect = null;
        int index = dbUrl.indexOf("@//");
        if (index != -1) {
            ezConnect = dbUrl.substring(index);
        } else {
            index = dbUrl.toUpperCase().indexOf("HOST=");
            if (index != -1) {
                String database = StringUtils.defaultIfBlank(getTnsVariable(dbUrl, "SERVICE_NAME"),
                        getTnsVariable(dbUrl, "SID"));
                ezConnect = "@//" + getTnsVariable(dbUrl, "HOST") + ":" + getTnsVariable(dbUrl, "PORT") + "/"
                        + database;
            } else {
                ezConnect = parseDbUrl(dbUrl);
            }
        }
        return ezConnect;
    }
    
    protected static String parseDbUrl(String dbUrl) {
        // jdbc:oracle:thin:@10.10.10.10:1521/SERVICE_NAME_PROD
        // jdbc:oracle:thin:@10.10.10.10:1521:DBNAME
        String ret = null;
        int index = dbUrl.indexOf("@");
        if (index != -1) {
            ret = dbUrl.substring(index).replace("@", "@//");
            // Split on the first occurrence of :
            String[] array = ret.split(":", 2);
            StringBuilder sb = new StringBuilder(array[0]).append(":");
            int indexOfColon = array[1].indexOf(":");
            int indexOfSlash = array[1].indexOf("/");
            if(indexOfSlash > -1  && indexOfColon > -1 && indexOfSlash < indexOfColon) {
                // Found a / before a colon, use the / and the rest of the string as is
                sb.append(array[1]);
            } else if(indexOfSlash > -1) {
                // Found a / and no colon, use the / and the rest of the string as is
                sb.append(array[1]);
            } else if(indexOfColon > -1) {
                // Found a : and no /, use up top the :, then append a /, then the rest of the string as is
                sb.append(array[1].substring(0, indexOfColon)).append("/").append(array[1].substring(indexOfColon + 1));
            }
            index = ret.lastIndexOf(":");
            ret = sb.toString();
        }
        return ret;
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
    
    protected String getLoaderName() {
        return "SQL*Loader";
    }

}
