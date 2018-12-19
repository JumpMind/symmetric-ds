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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleBulkDatabaseWriter extends AbstractBulkDatabaseWriter {

    protected final static String FIELD_TERMINATOR = "|}";

    protected final static String LINE_TERMINATOR = "|>";

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

    protected int rows = 0;

    public OracleBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform,
            IStagingManager stagingManager, String tablePrefix, String sqlLoaderCommand, String sqlLoaderOptions,
            String dbUser, String dbPassword, String dbUrl, String ezConnectString, DatabaseWriterSettings settings) {
        super(symmetricPlatform, targetPlatform, tablePrefix, settings);
        logger = LoggerFactory.getLogger(getClass());
        this.stagingManager = stagingManager;
        this.sqlLoaderCommand = sqlLoaderCommand;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.dbUrl = dbUrl;
        this.ezConnectString = StringUtils.defaultIfBlank(ezConnectString, getConnectString(dbUrl));

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
                createStagingFile();
            }
            return true;
        } else {
            return false;
        }
    }

    protected void createStagingFile() {
        long batchId = getBatch().getBatchId();
        controlResource = stagingManager.create("bulkloaddir", StringUtils.leftPad(batchId + "-ctl", 14, "0"));
        dataResource = stagingManager.create("bulkloaddir", batchId);

        try {
            OutputStream out = controlResource.getOutputStream();
            out.write(("LOAD DATA\n").getBytes());
            out.write(getInfileControl().getBytes());
            out.write(("APPEND INTO TABLE " + targetTable.getQualifiedTableName("\"", ".", ".") + "\n").getBytes());

            out.write(("FIELDS TERMINATED BY '" + FIELD_TERMINATOR + "'\n").getBytes());
            out.write(getLineTerminatedByControl().getBytes());
            
            out.write("TRAILING NULLCOLS\n".getBytes());

            StringBuilder columns = new StringBuilder("(");
            int index = 0;
            for (Column column : targetTable.getColumns()) {
                if (index++ > 0) {
                    columns.append(", ");
                }
                columns.append(column.getName());
                int type = column.getMappedTypeCode();
                if (type == Types.CLOB || type == Types.NCLOB) {
                    columns.append(" CLOB");
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
        return "INFILE '" + dataResource.getFile().getName() + "' \"str '" + LINE_TERMINATOR + "'\"\n";
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
                        if (hasBinaryType && columns[i].isOfBinaryType()) {
                            if (batch.getBinaryEncoding().equals(BinaryEncoding.BASE64)) {
                                out.write(Base64.decodeBase64(parsedData[i].getBytes()));
                            } else if (batch.getBinaryEncoding().equals(BinaryEncoding.HEX)) {
                                out.write(Hex.decodeHex(parsedData[i].toCharArray()));
                            }
                        } else {
                            out.write(parsedData[i].getBytes());
                        }
                    }
                    if (i + 1 < parsedData.length) {
                        out.write(FIELD_TERMINATOR.getBytes());
                    }
                }

                out.write(LINE_TERMINATOR.getBytes());
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
    
    protected String getLoaderName() {
        return "SQL*Loader";
    }

}
