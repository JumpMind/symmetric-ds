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

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DefaultDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class RedshiftBulkDatabaseWriter extends DefaultDatabaseWriter {

    protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected int loadedRows = 0;
    protected long loadedBytes = 0;
    protected boolean needsExplicitIds;
    protected Table table = null;

    protected int maxRowsBeforeFlush;
    protected long maxBytesBeforeFlush;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private String appendToCopyCommand;
    private String s3Endpoint;

    public RedshiftBulkDatabaseWriter(IDatabasePlatform platform, IStagingManager stagingManager, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers, int maxRowsBeforeFlush, long maxBytesBeforeFlush, String bucket,
            String accessKey, String secretKey, String appendToCopyCommand, String s3Endpoint) {
        super(platform);
        this.stagingManager = stagingManager;
        this.writerSettings.setDatabaseWriterFilters(filters);
        this.writerSettings.setDatabaseWriterErrorHandlers(errorHandlers);
        this.writerSettings.setCreateTableFailOnError(false);
        this.maxRowsBeforeFlush = maxRowsBeforeFlush;
        this.maxBytesBeforeFlush = maxBytesBeforeFlush;
        this.bucket = bucket;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.appendToCopyCommand = appendToCopyCommand;
        this.s3Endpoint = s3Endpoint;
    }

    public boolean start(Table table) {
        this.table = table;
        if (super.start(table)) {
            needsExplicitIds = false;
            if (targetTable != null) {
	            for (Column column : targetTable.getColumns()) {
	                if (column.isAutoIncrement()) {
	                    needsExplicitIds = true;
	                    break;
	                }
	            }
            }

            if (stagedInputFile == null) {
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
            stagedInputFile.close();
            stagedInputFile.delete();
        } finally {
            super.end(table);
        }
    }

    public void write(CsvData data) {
        if (filterBefore(data)) {
            try {
                DataEventType dataEventType = data.getDataEventType();
        
                switch (dataEventType) {
                    case INSERT:
                        statistics.get(batch).increment(DataWriterStatisticConstants.STATEMENTCOUNT);
                        statistics.get(batch).increment(DataWriterStatisticConstants.LINENUMBER);
                        statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                        try {
                            String[] parsedData = data.getParsedData(CsvData.ROW_DATA);
                            String formattedData = CsvUtils.escapeCsvData(parsedData, '\n', '"', CsvWriter.ESCAPE_MODE_DOUBLED, "\\N");
                            stagedInputFile.getWriter().write(formattedData);
                            loadedRows++;
                            loadedBytes += formattedData.getBytes().length;
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
                filterAfter(data);
            } catch (RuntimeException e) {
                if (filterError(data, e)) {
                    throw e;
                }
            }
        }
    }

    protected void flush() {
        if (loadedRows > 0) {
            stagedInputFile.close();
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);  
            AmazonS3 s3client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
            if (isNotBlank(s3Endpoint)) {
                s3client.setEndpoint(s3Endpoint);
            }
            String objectKey = stagedInputFile.getFile().getName();
            try {
                s3client.putObject(bucket, objectKey, stagedInputFile.getFile());
            } catch (AmazonServiceException ase) {
                log.error("Exception from AWS service: " + ase.getMessage());
            } catch (AmazonClientException ace) {
                log.error("Exception from AWS client: " + ace.getMessage());
            }

            try {
                JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) transaction;
                Connection c = jdbcTransaction.getConnection();
                String sql = "COPY " + getTargetTable().getFullyQualifiedTableName() +
                        " (" + Table.getCommaDeliminatedColumns(table.getColumns()) +
                        ") FROM 's3://" + bucket + "/" + objectKey + 
                        "' CREDENTIALS 'aws_access_key_id=" + accessKey + ";aws_secret_access_key=" + secretKey + 
                        "' CSV DATEFORMAT 'YYYY-MM-DD HH:MI:SS' " + (needsExplicitIds ? "EXPLICIT_IDS" : "") + 
                        (isNotBlank(appendToCopyCommand) ? (" " + appendToCopyCommand) : "");
                Statement stmt = c.createStatement();

                log.debug(sql);
                stmt.execute(sql);
                stmt.close();
                transaction.commit();
            } catch (SQLException ex) {
                throw platform.getSqlTemplate().translate(ex);
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            }

            stagedInputFile.delete();
            try {
                s3client.deleteObject(bucket, objectKey);
            } catch (AmazonServiceException ase) {
                log.error("Exception from AWS service: " + ase.getMessage());
            } catch (AmazonClientException ace) {
                log.error("Exception from AWS client: " + ace.getMessage());
            }

            createStagingFile();
            loadedRows = 0;
            loadedBytes = 0;
        }
    }

    protected void createStagingFile() {
        stagedInputFile = stagingManager.create(0, "bulkloaddir", table.getName() + getBatch().getBatchId() + ".csv");
    }

}
