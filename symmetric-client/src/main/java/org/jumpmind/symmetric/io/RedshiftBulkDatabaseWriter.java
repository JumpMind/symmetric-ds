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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.service.IParameterService;

public class RedshiftBulkDatabaseWriter extends CloudBulkDatabaseWriter {

    protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected boolean needsExplicitIds;
    protected Table table = null;

    private String appendToCopyCommand;
    
    public RedshiftBulkDatabaseWriter(IDatabasePlatform symmetricPlatform,
            IDatabasePlatform targetPlatform, String tablePrefix, IStagingManager stagingManager, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers, IParameterService parameterService, DatabaseWriterSettings settings) {
        
        super(symmetricPlatform, targetPlatform, tablePrefix, stagingManager, filters, errorHandlers, parameterService, settings);
        
        this.appendToCopyCommand = parameterService.getString(ParameterConstants.REDSHIFT_APPEND_TO_COPY_COMMAND);
        
        if (parameterService.getInt(ParameterConstants.REDSHIFT_BULK_LOAD_MAX_ROWS_BEFORE_FLUSH) > 0) {
            super.maxRowsBeforeFlush = parameterService.getInt(ParameterConstants.REDSHIFT_BULK_LOAD_MAX_ROWS_BEFORE_FLUSH);
        }
        
        if (parameterService.getLong(ParameterConstants.REDSHIFT_BULK_LOAD_MAX_BYTES_BEFORE_FLUSH) > 0) {
            super.maxBytesBeforeFlush = parameterService.getLong(ParameterConstants.REDSHIFT_BULK_LOAD_MAX_BYTES_BEFORE_FLUSH);
        }
        
        if (StringUtils.isNotBlank(parameterService.getString(ParameterConstants.REDSHIFT_BULK_LOAD_S3_BUCKET))) {
            super.s3Bucket = parameterService.getString(ParameterConstants.REDSHIFT_BULK_LOAD_S3_BUCKET);
        }
        
        if (StringUtils.isNotBlank(parameterService.getString(ParameterConstants.REDSHIFT_BULK_LOAD_S3_ACCESS_KEY))) {
            super.s3AccessKey = parameterService.getString(ParameterConstants.REDSHIFT_BULK_LOAD_S3_ACCESS_KEY);
        }
        
        if (StringUtils.isNotBlank(parameterService.getString(ParameterConstants.REDSHIFT_BULK_LOAD_S3_SECRET_KEY))) {
            super.s3SecretKey = parameterService.getString(ParameterConstants.REDSHIFT_BULK_LOAD_S3_SECRET_KEY);
        }
        
        if (StringUtils.isNotBlank(parameterService.getString(ParameterConstants.REDSHIFT_BULK_LOAD_S3_ENDPOINT))) {
            super.s3Endpoint = parameterService.getString(ParameterConstants.REDSHIFT_BULK_LOAD_S3_ENDPOINT);
        }
    }

    @Override
    public void copyToCloudStorage() {
        copyToS3CloudStorage();
    }

    @Override
    public void loadToCloudDatabase() {
        try {
            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) getTargetTransaction();
            Connection c = jdbcTransaction.getConnection();
            String sql = "COPY " + getTargetTable().getFullyQualifiedTableName() +
                    " (" + Table.getCommaDeliminatedColumns(targetTable.getColumns()) +
                    ") FROM 's3://" + s3Bucket + "/" + super.fileName + 
                    "' REGION '" + s3Region +
                    "' CREDENTIALS 'aws_access_key_id=" + s3AccessKey + ";aws_secret_access_key=" + s3SecretKey + 
                    "' DELIMITER '" + fieldTerminator + 
                    "' CSV DATEFORMAT 'YYYY-MM-DD HH:MI:SS' " + (needsExplicitIds ? "EXPLICIT_IDS" : "") + 
                    (isNotBlank(appendToCopyCommand) ? (" " + appendToCopyCommand) : "");
            Statement stmt = c.createStatement();

            log.debug(sql);
            stmt.execute(sql);
            stmt.close();
            getTargetTransaction().commit();
        } catch (SQLException ex) {
            throw getPlatform().getSqlTemplate().translate(ex);
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
        }
    }

    @Override
    public void cleanUpCloudStorage() {
        cleanUpS3Storage();
    }


}
