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


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.service.IParameterService;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;


public class BigQueryBulkDatabaseWriter extends CloudBulkDatabaseWriter {

    BigQuery bigquery;
    
    public BigQueryBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform,
            String tablePrefix, IStagingManager stagingManager, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers, IParameterService parameterService,
            ISecurityService securityService, DatabaseWriterSettings writerSettings, BigQuery bq)
            throws FileNotFoundException, IOException {
        super(symmetricPlatform, targetPlatform, tablePrefix, stagingManager, filters, errorHandlers, parameterService,
                securityService, writerSettings);
        fieldTerminator = ",";
        bigquery = bq;
    }

    @Override
    public void loadToCloudDatabase() throws SQLException {
        try {
            File csvPath = this.stagedInputFile.getFile();
            
            TableId tableId = TableId.of(this.targetTable.getSchema(), this.targetTable.getName());
            WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).setAutodetect(false).setDestinationTable(tableId).build();
            // The location must be specified; other fields can be auto-detected.
            JobId jobId = JobId.newBuilder()
                    .setLocation(bigquery.getOptions().getLocation())
                    .setProject(bigquery.getOptions().getProjectId()).build();
            
            TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
            // Write data to writer
            OutputStream stream = Channels.newOutputStream(writer);
            FileUtils.copyFile(csvPath, stream);
            stream.close();
            
            // Get load job
            Job job = writer.getJob();
            job = job.waitFor();
        } catch (Exception ex) {
            throw getPlatform().getSqlTemplate().translate(ex);
        } 
    }
    
    @Override
    protected Table lookupTableAtTarget(Table sourceTable) {
        return sourceTable;
    }

    @Override
    public void copyToCloudStorage() throws SQLException {
    }

    @Override
    public void cleanUpCloudStorage() throws SQLException {
    }
}
