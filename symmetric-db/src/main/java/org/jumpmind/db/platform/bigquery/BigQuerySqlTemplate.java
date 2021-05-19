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
package org.jumpmind.db.platform.bigquery;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.UUID;

import org.jumpmind.db.sql.AbstractJavaDriverSqlTemplate;
import org.jumpmind.db.sql.ISqlResultsListener;
import org.jumpmind.db.sql.ISqlStatementSource;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;

public class BigQuerySqlTemplate extends AbstractJavaDriverSqlTemplate {

    BigQuery bigquery;
    
    public BigQuerySqlTemplate(BigQuery bq) {
        bigquery = bq;
    }
    @Override
    public String getDatabaseProductName() {
        return "bigquery";
    }
    
    @Override
    public int update(boolean autoCommit, boolean failOnError, boolean failOnDrops, boolean failOnSequenceCreate, int commitRate,
            ISqlResultsListener listener, ISqlStatementSource source) {
        
        for (String statement = source.readSqlStatement(); statement != null; statement = source
                .readSqlStatement()) {
            if (isNotBlank(statement)) {
                try {
                    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(statement).build();

                JobId jobId = JobId.of(UUID.randomUUID().toString());
                Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

                // Wait for the query to complete.
                queryJob = queryJob.waitFor();
                
                // Check for errors
                if (queryJob == null) {
                  throw new RuntimeException("Job no longer exists");
                } else if (queryJob.getStatus().getError() != null) {
                  // You can also look at queryJob.getStatus().getExecutionErrors() for all
                  // errors, not just the latest one.
                  throw new RuntimeException(queryJob.getStatus().getError().toString());
                }
                
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
         
        }
        return 1;
    }
}
