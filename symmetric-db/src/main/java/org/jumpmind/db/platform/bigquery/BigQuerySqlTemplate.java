package org.jumpmind.db.platform.bigquery;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.FileInputStream;
import java.util.UUID;

import org.jumpmind.db.sql.AbstractJavaDriverSqlTemplate;
import org.jumpmind.db.sql.ISqlResultsListener;
import org.jumpmind.db.sql.ISqlStatementSource;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
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
