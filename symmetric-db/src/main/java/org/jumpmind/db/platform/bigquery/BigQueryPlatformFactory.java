package org.jumpmind.db.platform.bigquery;

import java.io.FileInputStream;

import org.jumpmind.db.sql.SqlTemplateSettings;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;

public class BigQueryPlatformFactory {

    public BigQueryPlatformFactory() {
    }

    public BigQueryPlatform createDatabasePlatform(SqlTemplateSettings settings, String projectId, String location, String filePath) {
        try {
            HttpTransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions();
            transportOptions = transportOptions.toBuilder().setConnectTimeout(60000).setReadTimeout(60000).build();

            BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).setLocation(location)
                    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(filePath)))
                    .setTransportOptions(transportOptions).build().getService();
            
            return new BigQueryPlatform(settings, bigquery);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
