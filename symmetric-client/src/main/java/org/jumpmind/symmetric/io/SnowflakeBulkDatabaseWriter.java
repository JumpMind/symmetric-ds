package org.jumpmind.symmetric.io;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.service.IParameterService;

public class SnowflakeBulkDatabaseWriter extends CloudBulkDatabaseWriter {

    protected String internalStage;
    
    protected String stagingType;
    
    public final static String STAGING_TYPE_SNOWFLAKE_INTERNAL = "SNOWFLAKE_INTERNAL";
    public final static String STAGING_TYPE_AWS_S3 = "AWS_S3";
    public final static String STAGING_TYPE_AZURE = "AZURE";
    
    public final static String FILE_FORMAT_CSV = "symmetricds_csv_format";
    
    public SnowflakeBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, 
            String tablePrefix, IStagingManager stagingManager, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers, IParameterService parameterService, DatabaseWriterSettings writerSettings) {
        super(symmetricPlatform, targetPlatform, tablePrefix, stagingManager, filters, errorHandlers, parameterService, writerSettings);
        
        this.internalStage = parameterService.getString("snowflake.internal.stage.name", "symmetricds_stage");
        this.stagingType = parameterService.getString(ParameterConstants.SNOWFLAKE_STAGING_TYPE, STAGING_TYPE_SNOWFLAKE_INTERNAL);
    }

    @Override
    public void copyToCloudStorage() throws SQLException {
        if (stagingType != null && stagingType.equals(STAGING_TYPE_AWS_S3)) {
            copyToS3CloudStorage();
        } else if (stagingType != null && stagingType.equals(STAGING_TYPE_AZURE)) {
            copyToAzureCloudStorage();
        } else {
            String fileFormat = String.format("create or replace file format %s.%s type = csv "
                    + "field_delimiter = '%s' ", targetTable.getSchema(), FILE_FORMAT_CSV, fieldTerminator);
            
            String createStage = String.format("create or replace stage %s.%s", targetTable.getSchema(), internalStage);
                    
            String putCommand = String.format("put file://%s @%s.%s;", stagedInputFile.getFile(), targetTable.getSchema(), internalStage);
              
            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) getTargetTransaction();
            Connection c = jdbcTransaction.getConnection();
            
            Statement stmt = c.createStatement();
            
            stmt.execute(createStage);
            stmt.execute(fileFormat);
            stmt.execute(putCommand);
            stmt.close();
            
        }
    }

    @Override
    public void loadToCloudDatabase() throws SQLException {
        String copyCommand = null;
        if (stagingType != null && stagingType.equals(STAGING_TYPE_AWS_S3)) {
            copyCommand = String.format("copy into %s.%s from 's3://%s/%s' credentials=(aws_key_id='%s' aws_secret_key='%s')"
                    + " file_format = (format_name='%s.%s')", 
                    targetTable.getSchema(), targetTable.getName(), s3Bucket, fileName, s3AccessKey, 
                    s3SecretKey, targetTable.getSchema(), FILE_FORMAT_CSV);
        } else if (stagingType != null && stagingType.equals(STAGING_TYPE_AZURE)) {
            copyCommand = String.format("copy into %s.%s from 'azure://%s.blob.core.windows.net/%s/%s' credentials=(azure_sas_token = '%s')"
                    + " file_format=(format_name='%s.%s')", 
                    targetTable.getSchema(), targetTable.getName(), this.azureAccountName, this.azureBlobContainer, this.fileName, 
                    this.azureSasToken, 
                    targetTable.getSchema(), FILE_FORMAT_CSV);
        } else {
            copyCommand = String.format("copy into %s.%s from '@%s.%s/%s' file_format = (format_name='%s.%s')", 
                    targetTable.getSchema(), targetTable.getName(), targetTable.getSchema(), this.internalStage, 
                    fileName, targetTable.getSchema(), FILE_FORMAT_CSV);
        }
        
        if (copyCommand != null) {
            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) getTargetTransaction();
            Connection c = jdbcTransaction.getConnection();
            
            Statement stmt = c.createStatement();
            
            stmt.execute(copyCommand);
            stmt.close();
        } 
    }

    @Override
    public void cleanUpCloudStorage() throws SQLException {
        if (stagingType != null && stagingType.equals(STAGING_TYPE_AWS_S3)) {
            cleanUpS3Storage();
        } else if (stagingType != null && stagingType.equals(STAGING_TYPE_AZURE)) {
            cleanUpAzureStorage();
        } else {
            String removeCommand = String.format("remove @%s.%s/%s", targetTable.getSchema(), internalStage, fileName);
            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) getTargetTransaction();
            Connection c = jdbcTransaction.getConnection();
            Statement stmt = c.createStatement();
            
            stmt.execute(removeCommand);
            stmt.close();
        }
    }

}
