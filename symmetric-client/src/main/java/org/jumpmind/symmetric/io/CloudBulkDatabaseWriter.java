package org.jumpmind.symmetric.io;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.OutputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.service.IParameterService;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;


public abstract class CloudBulkDatabaseWriter extends AbstractBulkDatabaseWriter {
    protected IStagingManager stagingManager;
    protected IStagedResource stagedInputFile;
    protected boolean needsBinaryConversion;
    protected boolean needsColumnsReordered;
    protected int loadedRows = 0;
    protected long loadedBytes = 0;
    protected boolean needsExplicitIds;
    protected Table table = null;
    protected Table databaseTable = null;
    protected String fileName = null;
    protected String rowTerminator = "\r\n";
    protected String fieldTerminator = "|";
    
    protected int maxRowsBeforeFlush;
    protected long maxBytesBeforeFlush;
    protected String s3Bucket;
    protected String s3AccessKey;
    protected String s3SecretKey;
    protected String s3Endpoint;
    protected String s3ObjectKey;
    protected String s3Region;
    
    protected String azureAccountName;
    protected String azureAccountKey;
    protected String azureBlobContainer;
    protected String azureSasToken;
    
    protected AmazonS3 s3client;
    protected CloudBlockBlob azureBlobReference;
    
    public abstract void copyToCloudStorage() throws SQLException;
    public abstract void loadToCloudDatabase() throws SQLException;
    public abstract void cleanUpCloudStorage() throws SQLException;
    
    public CloudBulkDatabaseWriter(IDatabasePlatform symmetricPlatform,
            IDatabasePlatform targetPlatform, String tablePrefix, IStagingManager stagingManager, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers, IParameterService parameterService, DatabaseWriterSettings settings) {
        super(symmetricPlatform, targetPlatform, tablePrefix);
        this.stagingManager = stagingManager;
        this.writerSettings = settings;
        this.writerSettings.setDatabaseWriterFilters(filters);
        this.writerSettings.setDatabaseWriterErrorHandlers(errorHandlers);
        this.writerSettings.setCreateTableFailOnError(false);
        
        this.maxRowsBeforeFlush = parameterService.getInt(ParameterConstants.CLOUD_BULK_LOAD_MAX_ROWS_BEFORE_FLUSH, -1);
        this.maxBytesBeforeFlush = parameterService.getLong(ParameterConstants.CLOUD_BULK_LOAD_MAX_ROWS_BEFORE_FLUSH, -1);
        
        this.s3Bucket = parameterService.getString(ParameterConstants.CLOUD_BULK_LOAD_S3_BUCKET);
        this.s3AccessKey = parameterService.getString(ParameterConstants.CLOUD_BULK_LOAD_S3_ACCESS_KEY);
        this.s3SecretKey = parameterService.getString(ParameterConstants.CLOUD_BULK_LOAD_S3_SECRET_KEY);
        this.s3Endpoint = parameterService.getString(ParameterConstants.CLOUD_BULK_LOAD_S3_ENDPOINT);
        this.s3Region = parameterService.getString(ParameterConstants.CLOUD_BULK_LOAD_S3_REGION, Regions.US_EAST_1.getName());
        
        this.azureAccountName = parameterService.getString(ParameterConstants.CLOUD_BULK_LOAD_AZURE_ACCOUNT_NAME);
        this.azureAccountKey = parameterService.getString(ParameterConstants.CLOUD_BULK_LOAD_AZURE_ACCOUNT_KEY);
        this.azureBlobContainer = parameterService.getString(ParameterConstants.CLOUD_BULK_LOAD_AZURE_BLOB_CONTAINER, "symmetricds");
        this.azureSasToken = parameterService.getString(ParameterConstants.CLOUD_BULK_LOAD_AZURE_SAS_TOKEN);
    }
    
    public void copyToS3CloudStorage() {
        stagedInputFile.close();
        statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS); 
        
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(s3AccessKey, s3SecretKey);
 
        s3client = AmazonS3ClientBuilder.standard()
                                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                                .withRegion(s3Region)
                                .build();
        
        TransferManager tm = TransferManagerBuilder.standard()
                .withS3Client(s3client)
                .build();
        
        
        if (isNotBlank(s3Endpoint)) {
            s3client.setEndpoint(s3Endpoint);
        }
        s3ObjectKey = stagedInputFile.getFile().getName();
        try {
            Upload upload = tm.upload(s3Bucket, s3ObjectKey, stagedInputFile.getFile());
            upload.waitForCompletion();
        } catch (AmazonServiceException ase) {
            log.error("Exception from AWS service: " + ase.getMessage());
        } catch (AmazonClientException ace) {
            log.error("Exception from AWS client: " + ace.getMessage());
        } catch (InterruptedException e) {
            log.info("Upload to AWS interrupted", e);
        }
    }
    
    public void copyToAzureCloudStorage()  {
       try {
            String connectionString = String.format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net", 
                    this.azureAccountName, this.azureAccountKey);
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);

            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference(azureBlobContainer);

            azureBlobReference = container.getBlockBlobReference(this.fileName);
            container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());
            azureBlobReference.uploadFromFile(this.stagedInputFile.getFile().getPath());
        } catch (Exception e) {
            log.warn("Unable to copy staged file to Azure ", this.fileName);
        }
    }
    
    public void cleanUpS3Storage() {
        try {
            if (s3ObjectKey != null) {
                s3client.deleteObject(s3Bucket, s3ObjectKey);
            }
        } catch (AmazonServiceException ase) {
            log.info("Exception from AWS service: " + ase.getMessage());
        } catch (AmazonClientException ace) {
            log.info("Exception from AWS client: " + ace.getMessage());
        }
    }
    
    public void cleanUpAzureStorage() {
        try {
            if (azureBlobReference != null) {
                azureBlobReference.deleteIfExists();
            }
        } catch (StorageException se) {
            log.warn("Exception from Azure service while cleaning up blob reference: " + se.getMessage());
        }
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

            if (sourceTable != null && targetTable == null) {
                String qualifiedName = sourceTable.getFullyQualifiedTableName();
                if (writerSettings.isIgnoreMissingTables()) {                    
                    if (!missingTables.contains(qualifiedName)) {
                        log.info("Did not find the {} table in the target database. This might have been part of a sql "
                                + "command (truncate) but will work if the fully qualified name was in the sql provided", qualifiedName);
                        missingTables.add(qualifiedName);
                    }
                } else {
                    throw new SymmetricException("Could not load the %s table.  It is not in the target database", qualifiedName);
                }
            }
            
            needsBinaryConversion = false;
            if (! batch.getBinaryEncoding().equals(BinaryEncoding.HEX) && targetTable != null) {
                for (Column column : targetTable.getColumns()) {
                    if (column.isOfBinaryType()) {
                        needsBinaryConversion = true;
                        break;
                    }
                }
            }
            databaseTable = getPlatform(table).getTableFromCache(sourceTable.getCatalog(), sourceTable.getSchema(),
                    sourceTable.getName(), false);
            if (targetTable != null && databaseTable != null) {
                String[] csvNames = targetTable.getColumnNames();
                String[] columnNames = databaseTable.getColumnNames();
                needsColumnsReordered = false;
                for (int i = 0; i < csvNames.length; i++) {
                    if (! csvNames[i].equals(columnNames[i])) {
                        needsColumnsReordered = true;
                        break;
                    }
                }
            }
            if (this.stagedInputFile == null) {
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
    
    public void bulkWrite(CsvData data) {
        
        if (filterBefore(data)) {
            try {
                DataEventType dataEventType = data.getDataEventType();
        
                switch (dataEventType) {
                    case INSERT:
                        statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
                        try {
                            String[] parsedData = data.getParsedData(CsvData.ROW_DATA);
                            
                            if (needsBinaryConversion) {
                                Column[] columns = targetTable.getColumns();
                                for (int i = 0; i < columns.length; i++) {
                                    if (columns[i].isOfBinaryType()) {
                                        if (batch.getBinaryEncoding().equals(BinaryEncoding.BASE64) && parsedData[i] != null) {
                                            parsedData[i] = new String(Hex.encodeHex(Base64.decodeBase64(parsedData[i].getBytes())));
                                        }
                                    }
                                }
                            }
                            OutputStream out =  this.stagedInputFile.getOutputStream();
                            if (needsColumnsReordered) {
                                Map<String, String> mapData = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
                                String[] columnNames = databaseTable.getColumnNames();
                                for (int i = 0; i < columnNames.length; i++) {
                                    String columnData = mapData.get(columnNames[i]);
                                    if (columnData != null) {
                                        out.write(columnData.getBytes());
                                        loadedBytes += columnData.getBytes().length;
                                    }
                                    if (i + 1 < columnNames.length) {
                                        out.write(fieldTerminator.getBytes());
                                        loadedBytes += fieldTerminator.getBytes().length;
                                    }
                                    
                                }
                            } else {
                                for (int i = 0; i < parsedData.length; i++) {
                                    if (parsedData[i] != null) {
                                        out.write(parsedData[i].getBytes());
                                        loadedBytes += parsedData[i].getBytes().length;
                                    }
                                    if (i + 1 < parsedData.length) {
                                        out.write(fieldTerminator.getBytes());
                                        loadedBytes += fieldTerminator.getBytes().length;
                                    }
                                }
                            }
                            
                            
                            out.write(rowTerminator.getBytes());
                            loadedBytes += rowTerminator.getBytes().length;
                            loadedRows++;
                        } catch (Exception ex) {
                            throw getPlatform().getSqlTemplate().translate(ex);
                        } finally {
                            statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
                            statistics.get(batch).increment(DataWriterStatisticConstants.ROWCOUNT);
                            statistics.get(batch).increment(DataWriterStatisticConstants.LINENUMBER);
                            statistics.get(batch).increment(DataWriterStatisticConstants.BYTECOUNT);
                        }
                        break;
                    case UPDATE:
                    case DELETE:
                    default:
                        flush();
                        context.put(ContextConstants.CONTEXT_BULK_WRITER_TO_USE, "default");
                        super.write(data);
                        break;
                }
        
                if ((loadedRows >= maxRowsBeforeFlush && maxRowsBeforeFlush > 0) 
                        || (loadedBytes >= maxBytesBeforeFlush && maxBytesBeforeFlush > 0)) {
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
            this.stagedInputFile.close();
            statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
            
            try {
                copyToCloudStorage();
                loadToCloudDatabase();
                stagedInputFile.delete();
                cleanUpCloudStorage();
                
                createStagingFile();

            } catch (Throwable ex) {
                throw getPlatform().getSqlTemplate().translate(ex);
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
                this.stagedInputFile.delete();
                loadedRows = 0;
                loadedBytes = 0;
            }
        }
    }

    protected void createStagingFile() {
        this.fileName = table.getName() + this.getBatch().getBatchId() + ".csv";
        stagedInputFile = stagingManager.create("bulkloaddir", this.fileName);
    }

}
