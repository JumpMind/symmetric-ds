package org.jumpmind.symmetric.core.db;

import java.util.Map;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.process.sql.TableToExtract;

public interface IDataCaptureBuilder {
    
    public BinaryEncoding getBinaryEncoding();

    public String createTableExtractSql(TableToExtract tableToExtract, boolean supportsBigLobs);

    public String createTableExtractSql(TableToExtract tableToExtract,
            Map<String, String> replacementTokens, boolean supportsBigLobs);
    
    public String createTableExtractCountSql(TableToExtract tableToExtract,
            Map<String, String> replacementTokens);
    
    public void configureRequiredFunctions();
    
}
