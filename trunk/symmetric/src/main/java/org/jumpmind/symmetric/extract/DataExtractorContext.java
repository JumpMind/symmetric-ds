package org.jumpmind.symmetric.extract;

import java.util.ArrayList;
import java.util.List;

public class DataExtractorContext implements Cloneable {
    
    private List<String> auditRecordsWritten = new ArrayList<String>();
    private String lastTableName;

    public DataExtractorContext copy() {
        DataExtractorContext newVersion;
        try {
            newVersion = (DataExtractorContext)super.clone();
            newVersion.auditRecordsWritten = new ArrayList<String>();
            return newVersion;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getAuditRecordsWritten() {
        return auditRecordsWritten;
    }

    public void setLastTableName(String tableName) {
        lastTableName = tableName;
    }
    
    public boolean isLastTable(String tableName) {
        return lastTableName.equals(tableName);
    }

}
