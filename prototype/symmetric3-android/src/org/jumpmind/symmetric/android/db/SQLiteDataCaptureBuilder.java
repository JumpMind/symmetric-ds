package org.jumpmind.symmetric.android.db;

import java.util.Map;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.db.AbstractDataCaptureBuilder;
import org.jumpmind.symmetric.core.db.IDbDialect;

public class SQLiteDataCaptureBuilder extends AbstractDataCaptureBuilder {

    public SQLiteDataCaptureBuilder(IDbDialect dbDialect) {
        super(dbDialect);
        // TODO Auto-generated constructor stub
    }
    
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    @Override
    protected String getClobColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getNewTriggerValue() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getOldTriggerValue() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getBlobColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getWrappedBlobColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getTableExtractSqlTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Map<String, String> getFunctionTemplatesToInstall() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getFunctionInstalledSqlTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getEmptyColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getStringColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getXmlColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getArrayColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getNumberColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getDateTimeColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getBooleanColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getTimeColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getDateColumnTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getTriggerConcatCharacter() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getOldColumnPrefix() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getNewColumnPrefix() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getInsertTriggerTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getUpdateTriggerTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getDeleteTriggerTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getTransactionTriggerExpression() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected boolean isTransactionIdOverrideSupported() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    protected String preProcessTriggerSqlClause(String sqlClause) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getDataHasChangedCondition() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getSyncTriggersExpression() {
        // TODO Auto-generated method stub
        return null;
    }

}

