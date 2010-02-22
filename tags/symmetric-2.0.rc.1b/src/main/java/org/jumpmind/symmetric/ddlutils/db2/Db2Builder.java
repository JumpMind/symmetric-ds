package org.jumpmind.symmetric.ddlutils.db2;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.CreationParameters;
import org.apache.ddlutils.platform.db2.Db2v8Builder;

@SuppressWarnings("unchecked")
public class Db2Builder extends Db2v8Builder {

    protected final Log logger = LogFactory.getLog(getClass());
    
    public Db2Builder(Platform platform) {
        super(platform);
    }

    @Override
    protected void writeCopyDataStatement(Table sourceTable, Table targetTable) throws IOException {
        super.writeCopyDataStatement(sourceTable, targetTable);
    }
    
    @Override
    protected void processChanges(Database currentModel, Database desiredModel, List changes,
            CreationParameters params) throws IOException {
        for (Object object : changes) {
            logger.info("Change detected: " + object.getClass().getSimpleName());
        }
        super.processChanges(currentModel, desiredModel, changes, params);
    }

}
