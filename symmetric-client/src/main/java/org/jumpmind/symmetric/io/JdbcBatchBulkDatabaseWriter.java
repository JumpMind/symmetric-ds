package org.jumpmind.symmetric.io;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;

public class JdbcBatchBulkDatabaseWriter extends AbstractBulkDatabaseWriter {

    public JdbcBatchBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, String tablePrefix) {
        super(symmetricPlatform, targetPlatform, tablePrefix);
    }

    @Override
    public void start(Batch batch) {
        super.start(batch);
        if (context.get(ContextConstants.CONTEXT_BULK_WRITER_TO_USE) == null || !context.get(ContextConstants.CONTEXT_BULK_WRITER_TO_USE).equals("default")) {
            getTransaction().setInBatchMode(true);
            ((JdbcSqlTransaction) getTransaction()).setBatchSize(((JdbcSqlTemplate) getPlatform()
                    .getSqlTemplate()).getSettings().getBatchBulkLoaderSize());
        }
    }
    
    @Override
    protected LoadStatus insert(CsvData data) {
        LoadStatus loadStatus = super.insert(data);
        if (loadStatus == LoadStatus.CONFLICT) {
            loadStatus = LoadStatus.SUCCESS;
        }
        return loadStatus;
    }
    
    @Override
    protected LoadStatus update(CsvData data, boolean applyChangesOnly, boolean useConflictDetection) {
        LoadStatus loadStatus = super.update(data, applyChangesOnly, useConflictDetection);
        if (loadStatus == LoadStatus.CONFLICT) {
            loadStatus = LoadStatus.SUCCESS;
        }
        return loadStatus;
    }
    
    @Override
    protected void bulkWrite(CsvData data) {
        writeDefault(data);
    }

    @Override
    public void end(Batch batch, boolean inError) {
        super.end(batch, inError);
        getTransaction().flush();
    }
    
}
