package org.jumpmind.symmetric.load;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterFilterAdapter;

/**
 * An extension that prefixes the table name with a schema name that is equal to
 * the incoming node_id.
 */
public class SchemaPerNodeDataLoaderFilter extends DatabaseWriterFilterAdapter {

    private String tablePrefix;

    private String schemaPrefix;

    @Override
    public boolean beforeWrite(
            DataContext context, Table table, CsvData data) {
        if (!table.getName().startsWith(tablePrefix)) {
            Batch batch = context.getBatch();
            String sourceNodeId = batch.getSourceNodeId();
            table.setSchema(schemaPrefix != null ? schemaPrefix + sourceNodeId : sourceNodeId);
        }
        return true;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setSchemaPrefix(String schemaPrefix) {
        this.schemaPrefix = schemaPrefix;
    }

}