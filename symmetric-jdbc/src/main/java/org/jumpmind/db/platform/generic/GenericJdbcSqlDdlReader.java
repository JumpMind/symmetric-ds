package org.jumpmind.db.platform.generic;

import java.sql.Connection;

import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;

public class GenericJdbcSqlDdlReader extends AbstractJdbcDdlReader {

    public GenericJdbcSqlDdlReader(IDatabasePlatform platform) {
        super(platform);
    }
    
    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
        return "PRIMARY".equals(index.getName());
    }

}
