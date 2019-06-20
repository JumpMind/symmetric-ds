package org.jumpmind.db.platform.hana;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlReader;

public class HanaDdlReader extends AbstractJdbcDdlReader implements IDdlReader {

    public HanaDdlReader(IDatabasePlatform platform) {
        super(platform);
    }
    
    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData, Map<String, Object> values) throws SQLException {
        Table table = super.readTable(connection, metaData, values);
        if (table != null) {
            // Sql Server does not return the auto-increment status via the
            // database metadata
            determineAutoIncrementFromResultSetMetaData(connection, table, table.getColumns());
        }
        return table;
    }


    
}
