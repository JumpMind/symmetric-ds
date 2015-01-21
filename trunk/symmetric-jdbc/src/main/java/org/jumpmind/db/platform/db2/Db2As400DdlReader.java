package org.jumpmind.db.platform.db2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;

public class Db2As400DdlReader extends Db2DdlReader {

	public Db2As400DdlReader(IDatabasePlatform platform) {
        super(platform);
        setSystemSchemaName("QSYS2");
    }

	@Override
	protected String getTrueValue() {
    	return "YES";
    }
	
	@Override
	protected String getSysColumnsSchemaColumn() {
	    return "DBNAME";
	}

    protected boolean isInternalPrimaryKeyIndex(Connection connection,
    		DatabaseMetaDataWrapper metaData, Table table, IIndex index) throws SQLException {
        ResultSet pkData = null;
        HashSet<String> pkColNames = new HashSet<String>();

        try {
            pkData = metaData.getPrimaryKeys(table.getName());
            while (pkData.next()) {
                Map<String, Object> values = readMetaData(pkData, getColumnsForPK());
                pkColNames.add((String) values.get("COLUMN_NAME"));
            }
        } finally {
            if (pkData != null) {
                pkData.close();
            }
        }

        boolean indexMatchesPk = true;
        for (int i = 0; i < index.getColumnCount(); i++) {
        	if (!pkColNames.contains(index.getColumn(i).getName())) {
        		indexMatchesPk = false;
        		break;
        	}
        }

        return indexMatchesPk;
    }

    protected Table postprocessTableFromDatabase(Table table) {
    	super.postprocessTableFromDatabase(table);
        if (table != null) {
            for (int columnIdx = 0; columnIdx < table.getColumnCount(); columnIdx++) {
                Column column = table.getColumn(columnIdx);
                if (StringUtils.isBlank(column.getDefaultValue())) {
                	column.setDefaultValue(null);
                }
            }
        }
        return table;
    }

}
