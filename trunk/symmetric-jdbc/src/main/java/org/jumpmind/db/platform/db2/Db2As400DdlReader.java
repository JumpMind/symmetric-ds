package org.jumpmind.db.platform.db2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
	protected String getSysColumnsDefaultValueColumn() {
	    return "DFTVALUE";
	}
	
	@Override
	protected String getSysColumnsSchemaColumn() {
	    return "DBNAME";
	}
	
	@Override
    public List<String> getSchemaNames(String catalog) {
        try {
            return super.getSchemaNames(catalog);
        } catch (Exception ex) {
            ArrayList<String> list = new ArrayList<String>();
            list.add(platform.getSqlTemplate().queryForObject(
                    "select CURRENT SCHEMA from sysibm.sysdummy1", String.class));
            return list;
        }
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

}
