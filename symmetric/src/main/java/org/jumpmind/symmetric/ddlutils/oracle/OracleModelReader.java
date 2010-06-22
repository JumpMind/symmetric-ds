package org.jumpmind.symmetric.ddlutils.oracle;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.platform.DatabaseMetaDataWrapper;
import org.jumpmind.symmetric.ddl.platform.oracle.Oracle10ModelReader;

public class OracleModelReader extends Oracle10ModelReader {

    static final ILog log = LogFactory.getLog(OracleModelReader.class);
    
    public OracleModelReader(Platform platform) {
        super(platform);
    }
    
    @SuppressWarnings("unchecked")
    protected Collection readIndices(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException
    {
        // Oracle bug 4999817 causes a table analyze to execute in response to a call to 
	// DatabaseMetaData#getIndexInfo.
	// The bug is fixed in driver version 10.2.0.4.  The bug is present in at least
	// driver versions 10.2.0.1.0, 10.1.0.2.0, and 9.2.0.5.
	// To avoid this bug, we will access user_indexes view.
        // This also allows us to filter system-generated indices which are identified by either
        // having GENERATED='Y' in the query result, or by their index names being equal to the
        // name of the primary key of the table

        StringBuffer query = new StringBuffer();

        query.append("SELECT a.INDEX_NAME, a.INDEX_TYPE, a.UNIQUENESS, b.COLUMN_NAME, b.COLUMN_POSITION FROM USER_INDEXES a, USER_IND_COLUMNS b WHERE ");
        query.append("a.TABLE_NAME=? AND a.GENERATED=? AND a.TABLE_TYPE=? AND a.TABLE_NAME=b.TABLE_NAME AND a.INDEX_NAME=b.INDEX_NAME AND ");
        query.append("a.INDEX_NAME NOT IN (SELECT DISTINCT c.CONSTRAINT_NAME FROM USER_CONSTRAINTS c WHERE c.CONSTRAINT_TYPE=? AND c.TABLE_NAME=a.TABLE_NAME");
        if (metaData.getSchemaPattern() != null)
        {
            query.append(" AND c.OWNER LIKE ?) AND a.TABLE_OWNER LIKE ?");
        }
        else
        {
            query.append(")");
        }

        Map               indices = new ListOrderedMap();
        PreparedStatement stmt    = null;

        try
        {
            stmt = getConnection().prepareStatement(query.toString());
            stmt.setString(1, getPlatform().isDelimitedIdentifierModeOn() ? tableName : tableName.toUpperCase());
            stmt.setString(2, "N");
            stmt.setString(3, "TABLE");
            stmt.setString(4, "P");
            if (metaData.getSchemaPattern() != null)
            {
                stmt.setString(5, metaData.getSchemaPattern().toUpperCase());
                stmt.setString(6, metaData.getSchemaPattern().toUpperCase());
            }

            ResultSet rs     = stmt.executeQuery();
            Map       values = new HashMap();

            while (rs.next())
            {        
                String name =rs.getString(1);                
                String type = rs.getString(2);
                // Only read in normal oracle indexes
                if (type.startsWith("NORMAL"))  
                {
                    values.put("INDEX_TYPE",       new Short(DatabaseMetaData.tableIndexOther));
                    values.put("INDEX_NAME",       name);    
                    values.put("NON_UNIQUE",       "UNIQUE".equalsIgnoreCase(rs.getString(3)) ? Boolean.FALSE : Boolean.TRUE);
                    values.put("COLUMN_NAME",      rs.getString(4));
                    values.put("ORDINAL_POSITION", new Short(rs.getShort(5)));

                    readIndex(metaData, values, indices);
                } else {
                    log.warn("DDLUtilsSkipOracleIndex", name);
                }
            }
        }
        finally
        {
            if (stmt != null)
            {
                stmt.close();
            }
        }
        return indices.values();
    }

}
