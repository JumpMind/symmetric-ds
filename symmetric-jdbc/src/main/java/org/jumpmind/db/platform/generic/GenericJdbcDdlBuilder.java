package org.jumpmind.db.platform.generic;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.db.alter.ColumnAutoIncrementChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlException;

public class GenericJdbcDdlBuilder extends AbstractDdlBuilder {

    public GenericJdbcDdlBuilder(String databaseName, IDatabasePlatform platform) {
        super(databaseName);

        databaseInfo.setTriggersSupported(false);
        databaseInfo.setForeignKeysSupported(false);
        databaseInfo.setNullAsDefaultValueRequired(true);
        databaseInfo.setHasNullDefault(Types.TIMESTAMP, true);
        databaseInfo.setHasNullDefault(Types.DATE, true);
        databaseInfo.setHasNullDefault(Types.TIME, true);

        DataSource ds = platform.getDataSource();
        ResultSet rs = null;
        Connection c = null;
        try {
            c = ds.getConnection();
            DatabaseMetaData meta = c.getMetaData();
            String quoteString = meta.getIdentifierQuoteString();
            if (isNotBlank(quoteString)) {
                databaseInfo.setDelimiterToken(quoteString);
            } else {
                databaseInfo.setDelimitedIdentifiersSupported(false);
            }
            
            rs = meta.getTypeInfo();
            if (!setNativeMapping(Types.LONGVARCHAR, rs, Types.LONGVARCHAR)) {
                if (!setNativeMapping(Types.LONGVARCHAR, rs, Types.CLOB)) {
                    setNativeMapping(Types.LONGVARCHAR, rs, Types.VARCHAR);
                }
            }
            rs.close();
            
        } catch (SQLException ex) {
            throw new SqlException(ex);
        } finally {
            JdbcSqlTemplate.close(rs);
            JdbcSqlTemplate.close(c);
        }
    }
    
    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        /**
         *  Auto increment isn't supported for generic platforms
         */        
    }
    
    protected boolean setNativeMapping(int targetJdbcType, ResultSet rs, int acceptableType) throws SQLException {        
        rs.beforeFirst();
        while (rs.next()) {
            String name = rs.getString("TYPE_NAME");
            int type = rs.getInt("DATA_TYPE");
            if (type == acceptableType) {
                databaseInfo.addNativeTypeMapping(targetJdbcType, name, acceptableType);
                return true;
            }
        }
        return false;
        
    }
    
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if (change instanceof ColumnAutoIncrementChange) {
                /**
                 *  Auto increment isn't supported for generic platforms
                 */
                 changeIt.remove();
            }
        }
    }

}
