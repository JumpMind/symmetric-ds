package org.jumpmind.symmetric.ddl.platform.greenplum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.postgresql.PostgreSqlModelReader;

public class GreenplumModelReader extends PostgreSqlModelReader {

    public GreenplumModelReader(Platform platform) {
        super(platform);
    }

    @Override
    protected Collection<Table> readTables(Connection connection, String catalog,
            String schemaPattern, String[] tableTypes) throws SQLException {
        Collection<Table> tables = super.readTables(connection, catalog, schemaPattern, tableTypes);
        setDistributionKeys(connection, tables, schemaPattern);
        return tables;
    }

    protected void setDistributionKeys(Connection connection, Collection<Table> tables,
            String schema) throws SQLException {

        // get the distribution keys for segments
        StringBuffer query = new StringBuffer();

        query.append("select ");
        query.append("   t.relname, ");
        query.append("   a.attname ");
        query.append("from ");
        query.append("   pg_class t, ");
        query.append("   pg_namespace n, ");
        query.append("   pg_attribute a, ");
        query.append("   gp_distribution_policy p ");
        query.append("where ");
        query.append("   n.oid = t.relnamespace and ");
        query.append("   p.localoid = t.oid and ");
        query.append("   a.attrelid = t.oid and ");
        query.append("   a.attnum = any(p.attrnums) and ");
        query.append("   n.nspname = ? ");

        PreparedStatement prepStmt = connection.prepareStatement(query.toString());

        try {
            // set the schema parm in the query
            prepStmt.setString(1, schema);
            ResultSet rs = prepStmt.executeQuery();

            // for every row, find the table and set the distributionKey values
            // for the corresponding columns
            while (rs.next()) {
                Table table = findTable(tables, rs.getString(1).trim());
                if (table != null) {
                    Column column = table.findColumn(rs.getString(2).trim(), getPlatform()
                            .isDelimitedIdentifierModeOn());
                    if (column != null) {
                        column.setDistributionKey(true);
                    }
                }
            }
            rs.close();
        } finally {
            if (prepStmt != null) {
                prepStmt.close();
            }
        }
    }

    // TODO: don't like this - talk to eric and chris for alternatives
    private Table findTable(Collection<Table> tables, String tableName) {
        for (Iterator<Table> iter = tables.iterator(); iter.hasNext();) {
            Table table = (Table) iter.next();

            if (table.getName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        return null;
    }
}
