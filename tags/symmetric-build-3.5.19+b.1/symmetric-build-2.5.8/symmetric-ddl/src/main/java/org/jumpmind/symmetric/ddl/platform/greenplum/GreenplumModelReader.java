package org.jumpmind.symmetric.ddl.platform.greenplum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.DatabaseMetaDataWrapper;
import org.jumpmind.symmetric.ddl.platform.postgresql.PostgreSqlModelReader;

public class GreenplumModelReader extends PostgreSqlModelReader {

    public GreenplumModelReader(Platform platform) {
        super(platform);
    }

    protected void setDistributionKeys(Connection connection, Table table, String schema)
            throws SQLException {

        // get the distribution keys for segments
        StringBuilder query = new StringBuilder();

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
        query.append("   n.nspname = ? and ");
        query.append("   t.relname = ?");

        PreparedStatement prepStmt = connection.prepareStatement(query.toString());

        try {
            // set the schema parm in the query
            prepStmt.setString(1, schema);
            prepStmt.setString(2, table.getName());
            ResultSet rs = prepStmt.executeQuery();

            // for every row, set the distributionKey for the corresponding
            // columns
            while (rs.next()) {
                Column column = table.findColumn(rs.getString(2).trim(), getPlatform()
                        .isDelimitedIdentifierModeOn());
                if (column != null) {
                    column.setDistributionKey(true);
                }
            }
            rs.close();
        } finally {
            if (prepStmt != null) {
                prepStmt.close();
            }
        }
    }

    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        Table table = super.readTable(connection, metaData, values);
        setDistributionKeys(connection, table, metaData.getSchemaPattern());
        return table;
    }
}
