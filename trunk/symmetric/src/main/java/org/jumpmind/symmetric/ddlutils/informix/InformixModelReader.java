package org.jumpmind.symmetric.ddlutils.informix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.JdbcModelReader;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ddlutils.JdbcModelReaderSupport;

public class InformixModelReader extends JdbcModelReader {

    final ILog logger = LogFactory.getLog(getClass());

    private JdbcModelReaderSupport support;

    public InformixModelReader(Platform platform) {
	super(platform);
	setDefaultCatalogPattern(null);
	setDefaultSchemaPattern(null);
	support = new JdbcModelReaderSupport(platform);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Table readTable(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
	Table table = super.readTable(metaData, values);
	determineAutoIncrementFromResultSetMetaData(table, table.getColumns());
	return table;
    }

    @Override
    protected void determineAutoIncrementFromResultSetMetaData(Table table, Column columnsToCheck[])
	    throws SQLException {
	support.determineAutoIncrementFromResultSetMetaData(getConnection(), table, columnsToCheck, ":");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection readIndices(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {

	Connection conn = getConnection();
	String sql = "select rtrim(dbinfo('dbname')) as TABLE_CAT, st.owner as TABLE_SCHEM, st.tabname as TABLE_NAME, "
		+ "case when idxtype = 'U' then 0 else 1 end NON_UNIQUE, si.owner as INDEX_QUALIFIER, si.idxname as INDEX_NAME,  "
		+ "3 as TYPE,  "
		+ "case when sc.colno = si.part1 then 1 "
		+ "when sc.colno = si.part1 then 1 "
		+ "when sc.colno = si.part2 then 2 "
		+ "when sc.colno = si.part3 then 3 "
		+ "when sc.colno = si.part4 then 4 "
		+ "when sc.colno = si.part5 then 5 "
		+ "when sc.colno = si.part6 then 6 "
		+ "when sc.colno = si.part7 then 7 "
		+ "when sc.colno = si.part8 then 8 "
		+ "else 0 end as ORDINAL_POSITION,  "
		+ "sc.colname as COLUMN_NAME, "
		+ "null::varchar as ASC_OR_DESC, 0 as CARDINALITY, 0 as PAGES, null::varchar as FILTER_CONDITION "
		+ "from sysindexes si "
		+ "inner join systables st on si.tabid = st.tabid "
		+ "inner join syscolumns sc on si.tabid = sc.tabid "
		+ "where st.tabname like ? "
		+ "and (sc.colno = si.part1 or sc.colno = si.part2 or sc.colno = si.part3 or  "
		+ "sc.colno = si.part4 or sc.colno = si.part5 or sc.colno = si.part6 or  "
		+ "sc.colno = si.part7 or sc.colno = si.part8)";
	PreparedStatement ps = conn.prepareStatement(sql);
	ps.setString(1, tableName);

	ResultSet rs = ps.executeQuery();

	Map indices = new ListOrderedMap();
	while (rs.next()) {
	    Map values = readColumns(rs, getColumnsForIndex());
	    readIndex(metaData, values, indices);
	}

	rs.close();
	ps.close();
	return indices.values();
    }

    public void removeSystemIndices(DatabaseMetaDataWrapper metaData, Table table) throws SQLException {
	super.removeSystemIndices(metaData, table);
    }
    
    @Override
    protected boolean isInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData, Table table, Index index)
	    throws SQLException {
	return index.getName().startsWith(" ");
    }

    @Override
    protected boolean isInternalForeignKeyIndex(DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk,
	    Index index1) throws SQLException {
	return fk.getName().startsWith(" ");
    }
}
