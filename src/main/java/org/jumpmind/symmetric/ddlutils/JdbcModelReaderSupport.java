package org.jumpmind.symmetric.ddlutils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.JdbcModelReader;

/**
 * Common methods needed to fix bugs and enhance the DdlUtils JdbcModelReader
 * class.
 * 
 * @author elong
 */
public class JdbcModelReaderSupport extends JdbcModelReader {

    public JdbcModelReaderSupport(Platform platform) {
	super(platform);
    }

    public void determineAutoIncrementFromResultSetMetaData(Connection conn,
	    Table table, final Column columnsToCheck[]) throws SQLException {
	determineAutoIncrementFromResultSetMetaData(conn, table,
		columnsToCheck, ".");
    }

    /**
     * Fix problems following problems: 1) identifiers that use keywords 2)
     * different catalog and schema 3) different catalog separator character
     */
    public void determineAutoIncrementFromResultSetMetaData(Connection conn,
	    Table table, final Column columnsToCheck[], String catalogSeparator)
	    throws SQLException {
	if (columnsToCheck == null || columnsToCheck.length == 0) {
	    return;
	}
	StringBuilder query = new StringBuilder();
	query.append("SELECT ");
	for (int idx = 0; idx < columnsToCheck.length; idx++) {
	    if (idx > 0) {
		query.append(",");
	    }
	    query.append("t.");
	    appendIdentifier(query, columnsToCheck[idx].getName());
	}
	query.append(" FROM ");

	if (table.getCatalog() != null && !table.getCatalog().trim().equals("")) {
	    appendIdentifier(query, table.getCatalog());
	    query.append(catalogSeparator);
	}
	if (table.getSchema() != null && !table.getSchema().trim().equals("")) {
	    appendIdentifier(query, table.getSchema()).append(".");
	}
	appendIdentifier(query, table.getName()).append(" t WHERE 1 = 0");

	Statement stmt = null;
	try {
	    stmt = conn.createStatement();
	    ResultSet rs = stmt.executeQuery(query.toString());
	    ResultSetMetaData rsMetaData = rs.getMetaData();

	    for (int idx = 0; idx < columnsToCheck.length; idx++) {
		if (rsMetaData.isAutoIncrement(idx + 1)) {
		    columnsToCheck[idx].setAutoIncrement(true);
		}
	    }
	} finally {
	    if (stmt != null) {
		stmt.close();
	    }
	}
    }

    public StringBuilder appendIdentifier(StringBuilder query, String identifier) {
	if (getPlatform().isDelimitedIdentifierModeOn()) {
	    query.append(getPlatformInfo().getDelimiterToken());
	}
	query.append(identifier);
	if (getPlatform().isDelimitedIdentifierModeOn()) {
	    query.append(getPlatformInfo().getDelimiterToken());
	}
	return query;
    }
}
