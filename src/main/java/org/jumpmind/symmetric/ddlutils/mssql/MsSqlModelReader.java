package org.jumpmind.symmetric.ddlutils.mssql;

import java.sql.SQLException;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.mssql.MSSqlModelReader;
import org.jumpmind.symmetric.ddlutils.JdbcModelReaderSupport;

public class MsSqlModelReader extends MSSqlModelReader {

    private JdbcModelReaderSupport support;

    public MsSqlModelReader(Platform platform) {
	super(platform);
	support = new JdbcModelReaderSupport(platform);
    }

    @Override
    protected void determineAutoIncrementFromResultSetMetaData(Table table, Column columnsToCheck[])
	    throws SQLException {
	support.determineAutoIncrementFromResultSetMetaData(getConnection(), table, columnsToCheck);
    }
}
