package org.jumpmind.symmetric.ddlutils.informix;

import java.io.IOException;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.SqlBuilder;

public class InformixBuilder extends SqlBuilder {

    public InformixBuilder(Platform platform) {
	super(platform);
    }

    protected void writeColumn(Table table, Column column) throws IOException {
	if (column.isAutoIncrement()) {
	    printIdentifier(getColumnName(column));
	    print(" SERIAL");
	} else {
	    super.writeColumn(table, column);
	}
    }

    public String getSelectLastIdentityValues(Table table) {
	return "select dbinfo('sqlca.sqlerrd1') from sysmaster:sysdual";
    }

}
