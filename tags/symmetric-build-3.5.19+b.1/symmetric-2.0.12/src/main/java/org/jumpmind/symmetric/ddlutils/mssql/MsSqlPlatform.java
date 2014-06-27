package org.jumpmind.symmetric.ddlutils.mssql;

import org.apache.ddlutils.platform.mssql.MSSqlPlatform;

public class MsSqlPlatform extends MSSqlPlatform {

    public MsSqlPlatform() {
	super();
	setDelimitedIdentifierModeOn(true);
	setModelReader(new MsSqlModelReader(this));
    }
}
