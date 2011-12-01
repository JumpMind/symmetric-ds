package org.jumpmind.db.platform.informix;

import java.sql.Types;

import javax.sql.DataSource;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.util.Log;

public class InformixPlatform extends AbstractJdbcDatabasePlatform implements IDatabasePlatform {

    public static final String DATABASENAME = "Informix Dynamic Server11";

    public static final String JDBC_DRIVER = "com.informix.jdbc.IfxDriver";

    public static final String JDBC_SUBPROTOCOL = "informix-sqli";

    public InformixPlatform(DataSource dataSource, Log log) {
        super(dataSource, log);

        info.addNativeTypeMapping(Types.VARCHAR, "VARCHAR", Types.VARCHAR);
        info.addNativeTypeMapping(Types.LONGVARCHAR, "LVARCHAR", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME YEAR TO FRACTION", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TIME, "DATETIME YEAR TO FRACTION", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.BINARY, "BYTE", Types.BINARY);
        info.addNativeTypeMapping(Types.VARBINARY, "BYTE", Types.BINARY);

        info.addNativeTypeMapping(Types.BIT, "BOOLEAN", Types.BOOLEAN);
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.DOUBLE, "FLOAT", Types.DOUBLE);

        info.setDefaultSize(Types.VARCHAR, 255);
        info.setDefaultSize(Types.CHAR, 255);

        info.setAlterTableForDropUsed(true);
        info.setSystemIndicesReturned(true);

        ddlReader = new InformixDdlReader(log, this);
        ddlBuilder = new InformixBuilder(log, this);
    }

    public String getName() {
        return DATABASENAME;
    }
}
