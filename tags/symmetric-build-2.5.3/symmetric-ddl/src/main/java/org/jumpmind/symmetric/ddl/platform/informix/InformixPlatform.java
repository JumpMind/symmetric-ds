package org.jumpmind.symmetric.ddl.platform.informix;

import java.sql.Types;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

public class InformixPlatform extends PlatformImplBase implements Platform {

    public static final String DATABASENAME = "Informix Dynamic Server11";
    
    public static final String JDBC_DRIVER = "com.informix.jdbc.IfxDriver";
    
    public static final String JDBC_SUBPROTOCOL = "informix-sqli";

    public InformixPlatform() {
        PlatformInfo info = getPlatformInfo();

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
        
        setSqlBuilder(new InformixBuilder(this));
        setModelReader(new InformixModelReader(this));
    }

    public String getName() {
        return DATABASENAME;
    }
}
