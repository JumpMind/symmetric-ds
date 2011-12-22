package org.jumpmind.db.platform.informix;

import java.sql.Types;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.log.Log;

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
        
        info.setNonBlankCharColumnSpacePadded(true);
        info.setBlankCharColumnSpacePadded(true);
        info.setCharColumnSpaceTrimmed(false);
        info.setEmptyStringNulled(false);
        info.setAutoIncrementUpdateAllowed(false);
        
        Map<String, String> env = System.getenv();
        String clientIdentifierMode = env.get("DELIMIDENT");
        if (clientIdentifierMode != null && clientIdentifierMode.equalsIgnoreCase("y")) {
            info.setIdentifierQuoteString("\"");
        }
        
        primaryKeyViolationCodes = new int[] {-268};

        ddlReader = new InformixDdlReader(log, this);
        ddlBuilder = new InformixBuilder(log, this);
    }

    public String getName() {
        return DATABASENAME;
    }
    

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = getSqlTemplate().queryForObject("select trim(user) from sysmaster:sysdual",
                    String.class);
        }
        return defaultSchema;
    }
    

}
