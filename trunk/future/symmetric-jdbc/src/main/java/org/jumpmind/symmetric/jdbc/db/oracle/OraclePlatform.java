package org.jumpmind.symmetric.jdbc.db.oracle;

import java.sql.Types;

import org.jumpmind.symmetric.data.common.StringUtils;
import org.jumpmind.symmetric.data.db.PlatformInfo;
import org.jumpmind.symmetric.jdbc.db.AbstractJdbcPlatform;
import org.jumpmind.symmetric.jdbc.sql.Template;

public class OraclePlatform extends AbstractJdbcPlatform {

    public static String PLATFORMID = "Oracle";

    public OraclePlatform() {
        PlatformInfo info = getPlatformInfo();

        info.setMaxIdentifierLength(30);
        info.setIdentityStatusReadingSupported(false);

        // Note that the back-mappings are partially done by the model reader,
        // not the driver
        info.addNativeTypeMapping(Types.ARRAY, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.BIGINT, "NUMBER(38)");
        info.addNativeTypeMapping(Types.BINARY, "RAW", Types.VARBINARY);
        info.addNativeTypeMapping(Types.BIT, "NUMBER(1)");
        info.addNativeTypeMapping(Types.DATE, "DATE", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.DECIMAL, "NUMBER");
        info.addNativeTypeMapping(Types.DISTINCT, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT, "FLOAT", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.LONGVARCHAR, "CLOB", Types.CLOB);
        info.addNativeTypeMapping(Types.NULL, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.NUMERIC, "NUMBER", Types.DECIMAL);
        info.addNativeTypeMapping(Types.INTEGER, "NUMBER", Types.DECIMAL);
        info.addNativeTypeMapping(Types.OTHER, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.REF, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.SMALLINT, "NUMBER(5)");
        info.addNativeTypeMapping(Types.STRUCT, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.TIME, "DATE", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TIMESTAMP, "DATE");
        info.addNativeTypeMapping(Types.TINYINT, "NUMBER(3)");
        info.addNativeTypeMapping(Types.VARBINARY, "RAW");
        info.addNativeTypeMapping(Types.VARCHAR, "VARCHAR2");
        info.addNativeTypeMapping(Types.TIMESTAMP, "TIMESTAMP");
        info.addNativeTypeMapping("BOOLEAN", "NUMBER(1)", "BIT");
        info.addNativeTypeMapping("DATALINK", "BLOB", "BLOB");

        info.setDefaultSize(Types.CHAR, 254);
        info.setDefaultSize(Types.VARCHAR, 254);
        info.setDefaultSize(Types.BINARY, 254);
        info.setDefaultSize(Types.VARBINARY, 254);
        
        info.setStoresUpperCaseNamesInCatalog(true);
        
        this.jdbcModelReader = new OracleJdbcModelReader(this, dataSource);

    }
    
    @Override
    public String getDefaultSchema() {
        if (StringUtils.isBlank(this.defaultSchema)) {
            this.defaultSchema = (String) new Template(dataSource).queryForObject(
                    "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual", String.class);
        }
        return defaultSchema;
    }
}
