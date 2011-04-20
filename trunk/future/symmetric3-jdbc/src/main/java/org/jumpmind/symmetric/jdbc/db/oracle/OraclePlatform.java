package org.jumpmind.symmetric.jdbc.db.oracle;

import java.sql.Types;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.oracle.OracleSqlBuilder;
import org.jumpmind.symmetric.jdbc.db.AbstractJdbcPlatform;
import org.jumpmind.symmetric.jdbc.sql.Template;

public class OraclePlatform extends AbstractJdbcPlatform {

    protected static final int[] DATA_INTEGRITY_SQL_ERROR_CODES = { 1, 1400, 1722, 2291, 2292,
            12899 };

    public OraclePlatform(DataSource dataSource) {
        super(dataSource);
        
        platformInfo.setMaxIdentifierLength(30);
        platformInfo.setIdentityStatusReadingSupported(false);

        // Note that the back-mappings are partially done by the model reader,
        // not the driver
        platformInfo.addNativeTypeMapping(Types.ARRAY, "BLOB", Types.BLOB);
        platformInfo.addNativeTypeMapping(Types.BIGINT, "NUMBER(38)");
        platformInfo.addNativeTypeMapping(Types.BINARY, "RAW", Types.VARBINARY);
        platformInfo.addNativeTypeMapping(Types.BIT, "NUMBER(1)");
        platformInfo.addNativeTypeMapping(Types.DATE, "DATE", Types.TIMESTAMP);
        platformInfo.addNativeTypeMapping(Types.DECIMAL, "NUMBER");
        platformInfo.addNativeTypeMapping(Types.DISTINCT, "BLOB", Types.BLOB);
        platformInfo.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        platformInfo.addNativeTypeMapping(Types.FLOAT, "FLOAT", Types.DOUBLE);
        platformInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "BLOB", Types.BLOB);
        platformInfo.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB", Types.BLOB);
        platformInfo.addNativeTypeMapping(Types.LONGVARCHAR, "CLOB", Types.CLOB);
        platformInfo.addNativeTypeMapping(Types.NULL, "BLOB", Types.BLOB);
        platformInfo.addNativeTypeMapping(Types.NUMERIC, "NUMBER", Types.DECIMAL);
        platformInfo.addNativeTypeMapping(Types.INTEGER, "NUMBER", Types.DECIMAL);
        platformInfo.addNativeTypeMapping(Types.OTHER, "BLOB", Types.BLOB);
        platformInfo.addNativeTypeMapping(Types.REF, "BLOB", Types.BLOB);
        platformInfo.addNativeTypeMapping(Types.SMALLINT, "NUMBER(5)");
        platformInfo.addNativeTypeMapping(Types.STRUCT, "BLOB", Types.BLOB);
        platformInfo.addNativeTypeMapping(Types.TIME, "DATE", Types.TIMESTAMP);
        platformInfo.addNativeTypeMapping(Types.TIMESTAMP, "DATE");
        platformInfo.addNativeTypeMapping(Types.TINYINT, "NUMBER(3)");
        platformInfo.addNativeTypeMapping(Types.VARBINARY, "RAW");
        platformInfo.addNativeTypeMapping(Types.VARCHAR, "VARCHAR2");
        platformInfo.addNativeTypeMapping(Types.TIMESTAMP, "TIMESTAMP");
        platformInfo.addNativeTypeMapping("BOOLEAN", "NUMBER(1)", "BIT");
        platformInfo.addNativeTypeMapping("DATALINK", "BLOB", "BLOB");

        platformInfo.setDefaultSize(Types.CHAR, 254);
        platformInfo.setDefaultSize(Types.VARCHAR, 254);
        platformInfo.setDefaultSize(Types.BINARY, 254);
        platformInfo.setDefaultSize(Types.VARBINARY, 254);

        platformInfo.setStoresUpperCaseNamesInCatalog(true);
        platformInfo.setDateOverridesToTimestamp(true);
        platformInfo.setEmptyStringNulled(true);
        platformInfo.setBlankCharColumnSpacePadded(true);
        platformInfo.setNonBlankCharColumnSpacePadded(true);

        this.jdbcModelReader = new OracleJdbcModelReader(this, dataSource);
        this.sqlBuilder = new OracleSqlBuilder(this);

    }

    @Override
    public String getDefaultSchema() {
        if (StringUtils.isBlank(this.defaultSchema)) {
            this.defaultSchema = (String) new Template(this, dataSource).queryForObject(
                    "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual", String.class);
        }
        return defaultSchema;
    }

    @Override
    protected int[] getDataIntegritySqlErrorCodes() {
        return DATA_INTEGRITY_SQL_ERROR_CODES;
    }
}
