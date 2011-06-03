package org.jumpmind.symmetric.jdbc.db.h2;

import java.sql.Types;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.h2.H2TableBuilder;
import org.jumpmind.symmetric.core.db.h2.H2DataCaptureBuilder;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.jdbc.db.AbstractJdbcDbDialect;

public class H2DbDialect extends AbstractJdbcDbDialect {

    protected static final int[] DATA_INTEGRITY_SQL_ERROR_CODES = { 22003, 22012, 22025, 23000,
            23001, 23505, 90005 };

    public H2DbDialect(DataSource dataSource, Parameters parameters) {
        super(dataSource, parameters);

        dialectInfo.setNonPKIdentityColumnsSupported(false);
        dialectInfo.setIdentityOverrideAllowed(false);
        dialectInfo.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        dialectInfo.setNullAsDefaultValueRequired(false);
        dialectInfo.addNativeTypeMapping(Types.ARRAY, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.DISTINCT, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.NULL, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.REF, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.STRUCT, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.DATALINK, "BINARY", Types.BINARY);

        dialectInfo.addNativeTypeMapping(Types.BIT, "BOOLEAN", Types.BIT);

        dialectInfo.addNativeTypeMapping(Types.NUMERIC, "DECIMAL", Types.DECIMAL);
        dialectInfo.addNativeTypeMapping(Types.BINARY, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.BLOB, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.CLOB, "CLOB", Types.CLOB);
        dialectInfo.addNativeTypeMapping(Types.LONGVARCHAR, "VARCHAR", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        dialectInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "OTHER");

        dialectInfo.setDefaultSize(Types.CHAR, Integer.MAX_VALUE);
        dialectInfo.setDefaultSize(Types.VARCHAR, Integer.MAX_VALUE);
        dialectInfo.setDefaultSize(Types.BINARY, Integer.MAX_VALUE);
        dialectInfo.setDefaultSize(Types.VARBINARY, Integer.MAX_VALUE);

        dialectInfo.setDateOverridesToTimestamp(false);
        dialectInfo.setEmptyStringNulled(false);
        dialectInfo.setBlankCharColumnSpacePadded(true);
        dialectInfo.setNonBlankCharColumnSpacePadded(false);
        dialectInfo.setRequiresAutoCommitFalseToSetFetchSize(false);

        this.jdbcModelReader = new H2JdbcTableReader(this);
        this.tableBuilder = new H2TableBuilder(this);
        this.dataCaptureBuilder = new H2DataCaptureBuilder(this);

    }

    @Override
    protected int[] getDataIntegritySqlErrorCodes() {
        return DATA_INTEGRITY_SQL_ERROR_CODES;
    }

}
