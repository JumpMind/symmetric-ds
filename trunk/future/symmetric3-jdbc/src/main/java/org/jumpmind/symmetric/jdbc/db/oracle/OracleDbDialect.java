package org.jumpmind.symmetric.jdbc.db.oracle;

import java.sql.Types;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.DmlStatement;
import org.jumpmind.symmetric.core.db.DmlStatement.DmlType;
import org.jumpmind.symmetric.core.db.oracle.OracleTableBuilder;
import org.jumpmind.symmetric.core.db.oracle.OracleDataCaptureBuilder;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.jdbc.db.AbstractJdbcDbDialect;

public class OracleDbDialect extends AbstractJdbcDbDialect {

    protected static final int[] DATA_INTEGRITY_SQL_ERROR_CODES = { 1, 1400, 1722, 2291, 2292,
            12899 };

    public OracleDbDialect(DataSource dataSource, Parameters parameters) {
        super(dataSource, parameters);

        dialectInfo.setMaxIdentifierLength(30);
        dialectInfo.setIdentityStatusReadingSupported(false);

        // Note that the back-mappings are partially done by the model reader,
        // not the driver
        dialectInfo.addNativeTypeMapping(Types.ARRAY, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.BIGINT, "NUMBER(38)");
        dialectInfo.addNativeTypeMapping(Types.BINARY, "RAW", Types.VARBINARY);
        dialectInfo.addNativeTypeMapping(Types.BIT, "NUMBER(1)");
        dialectInfo.addNativeTypeMapping(Types.DATE, "DATE", Types.TIMESTAMP);
        dialectInfo.addNativeTypeMapping(Types.DECIMAL, "NUMBER");
        dialectInfo.addNativeTypeMapping(Types.DISTINCT, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        dialectInfo.addNativeTypeMapping(Types.FLOAT, "FLOAT", Types.DOUBLE);
        dialectInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.LONGVARCHAR, "CLOB", Types.CLOB);
        dialectInfo.addNativeTypeMapping(Types.NULL, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.NUMERIC, "NUMBER", Types.DECIMAL);
        dialectInfo.addNativeTypeMapping(Types.INTEGER, "NUMBER", Types.DECIMAL);
        dialectInfo.addNativeTypeMapping(Types.OTHER, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.REF, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.SMALLINT, "NUMBER(5)");
        dialectInfo.addNativeTypeMapping(Types.STRUCT, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.TIME, "DATE", Types.TIMESTAMP);
        dialectInfo.addNativeTypeMapping(Types.TIMESTAMP, "DATE");
        dialectInfo.addNativeTypeMapping(Types.TINYINT, "NUMBER(3)");
        dialectInfo.addNativeTypeMapping(Types.VARBINARY, "RAW");
        dialectInfo.addNativeTypeMapping(Types.VARCHAR, "VARCHAR2");
        dialectInfo.addNativeTypeMapping(Types.TIMESTAMP, "TIMESTAMP");
        dialectInfo.addNativeTypeMapping("BOOLEAN", "NUMBER(1)", "BIT");
        dialectInfo.addNativeTypeMapping("DATALINK", "BLOB", "BLOB");

        dialectInfo.setDefaultSize(Types.CHAR, 254);
        dialectInfo.setDefaultSize(Types.VARCHAR, 254);
        dialectInfo.setDefaultSize(Types.BINARY, 254);
        dialectInfo.setDefaultSize(Types.VARBINARY, 254);

        // These come from the old db dialect class
        dialectInfo.setDateOverridesToTimestamp(true);
        dialectInfo.setEmptyStringNulled(true);
        dialectInfo.setBlankCharColumnSpacePadded(true);
        dialectInfo.setNonBlankCharColumnSpacePadded(true);
        dialectInfo.setRequiresAutoCommitFalseToSetFetchSize(false);

        this.dataCaptureBuilder = new OracleDataCaptureBuilder(this);
        this.jdbcModelReader = new OracleJdbcTableReader(this);
        this.tableBuilder = new OracleTableBuilder(this);

    }

    @Override
    public String getDefaultSchema() {
        if (StringUtils.isBlank(this.defaultSchema)) {
            this.defaultSchema = (String) getJdbcSqlConnection().queryForObject(
                    "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual", String.class);
        }
        return defaultSchema;
    }

    @Override
    protected int[] getDataIntegritySqlErrorCodes() {
        return DATA_INTEGRITY_SQL_ERROR_CODES;
    }
    
    @Override
    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns, Column[] preFilteredColumns) {
        return new OracleDmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                preFilteredColumns, dialectInfo.isDateOverridesToTimestamp(),
                dialectInfo.getIdentifierQuoteString());
    }
}
