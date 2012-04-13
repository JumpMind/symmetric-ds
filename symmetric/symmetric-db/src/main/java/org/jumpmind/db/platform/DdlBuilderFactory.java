package org.jumpmind.db.platform;

import org.jumpmind.db.platform.db2.Db2DdlBuilder;
import org.jumpmind.db.platform.postgresql.PostgreSqlDdlBuilder;
import org.jumpmind.db.platform.sqlite.SqliteDdlBuilder;
import org.jumpmind.db.platform.sybase.SybaseDdlBuilder;

final public class DdlBuilderFactory {

    private DdlBuilderFactory() {
    }

    public static final IDdlBuilder createDdlBuilder(String databaseName) {
        if (DatabaseNamesConstants.DB2.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.DERBY.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.FIREBIRD.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.GREENPLUM.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.H2.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.HSQLDB.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.INFORMIX.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.INFORMIX.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.INTERBASE.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.MSSQL.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.MYSQL.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.ORACLE.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.POSTGRESQL.equals(databaseName)) {
            return new PostgreSqlDdlBuilder();
        } else if (DatabaseNamesConstants.SQLITE.equals(databaseName)) {
            return new SqliteDdlBuilder();  
        } else if (DatabaseNamesConstants.SYBASE.equals(databaseName)) {
            return new SybaseDdlBuilder();
        } else {
            return null;
        }
    }

}
