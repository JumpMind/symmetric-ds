package org.jumpmind.symmetric.jdbc.tools;

import junit.framework.Assert;

import org.jumpmind.symmetric.core.common.FileUtils;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.jdbc.db.JdbcDbDialectFactory;
import org.jumpmind.symmetric.jdbc.tools.copy.TableCopyProperties;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableCopyTest {

    static TableCopy tableCopy;
    static TableCopyProperties tableCopyProperties;

    @BeforeClass
    public static void setup() {
        FileUtils.deleteDirectory("target/h2");
        tableCopyProperties = new TableCopyProperties(
                "src/test/resources/test-tablecopy.properties");
        IDbDialect sourcePlatform = JdbcDbDialectFactory.createPlatform(
                tableCopyProperties.getSourceDataSource(), new Parameters(tableCopyProperties));
        IDbDialect targetPlatform = JdbcDbDialectFactory.createPlatform(
                tableCopyProperties.getTargetDataSource(), new Parameters(tableCopyProperties));
        Table[] tables = {
                new Table("table1", new Column("table_id", TypeMap.NUMERIC, "10,2", false, true,
                        true)),
                new Table("table2", new Column("table_id", TypeMap.VARCHAR, "10", false, false,
                        true), new Column("text", TypeMap.CLOB, null, false, false, false)) };
        sourcePlatform.alter(true, tables);
        targetPlatform.alter(true, tables);
        tableCopy = new TableCopy(tableCopyProperties);
    }

    @Test
    public void testSimpleTableCopy() throws Exception {
        tableCopy
                .getSourcePlatform()
                .getSqlTemplate()
                .update("insert into table1 values(1)", "insert into table1 values(2)",
                        "insert into table1 values(3)", "insert into table1 values(4)");
        tableCopy.copy();
        Assert.assertEquals(
                4,
                tableCopy.getTargetPlatform().getSqlTemplate()
                        .queryForInt("select count(*) from table1"));
    }
}
