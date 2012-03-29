package org.jumpmind.symmetric.jdbc.db;

import junit.framework.Assert;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.SqlScript;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.junit.Test;

public class JdbcDbDialectTest extends AbstractDatabaseTest {

    @Test
    public void testJdbcPlatformFactory() {
        IJdbcDbDialect platform = getDbDialect(true);
        Assert.assertNotNull(platform);
    }

    @Test
    public void testCreateTable() {
        IJdbcDbDialect platform = getDbDialect(true);
        Table table = new Table("test", new Column("test_id", TypeMap.NUMERIC, "10,2", false, true,
                true));
        String alterSql = platform.getAlterScriptFor(table);
        Assert.assertFalse(StringUtils.isBlank(alterSql));
        SqlScript script = new SqlScript(alterSql, platform);
        script.execute();
        alterSql = platform.getAlterScriptFor(table);
        Assert.assertTrue(
                "There should have been no changes to the table.  Instead, we received the alter script: "
                        + alterSql, StringUtils.isBlank(alterSql));
    }
}
