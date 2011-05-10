package org.jumpmind.symmetric.jdbc.db;

import junit.framework.Assert;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.core.sql.ISqlConnection;
import org.junit.Test;

public class JdbcPlatformTest extends AbstractDatabaseTest {

    @Test
    public void testJdbcPlatformFactory() {
        IJdbcDbPlatform platform = getPlatform();
        Assert.assertNotNull(platform);
    }    
    
    @Test
    public void testCreateTable() {
        IJdbcDbPlatform platform = getPlatform();
        Table table = new Table("test",
                new Column("test_id", TypeMap.NUMERIC, "10,2", false, true, true));
        ISqlConnection sqlConnection = platform.getSqlConnection();
        String alterSql = platform.getAlterScriptFor(table);
        Assert.assertFalse(StringUtils.isBlank(alterSql));
        sqlConnection.update(alterSql);
        alterSql = platform.getAlterScriptFor(table);
        Assert.assertTrue("There should have been no changes to the table.  Instead, we received the alter script: " + alterSql,StringUtils.isBlank(alterSql));
    }
}
