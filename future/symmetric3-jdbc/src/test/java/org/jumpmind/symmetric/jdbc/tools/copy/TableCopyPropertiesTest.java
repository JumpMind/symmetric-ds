package org.jumpmind.symmetric.jdbc.tools.copy;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.FileUtils;
import org.jumpmind.symmetric.jdbc.db.JdbcSqlTemplate;
import org.junit.Test;

public class TableCopyPropertiesTest {

    @Test
    public void testTableCopyProperties() throws Exception {
        TableCopyProperties properties = new TableCopyProperties();
        properties.loadExample();
    }

    @Test
    public void testGetTargetDataSource() {
        FileUtils.deleteDirectory("target/h2");
        TableCopyProperties properties = new TableCopyProperties(
                "src/test/resources/test-tablecopy.properties");
        DataSource dataSource = properties.getTargetDataSource();
        JdbcSqlTemplate template = new JdbcSqlTemplate(dataSource);
        template.update("create table test (test varchar(100))");
    }
}
