package org.jumpmind.db.platform;

import static org.junit.Assert.assertTrue;

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.h2.H2DdlBuilder;
import org.jumpmind.db.platform.oracle.OracleDdlBuilder;
import org.jumpmind.db.platform.postgresql.PostgreSqlDdlBuilder;
import org.junit.Before;
import org.junit.Test;

public class AbstractDdlBuilderTest {

    AbstractDdlBuilder[] ddlBuilders;

    @Before
    public void setup() {
        ddlBuilders = new AbstractDdlBuilder[] { new H2DdlBuilder(), new OracleDdlBuilder(), new PostgreSqlDdlBuilder() };
    }

    @Test
    public void testAlterTableWithNewVarcharSizeForPlatformWithPlatformColumn() throws Exception {
        for (AbstractDdlBuilder ddlBuilder : ddlBuilders) {

            Table currentTable = new Table("Test", new Column("ID", true, Types.INTEGER, 50, 0),
                    new Column("TXT", false, Types.VARCHAR, 50, 0));
            currentTable.getColumnWithName("TXT").addPlatformColumn(
                    new PlatformColumn(DatabaseNamesConstants.H2, "VARCHAR2", 50, 0));

            Table desiredTable = new Table("Test", new Column("ID", true, Types.INTEGER, 50, 0),
                    new Column("TXT", false, Types.VARCHAR, 255, 0));

            String alterSql = ddlBuilder.alterTable(currentTable, desiredTable);
            assertTrue("Failed to generate an appropriate alter for the following platform: "
                    + ddlBuilder.databaseName, alterSql.contains("255"));

        }
    }
    
    @Test
    public void testAlterTableWithNewVarcharSizeForPlatformWithoutPlatformColumn() throws Exception {
        for (AbstractDdlBuilder ddlBuilder : ddlBuilders) {

            Table currentTable = new Table("Test", new Column("ID", true, Types.INTEGER, 50, 0),
                    new Column("TXT", false, Types.VARCHAR, 50, 0));

            Table desiredTable = new Table("Test", new Column("ID", true, Types.INTEGER, 50, 0),
                    new Column("TXT", false, Types.VARCHAR, 255, 0));

            String alterSql = ddlBuilder.alterTable(currentTable, desiredTable);
            assertTrue("Failed to generate an appropriate alter for the following platform: "
                    + ddlBuilder.databaseName, alterSql.contains("255"));

        }
    }
    

}
