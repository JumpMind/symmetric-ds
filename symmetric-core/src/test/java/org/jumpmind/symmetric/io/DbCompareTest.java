package org.jumpmind.symmetric.io;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.DbCompareReport.TableReport;
import org.jumpmind.symmetric.service.IParameterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.jumpmind.db.sql.SqlScript;

public class DbCompareTest {
    protected DbCompare test;
    protected DbCompareConfig config;
    protected ISymmetricDialect sourceDialect;
    protected ISymmetricDialect targetDialect;
    protected final String nonEmptyTableWithPKNoDifferences = "NONEMPTYTABLEWITHPKNODIFFERENCES";
    protected final String emptyTableWithPK = "EMPTYTABLEWITHPK";
    protected final String emptyTableWithNoPK = "EMPTYTABLEWITHNOPK";
    protected final String nonEmptyTableWithPKWithDifferences = "NONEMPTYTABLEWITHPKWITHDIFFERENCES";
    protected final String nonEmptyTableWithNoPKNoDifferences = "NONEMPTYTABLEWITHNOPKNODIFFERENCES";
    public List<String> sourceTableNames;
    public List<String> targetTableNames;
    private static boolean firstTimeSetup = true;
    protected ISymmetricEngine sourceEngine;
    protected ISymmetricEngine targetEngine;
    protected SingleConnectionDataSource sourceds;
    protected IDatabasePlatform sourcePlatform;
    protected SingleConnectionDataSource targetds;
    protected IDatabasePlatform targetPlatform;

    @BeforeEach
    public void setUp() throws Exception {
        sourceds = getDataSource("jdbc:h2:mem:sourceDatabase");
        sourcePlatform = JdbcDatabasePlatformFactory.getInstance().create(sourceds, new SqlTemplateSettings(), true, false);
        targetds = getDataSource("jdbc:h2:mem:targetDatabase");
        targetPlatform = JdbcDatabasePlatformFactory.getInstance().create(targetds, new SqlTemplateSettings(), true, false);
        if (firstTimeSetup) {
            createTables();
        }
        this.sourceEngine = mock(ISymmetricEngine.class);
        this.targetEngine = mock(ISymmetricEngine.class);
        when(sourceEngine.getDatabasePlatform()).thenReturn(sourcePlatform);
        when(targetEngine.getDatabasePlatform()).thenReturn(targetPlatform);
        this.sourceDialect = mock(ISymmetricDialect.class);
        this.targetDialect = mock(ISymmetricDialect.class);
        when(sourceEngine.getTargetDialect()).thenReturn(sourceDialect);
        when(targetEngine.getTargetDialect()).thenReturn(targetDialect);
        when(sourceEngine.getSymmetricDialect()).thenReturn(sourceDialect);
        when(targetEngine.getSymmetricDialect()).thenReturn(targetDialect);
        when(sourceDialect.getTargetPlatform()).thenReturn(sourcePlatform);
        when(targetDialect.getTargetPlatform()).thenReturn(targetPlatform);
        IParameterService ps = mock(IParameterService.class);
        when(sourceDialect.getParameterService()).thenReturn(ps);
        when(targetDialect.getParameterService()).thenReturn(ps);
        firstTimeSetup = false;
    }

    private void createTables() throws Exception {
        SqlScript script = new SqlScript(getClass().getResource("createSourceTables.sql"), sourcePlatform.getSqlTemplate());
        script.execute(true);
        script = new SqlScript(getClass().getResource("fillSourceTables.sql"), sourcePlatform.getSqlTemplate());
        script.execute(true);
        script = new SqlScript(getClass().getResource("createTargetTables.sql"), targetPlatform.getSqlTemplate());
        script.execute(true);
        script = new SqlScript(getClass().getResource("fillTargetTables.sql"), targetPlatform.getSqlTemplate());
        script.execute(true);
    }

    private SingleConnectionDataSource getDataSource(String url) throws Exception {
        Class.forName("org.h2.Driver");
        Connection c = DriverManager.getConnection(url);
        return new SingleConnectionDataSource(c, true);
    }

    private void changeTableBeingTested(String tableName) {
        this.sourceTableNames = new ArrayList<>();
        this.sourceTableNames.add(tableName);
        this.targetTableNames = new ArrayList<>();
        this.targetTableNames.add(tableName);
        this.config = new DbCompareConfig();
        this.config.setUseSymmetricConfig(false);
        this.config.setSourceTableNames(this.sourceTableNames);
        this.config.setTargetTableNames(this.targetTableNames);
        this.test = new DbCompare(sourceEngine, targetEngine, config);
    }

    @Test
    public void isUnitypeTest() {
        changeTableBeingTested(nonEmptyTableWithPKNoDifferences);
        boolean isUnitype = test.isUniType("unitext");
        assertTrue(isUnitype);
        isUnitype = test.isUniType("unichar");
        assertTrue(isUnitype);
        isUnitype = test.isUniType("univarchar");
        assertTrue(isUnitype);
        isUnitype = test.isUniType("UNIVARCHAR2S");
        assertFalse(isUnitype);
        isUnitype = test.isUniType(" unichar");
        assertFalse(isUnitype);
    }

    @Test
    public void compareNonEmptyTableWithPKNoDifferences() {
        changeTableBeingTested(nonEmptyTableWithPKNoDifferences);
        DbCompareReport report = test.compare();
        TableReport reportOutput = report.getTableReports().get(0);
        assertTrue(reportOutput.getSourceTable().toUpperCase().equals(nonEmptyTableWithPKNoDifferences.toUpperCase()));
        assertTrue(reportOutput.getTargetTable().toUpperCase().equals(nonEmptyTableWithPKNoDifferences.toUpperCase()));
        assertTrue(reportOutput.getSourceRows() == 2);
        assertTrue(reportOutput.getTargetRows() == 2);
        assertTrue(reportOutput.getMatchedRows() == 2);
        assertTrue(reportOutput.getDifferentRows() == 0);
        assertTrue(reportOutput.getMissingRows() == 0);
        assertTrue(reportOutput.getExtraRows() == 0);
        assertTrue(reportOutput.getErrorRows() == 0);
        assertTrue(reportOutput.getThrowable() == null);
    }

    @Test
    public void compareEmptyTableWithPK() {
        changeTableBeingTested(emptyTableWithPK);
        DbCompareReport report = test.compare();
        TableReport reportOutput = report.getTableReports().get(0);
        assertTrue(reportOutput.getSourceTable().toUpperCase().equals(emptyTableWithPK.toUpperCase()));
        assertTrue(reportOutput.getTargetTable().toUpperCase().equals(emptyTableWithPK.toUpperCase()));
        assertTrue(reportOutput.getSourceRows() == 0);
        assertTrue(reportOutput.getTargetRows() == 0);
        assertTrue(reportOutput.getMatchedRows() == 0);
        assertTrue(reportOutput.getDifferentRows() == 0);
        assertTrue(reportOutput.getMissingRows() == 0);
        assertTrue(reportOutput.getExtraRows() == 0);
        assertTrue(reportOutput.getErrorRows() == 0);
        assertTrue(reportOutput.getThrowable() == null);
    }

    @Test
    public void compareEmptyTableWithNoPK() {
        changeTableBeingTested(emptyTableWithNoPK);
        DbCompareReport report = test.compare();
        assertEquals(report.getTableReports(), null);
    }

    @Test
    public void compareNonEmptyTableWithNoPKNoDifferences() {
        changeTableBeingTested(nonEmptyTableWithNoPKNoDifferences);
        DbCompareReport report = test.compare();
        assertEquals(report.getTableReports(), null);
    }

    @Test
    public void compareNonEmptyTableWithPKWithDifferences() {
        changeTableBeingTested(nonEmptyTableWithPKWithDifferences);
        DbCompareReport report = test.compare();
        TableReport reportOutput = report.getTableReports().get(0);
        assertTrue(reportOutput.getSourceTable().toUpperCase().equals(nonEmptyTableWithPKWithDifferences.toUpperCase()));
        assertTrue(reportOutput.getTargetTable().toUpperCase().equals(nonEmptyTableWithPKWithDifferences.toUpperCase()));
        assertTrue(reportOutput.getSourceRows() == 2);
        assertTrue(reportOutput.getTargetRows() == 2);
        assertTrue(reportOutput.getMatchedRows() == 0);
        assertTrue(reportOutput.getDifferentRows() == 2);
        assertTrue(reportOutput.getMissingRows() == 0);
        assertTrue(reportOutput.getExtraRows() == 0);
        assertTrue(reportOutput.getErrorRows() == 0);
        assertTrue(reportOutput.getThrowable() == null);
    }
}
