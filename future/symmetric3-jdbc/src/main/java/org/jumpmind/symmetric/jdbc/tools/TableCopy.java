package org.jumpmind.symmetric.jdbc.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.db.TableNotFoundException;
import org.jumpmind.symmetric.core.io.IoUtils;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.DataProcessor;
import org.jumpmind.symmetric.core.process.IDataFilter;
import org.jumpmind.symmetric.core.process.sql.SqlDataWriter;
import org.jumpmind.symmetric.core.process.sql.SqlTableDataReader;
import org.jumpmind.symmetric.core.process.sql.TableToExtract;
import org.jumpmind.symmetric.jdbc.db.JdbcDbPlatformFactory;
import org.jumpmind.symmetric.jdbc.tools.copy.TableCopyProperties;

public class TableCopy {

    static final Log logger = LogFactory.getLog(TableCopy.class);

    protected DataSource source;
    protected DataSource target;
    protected IDbPlatform targetPlatform;
    protected IDbPlatform sourcePlatform;
    protected Parameters parameters;
    protected List<TableToExtract> tablesToRead;

    public TableCopy(TableCopyProperties properties) {
        this.parameters = new Parameters(properties);

        this.source = properties.getSourceDataSource();
        this.sourcePlatform = JdbcDbPlatformFactory.createPlatform(source, parameters);

        this.target = properties.getTargetDataSource();
        this.targetPlatform = JdbcDbPlatformFactory.createPlatform(target, parameters);

        String[] tableNames = properties.getTables();

        tablesToRead = new ArrayList<TableToExtract>();
        for (String tableName : tableNames) {
            Table table = sourcePlatform.findTable(tableName);
            if (table != null) {
                String condition = properties.getConditionForTable(tableName);
                table.setSchemaName(null);
                table.setCatalogName(null);
                tablesToRead.add(new TableToExtract(table, condition));
            } else {
                throw new TableNotFoundException(tableName);
            }
        }
    }

    public void copy() {
        this.copy(tablesToRead);
    }

    public void copy(List<TableToExtract> tables) {
        long batchId = 1;
        for (TableToExtract tableToRead : tables) {
            logger.info("Copying %s (%d of %d)", tableToRead.getTable().getTableName(), batchId, tables.size());
            Batch batch = new Batch(batchId++);
            final int expectedCount = this.sourcePlatform.getSqlConnection().queryForInt(
                    this.sourcePlatform.getTriggerBuilder().createTableExtractCountSql(tableToRead,
                            parameters));
            DataProcessor processor = new DataProcessor(new SqlTableDataReader(this.sourcePlatform,
                    batch, tableToRead), new SqlDataWriter(this.targetPlatform, parameters,
                    new IDataFilter() {
                        int statementCount = 0;
                        int percent = 0;

                        public boolean filter(DataContext context, Table table, Data data) {
                            statementCount++;
                            int currentPercent = (int) ((double) statementCount / (double) expectedCount) * 100;
                            if (currentPercent != percent) {
                                percent = currentPercent;
                                logger.info(buildProgressBar(percent, expectedCount));
                            }
                            return true;
                        }
                    }));
            processor.process(new DataContext(parameters));
        }
    }

    protected String buildProgressBar(int percent, int expectedCount) {
        StringBuilder b = new StringBuilder("|");
        for (int i = 1; i <= 20; i++) {
            if (percent >= i * 5) {
                b.append("=");
            } else {
                b.append(" ");
            }
        }
        b.append("| ");
        b.append(percent);
        b.append("% of ");
        b.append(expectedCount);
        b.append(" rows\r");
        return b.toString();
    }

    public Parameters getParameters() {
        return parameters;
    }

    public DataSource getSource() {
        return source;
    }

    public IDbPlatform getSourcePlatform() {
        return sourcePlatform;
    }

    public List<TableToExtract> getTablesToRead() {
        return tablesToRead;
    }

    public DataSource getTarget() {
        return target;
    }

    public IDbPlatform getTargetPlatform() {
        return targetPlatform;
    }

    public static void main(String[] args) {
        if (args != null && args.length > 0) {
            File propFile = new File(args[0]);
            if (propFile.exists() && !propFile.isDirectory()) {
                TableCopyProperties properties = new TableCopyProperties(propFile);
                new TableCopy(properties).copy();
            } else {
                System.err.println(String.format("Could not find the properties file named %s",
                        args[0]));
            }
        } else {
            System.err
                    .println("Please provide the name of a configuration file as an argument to this utility.  Example content is as follows:");
            System.err.println();
            System.err.println(IoUtils.toString(TableCopyProperties.getExampleInputStream()));

        }
    }
    
}
