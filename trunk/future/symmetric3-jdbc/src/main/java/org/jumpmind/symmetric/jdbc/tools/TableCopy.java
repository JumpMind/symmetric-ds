package org.jumpmind.symmetric.jdbc.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.IoException;
import org.jumpmind.symmetric.core.common.IoUtils;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.TableNotFoundException;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.DataProcessor;
import org.jumpmind.symmetric.core.process.IDataFilter;
import org.jumpmind.symmetric.core.process.IDataWriter;
import org.jumpmind.symmetric.core.process.csv.CsvDataWriter;
import org.jumpmind.symmetric.core.process.sql.SqlDataWriter;
import org.jumpmind.symmetric.core.process.sql.SqlTableDataReader;
import org.jumpmind.symmetric.core.process.sql.TableToExtract;
import org.jumpmind.symmetric.jdbc.db.JdbcDbDialectFactory;
import org.jumpmind.symmetric.jdbc.tools.copy.TableCopyProperties;

public class TableCopy {

    static final Log logger = LogFactory.getLog(TableCopy.class);

    protected DataSource source;
    protected DataSource target;
    protected IDbDialect targetPlatform;
    protected IDbDialect sourcePlatform;
    protected Parameters parameters;
    protected List<TableToExtract> tablesToRead;
    protected File targetFile;
    
    public TableCopy() {     
    }

    public TableCopy(TableCopyProperties properties) {
        this.parameters = new Parameters(properties);

        this.source = properties.getSourceDataSource();
        this.sourcePlatform = JdbcDbDialectFactory.createPlatform(source, parameters);

        this.targetFile = properties.getTargetFile();
        if (targetFile == null) {
            this.target = properties.getTargetDataSource();
            this.targetPlatform = JdbcDbDialectFactory.createPlatform(target, parameters);
        } else if (targetFile.exists()) {
            throw new IllegalStateException(targetFile.getName() + " already exists");
        }

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
            logger.info("(%d of %d) Copying table %s ", batchId, tables.size(), tableToRead
                    .getTable().getTableName());
            Batch batch = new Batch(batchId++);
            int expectedCount = this.sourcePlatform.getSqlConnection().queryForInt(
                    this.sourcePlatform.getDataCaptureBuilder().createTableExtractCountSql(tableToRead,
                            parameters));
            DataProcessor processor = new DataProcessor(new SqlTableDataReader(this.sourcePlatform,
                    batch, tableToRead), getDataWriter(expectedCount));
            processor.process(new DataContext(parameters));
        }
    }

    protected IDataWriter getDataWriter(final int expectedCount) {
        IDataFilter progressFilter = new IDataFilter() {
            int statementCount = 0;
            int percent = 0;

            public boolean filter(DataContext context, Table table, Data data) {
                statementCount++;
                int currentPercent = (int) (((double) statementCount / (double) expectedCount) * 100);
                if (currentPercent != percent) {
                    percent = currentPercent;
                    logger.info(buildProgressBar(percent, expectedCount, percent < 100));
                }
                return true;
            }
        };

        if (targetFile != null) {
            try {
                return new CsvDataWriter(this.targetFile, progressFilter);
            } catch (IOException e) {
                throw new IoException(e);
            }
        } else {
            return new SqlDataWriter(this.targetPlatform, parameters, progressFilter);
        }
    }

    protected String buildProgressBar(int percent, int expectedCount, boolean includeCarriageReturn) {
        StringBuilder b = new StringBuilder("|");
        for (int i = 1; i <= 25; i++) {
            if (percent >= i * 4) {
                b.append("=");
            } else {
                b.append(" ");
            }
        }
        b.append("| ");
        b.append(percent);
        b.append("% of ");
        b.append(expectedCount);
        b.append(" rows");
        if (includeCarriageReturn) {
            b.append("\r");
        }
        return b.toString();
    }

    public Parameters getParameters() {
        return parameters;
    }

    public DataSource getSource() {
        return source;
    }

    public IDbDialect getSourcePlatform() {
        return sourcePlatform;
    }

    public List<TableToExtract> getTablesToRead() {
        return tablesToRead;
    }

    public DataSource getTarget() {
        return target;
    }

    public IDbDialect getTargetPlatform() {
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
