package org.jumpmind.symmetric.jdbc.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.IoUtils;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.TableNotFoundException;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.AbstractDataProcessorListener;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.DataProcessor;
import org.jumpmind.symmetric.core.process.IDataFilter;
import org.jumpmind.symmetric.core.process.IDataWriter;
import org.jumpmind.symmetric.core.process.csv.CsvDataReader;
import org.jumpmind.symmetric.core.process.csv.FileCsvDataWriter;
import org.jumpmind.symmetric.core.process.sql.SqlDataWriter;
import org.jumpmind.symmetric.core.process.sql.SqlTableDataReader;
import org.jumpmind.symmetric.core.process.sql.TableToExtract;
import org.jumpmind.symmetric.jdbc.db.JdbcDbDialectFactory;
import org.jumpmind.symmetric.jdbc.tools.copy.TableCopyProperties;

public class TableCopy {

    static final Log logger = LogFactory.getLog(TableCopy.class);

    public static void main(String[] args) {
        try {
            if (args != null && args.length > 0) {
                File propFile = new File(args[0]);
                if (propFile.exists() && !propFile.isDirectory()) {
                    TableCopyProperties properties = new TableCopyProperties(propFile);
                    new TableCopy(properties).copy();
                    System.exit(0);
                } else {
                    System.err.println(String.format("Could not find the properties file named %s",
                            args[0]));
                    System.exit(-1);
                }
            } else {
                System.err
                        .println("Please provide the name of a configuration file as an argument to this utility.  Example content is as follows:");
                System.err.println();
                System.err.println(IoUtils.toString(TableCopyProperties.getExampleInputStream()));
                System.exit(-1);
            }
        } catch (TableNotFoundException ex) {
            logger.error(ex.getMessage());
            System.exit(-1);
        } catch (IllegalStateException ex) {
            logger.error(ex.getMessage());
            System.exit(-1);
        } catch (Exception ex) {
            logger.error(ex);
            System.exit(-1);
        }
    }

    protected DataSource sourceDataSource;
    protected DataSource targetDataSource;
    protected IDbDialect targetDbDialect;
    protected IDbDialect sourceDbDialect;
    protected Parameters parameters;
    protected List<TableToExtract> tablesToRead;
    protected File[] sourceFiles;
    protected File targetFileDir;

    public TableCopy() {
    }

    public TableCopy(TableCopyProperties properties) {
        this.parameters = new Parameters(properties);

        this.sourceFiles = properties.getSourceFiles();
        if (this.sourceFiles == null || this.sourceFiles.length == 0) {
            this.sourceDataSource = properties.getSourceDataSource();
            this.sourceDbDialect = JdbcDbDialectFactory
                    .createPlatform(sourceDataSource, parameters);
        }

        this.targetFileDir = properties.getTargetFileDir();
        if (targetFileDir == null) {
            this.targetDataSource = properties.getTargetDataSource();
            this.targetDbDialect = JdbcDbDialectFactory
                    .createPlatform(targetDataSource, parameters);
        }

        IDbDialect dialect2UseForMetadataLookup = sourceDbDialect != null ? sourceDbDialect
                : targetDbDialect;

        String[] tableNames = properties.getTables();

        tablesToRead = new ArrayList<TableToExtract>();
        for (String tableName : tableNames) {
            Table table = dialect2UseForMetadataLookup.findTable(properties.getSourceCatalog(),
                    properties.getSourceSchema(), tableName, false);
            if (table != null) {
                String condition = properties.getConditionForTable(tableName);
                table.setSchemaName(null);
                table.setCatalogName(null);
                tablesToRead.add(new TableToExtract(table, condition));
            } else {
                if (sourceDbDialect != null
                        || !parameters.is(Parameters.LOADER_CREATE_TABLE_IF_DOESNT_EXIST, false)) {
                    throw new TableNotFoundException(tableName);
                }
            }
        }

    }

    public void copy() {
        if (parameters.is(Parameters.LOADER_DELETE_FIRST, false)) {
            delete(tablesToRead);
        }
        if (sourceFiles == null) {
            this.copyFromTables(tablesToRead);
        } else {
            this.copyFromFiles(sourceFiles);
        }
    }

    public void delete(List<TableToExtract> tables) {
        if (tables != null) {
            for (int i = tables.size() - 1; i >= 0; i--) {
                TableToExtract tableToDelete = tables.get(i);
                logger.info("(%d of %d) Deleting table %s ", tables.size() - i, tables.size(),
                        tableToDelete.getTable().getTableName());
                this.targetDbDialect.getSqlTemplate().delete(tableToDelete.getTable(), null);
            }
        }
    }

    public void copyFromTables(List<TableToExtract> tables) {
        long batchId = 1;
        for (TableToExtract tableToRead : tables) {
            logger.info("(%d of %d) Copying table %s ", batchId, tables.size(), tableToRead
                    .getTable().getTableName());
            Batch batch = new Batch(batchId++);
            int expectedCount = this.sourceDbDialect.getSqlTemplate().queryForInt(
                    this.sourceDbDialect.getDataCaptureBuilder().createTableExtractCountSql(
                            tableToRead, parameters));
            long ts = System.currentTimeMillis();
            DataProcessor processor = new DataProcessor(new SqlTableDataReader(
                    this.sourceDbDialect, batch, tableToRead), getDataWriter(true, expectedCount));
            processor.process(new DataContext(parameters));
            long totalTableCopyTime = System.currentTimeMillis() - ts;
            logger.info(
                    "It took %d ms to copy %d rows from table %s.  It took %d ms to read the data and %d ms to write the data.",
                    totalTableCopyTime, batch.getLineCount(),
                    tableToRead.getTable().getTableName(), batch.getDataReadMillis(),
                    batch.getDataWriteMillis());

        }
    }

    public void copyFromFiles(File[] sourceFiles) {
        for (File file : sourceFiles) {
            if (file.exists()) {
                long ts = System.currentTimeMillis();
                DataProcessor processor = new DataProcessor(new CsvDataReader(file), getDataWriter(
                        false, file.length()));
                LoadListenerListener loadListener = new LoadListenerListener();
                processor.setListener(loadListener);
                processor.process(new DataContext(parameters));
                long totalTableCopyTime = System.currentTimeMillis() - ts;
                if (loadListener.getTable() != null) {
                    Batch batch = loadListener.getBatch();
                    logger.info(
                            "It took %d ms to copy %d rows from table %s.  It took %d ms to read the data and %d ms to write the data. %d rows were inserted.",
                            totalTableCopyTime, batch.getLineCount(), loadListener.getTable()
                                    .getTableName(), batch.getDataReadMillis(), batch
                                    .getDataWriteMillis(), batch.getInsertCount());
                    if (batch.getFallbackUpdateCount() > 0) {
                        logger.info(
                                "The data loader fell back to an update %d times during the load",
                                batch.getFallbackUpdateCount());
                    }
                    if (batch.getInsertCollisionCount() > 0) {
                        logger.info(
                                "The data loader collided %d times during the load.  All row collisions were ignored.",
                                batch.getInsertCollisionCount());
                    }

                }
            } else {
                logger.error("Could not find " + file.getName());
            }
        }

    }

    /**
     * @param expectedSizeIsInRows
     *            If true, the expected size will be in row count format. If
     *            false, the expected size will be in bytes.
     * @param expectedSize
     * @return
     */
    protected IDataWriter getDataWriter(final boolean expectedSizeIsInRows, final long expectedSize) {
        IDataFilter progressFilter = new IDataFilter() {
            long statementCount = 0;
            long currentBatchSize = 0;
            long totalBatchSize = 0;
            int percent = 0;

            public boolean filter(DataContext context, Batch batch, Table table, Data data) {
                statementCount++;
                if (batch.getReadByteCount() < currentBatchSize) {
                    totalBatchSize += currentBatchSize;
                }
                currentBatchSize = batch.getReadByteCount();
                long actualSize = statementCount;
                if (!expectedSizeIsInRows) {
                    actualSize = currentBatchSize + totalBatchSize;
                }
                int currentPercent = (int) (((double) actualSize / (double) expectedSize) * 100);
                if (currentPercent != percent) {
                    percent = currentPercent;
                    logger.info(buildProgressBar(percent, batch.getLineCount()));
                }
                return true;
            }
        };

        if (targetFileDir != null) {
            return new FileCsvDataWriter(this.targetFileDir, progressFilter);
        } else {
            return new SqlDataWriter(this.targetDbDialect, parameters, progressFilter);
        }
    }

    protected String buildProgressBar(int percent, long lineCount) {
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
        b.append("% Processed ");
        b.append(lineCount);
        b.append(" rows ");
        b.append("\r");
        return b.toString();
    }

    public Parameters getParameters() {
        return parameters;
    }

    public DataSource getSource() {
        return sourceDataSource;
    }

    public IDbDialect getSourcePlatform() {
        return sourceDbDialect;
    }

    public List<TableToExtract> getTablesToRead() {
        return tablesToRead;
    }

    public DataSource getTarget() {
        return targetDataSource;
    }

    public IDbDialect getTargetPlatform() {
        return targetDbDialect;
    }

    class LoadListenerListener extends AbstractDataProcessorListener {
        private Table table;
        private Batch batch;

        public boolean processTable(DataContext context, Batch batch, Table table) {
            this.table = table;
            return true;
        }

        @Override
        public boolean batchBegin(DataContext context, Batch batch) {
            this.batch = batch;
            return true;
        }

        public Batch getBatch() {
            return batch;
        }

        public Table getTable() {
            return table;
        }
    }

}
