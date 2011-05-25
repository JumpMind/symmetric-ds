package org.jumpmind.symmetric.jdbc.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.db.TableNotFoundException;
import org.jumpmind.symmetric.core.io.IoUtils;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.DataProcessor;
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
        for (TableToExtract tableToRead : tables) {
            logger.log(LogLevel.INFO,
                    String.format("Copying %s", tableToRead.getTable().getTableName()));
            Batch batch = new Batch();
            DataProcessor<DataContext> processor = new DataProcessor<DataContext>(
                    new SqlTableDataReader(this.sourcePlatform, batch, tableToRead),
                    new SqlDataWriter(this.targetPlatform, parameters));
            processor.process();
        }
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
