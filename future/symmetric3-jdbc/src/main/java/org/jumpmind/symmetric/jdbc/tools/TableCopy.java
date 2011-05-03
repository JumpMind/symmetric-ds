package org.jumpmind.symmetric.jdbc.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.IPlatform;
import org.jumpmind.symmetric.core.io.IoUtils;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataProcessor;
import org.jumpmind.symmetric.core.process.sql.SqlDataContext;
import org.jumpmind.symmetric.core.process.sql.SqlDataWriter;
import org.jumpmind.symmetric.core.process.sql.SqlTableDataReader;
import org.jumpmind.symmetric.core.process.sql.TableToRead;
import org.jumpmind.symmetric.jdbc.db.JdbcPlatformFactory;
import org.jumpmind.symmetric.jdbc.tools.copy.TableCopyProperties;

public class TableCopy {

    protected DataSource source;
    protected DataSource target;
    protected IPlatform targetPlatform;
    protected IPlatform sourcePlatform;
    protected Parameters parameters;
    protected List<TableToRead> tablesToRead;

    public TableCopy(TableCopyProperties properties) {
        this.source = properties.getSourceDataSource();
        this.sourcePlatform = JdbcPlatformFactory.createPlatform(source);

        this.target = properties.getTargetDataSource();
        this.targetPlatform = JdbcPlatformFactory.createPlatform(target);

        this.parameters = new Parameters(properties);

        String[] tableNames = properties.getTables();

        List<TableToRead> tablesToCopy = new ArrayList<TableToRead>();
        for (String tableName : tableNames) {
            Table table = sourcePlatform.findTable(tableName, parameters);
            String condition = properties.getConditionForTable(tableName);
            tablesToCopy.add(new TableToRead(table, condition));
        }
    }

    public void copy() {
        this.copy(tablesToRead);
    }

    public void copy(List<TableToRead> tables) {
        SqlTableDataReader reader = new SqlTableDataReader(this.sourcePlatform, tables);
        SqlDataWriter writer = new SqlDataWriter(this.target, this.targetPlatform, parameters,
                null, null);
        DataProcessor<SqlDataContext> processor = new DataProcessor<SqlDataContext>(reader,
                writer);
        processor.process();

    }
    
    public Parameters getParameters() {
        return parameters;
    }
    
    public DataSource getSource() {
        return source;
    }
    
    public IPlatform getSourcePlatform() {
        return sourcePlatform;
    }
    
    public List<TableToRead> getTablesToRead() {
        return tablesToRead;
    }
    
    public DataSource getTarget() {
        return target;
    }
    
    public IPlatform getTargetPlatform() {
        return targetPlatform;
    }

    public static void main(String[] args) {

        if (args != null && args.length > 0) {
            File propFile = new File(args[0]);
            if (propFile.exists() && !propFile.isDirectory()) {
                TableCopyProperties properties = new TableCopyProperties(propFile);
                new TableCopy(properties).copy();

            }
        } else {
            System.err
                    .println("Please provide the name of a configuration file as an argument to this utility.  Example content is as follows:");
            System.err.println();
            System.err.println(IoUtils.toString(TableCopyProperties.getExampleInputStream()));

        }
    }
}
