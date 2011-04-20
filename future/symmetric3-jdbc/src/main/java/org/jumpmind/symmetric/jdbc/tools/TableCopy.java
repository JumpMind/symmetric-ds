package org.jumpmind.symmetric.jdbc.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.io.IoUtils;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.jdbc.db.IJdbcPlatform;
import org.jumpmind.symmetric.jdbc.db.JdbcPlatformFactory;
import org.jumpmind.symmetric.jdbc.tools.copy.TableCopyProperties;
import org.jumpmind.symmetric.jdbc.tools.copy.TableToCopy;

public class TableCopy {

    DataSource source;
    DataSource target;

    public TableCopy(DataSource source, DataSource target) {
        this.source = source;
        this.target = target;
    }

    public void copy(List<TableToCopy> tables) {

    }

    public static void main(String[] args) {

        if (args != null && args.length > 0) {
            File propFile = new File(args[0]);
            if (propFile.exists() && !propFile.isDirectory()) {
                TableCopyProperties properties = new TableCopyProperties(propFile);
                DataSource source = properties.getTargetDataSource();
                DataSource target = properties.getTargetDataSource();
                TableCopy copier = new TableCopy(source, target);
                IJdbcPlatform platform = JdbcPlatformFactory.createPlatform(source);
                String[] tableNames = properties.getTables();
                Parameters parameters = new Parameters(properties);
                List<TableToCopy> tablesToCopy = new ArrayList<TableToCopy>();
                for (String tableName : tableNames) {
                    Table table = platform.findTable(tableName, parameters);
                    String condition = properties.getConditionForTable(tableName);
                    tablesToCopy.add(new TableToCopy(table, condition));
                }
                copier.copy(tablesToCopy);
                
            }
        } else {
            System.err
                    .println("Please provide the name of a configuration file as an argument to this utility.  Example content is as follows:");
            System.err.println();
            System.err.println(IoUtils.toString(TableCopyProperties.getExampleInputStream()));

        }
    }
}
