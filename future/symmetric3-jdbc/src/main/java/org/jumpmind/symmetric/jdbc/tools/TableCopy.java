package org.jumpmind.symmetric.jdbc.tools;

import java.io.File;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.io.IoUtils;
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
                DataSource targetDataSource = properties.getTargetDataSource();
                DataSource sourceDataSource = properties.getTargetDataSource();
            }
        } else {
            System.err
                    .println("Please provide the name of a configuration file as an argument to this utility.  Example content is as follows:");
            System.err.println();
            System.err.println(IoUtils.toString(TableCopyProperties.getExampleInputStream()));

        }
    }
}
