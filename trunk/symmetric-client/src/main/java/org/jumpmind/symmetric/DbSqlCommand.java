package org.jumpmind.symmetric;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.dbcp.BasicDataSource;
import org.h2.tools.Shell;

public class DbSqlCommand extends AbstractCommandLauncher {
    
    private static final String OPTION_SQL = "sql";
    
    public DbSqlCommand() {
        super("dbsql", "", "DbSql.Option.");
    }
    
    public static void main(String[] args) {
        new DbSqlCommand().execute(args);
    }    
    
    @Override
    protected void printHelp(CommandLine cmd, Options options) {
        System.out.println(app + " version " + Version.version());
        System.out.println("Provides a sql shell for database interaction from the command line.\n");
        super.printHelp(cmd, options);
    }

    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
        addOption(options, null, OPTION_SQL, true);
    }    

    @Override
    protected boolean printHelpIfNoOptionsAreProvided() {
        return false;
    }

    @Override
    protected boolean requiresPropertiesFile() {
        return true;
    }

    @Override
    protected boolean executeWithOptions(CommandLine line) throws Exception {
        BasicDataSource basicDataSource = getDatabasePlatform(false).getDataSource();
        String url = basicDataSource.getUrl();
        String user = basicDataSource.getUsername();
        String password = basicDataSource.getPassword();
        String driver = basicDataSource.getDriverClassName();
        Shell shell = new Shell();
        
        if (line.hasOption(OPTION_SQL)) {
            String sql = line.getOptionValue(OPTION_SQL);
            shell.runTool("-url", url, "-user", user, "-password", password, "-driver", driver, "-sql", sql);
        } else {        
            shell.runTool("-url", url, "-user", user, "-password", password, "-driver", driver);
        }
        return true;
    }


}
