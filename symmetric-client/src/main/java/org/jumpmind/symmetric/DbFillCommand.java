package org.jumpmind.symmetric;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class DbFillCommand extends AbstractCommandLauncher {
    

    private static final String OPTION_SCHEMA = "schema";
    
    private static final String OPTION_CATALOG = "catalog";
    
    private static final String OPTION_COUNT = "count";
    
    public DbFillCommand() {
        super("dbfill", "[tablename...]", "DbFill.Option.");
    }
    
    public static void main(String[] args) {
        new DbFillCommand().execute(args);
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
    protected void printHelp(CommandLine cmd, Options options) {
        System.out.println(app + " version " + Version.version());
        System.out.println("Fill database tables with random generated data.\n");
        super.printHelp(cmd, options);
    }

    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
        addOption(options, null, OPTION_SCHEMA, true);
        addOption(options, null, OPTION_CATALOG, true);
        addOption(options, null, OPTION_COUNT, true);
    }
    
    @Override
    protected boolean executeWithOptions(CommandLine line) throws Exception {
        DbFill dbFill = new DbFill(getDatabasePlatform(false));

        if (line.hasOption(OPTION_SCHEMA)) {
            dbFill.setSchema(line.getOptionValue(OPTION_SCHEMA));
        }
        if (line.hasOption(OPTION_CATALOG)) {
            dbFill.setCatalog(line.getOptionValue(OPTION_CATALOG));
        }    
        if (line.hasOption(OPTION_COUNT)) {
            dbFill.setRecordCount(Integer.parseInt(line.getOptionValue(OPTION_COUNT)));
        }
        String[] args = line.getArgs();
        if (args.length == 0) {
            throw new RuntimeException("Must provide a list of tables to fill.");
        } else {
            dbFill.fillTables(args);
        }
        
        return true;
    }
    
    
    

}
