package org.jumpmind.symmetric;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;

public class DbFillCommand extends AbstractCommandLauncher {

    private static final String OPTION_SCHEMA = "schema";

    private static final String OPTION_CATALOG = "catalog";

    private static final String OPTION_COUNT = "count";

    private static final String OPTION_CASCADE = "cascade";

    private static final String OPTION_IGNORE_TABLES = "ignore";
    
    private static final String OPTION_INTERVAL = "interval";
    
    private static final String OPTION_STATEMENT = "statement";

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
        addOption(options, null, OPTION_CASCADE, false);
        addOption(options, null, OPTION_IGNORE_TABLES, true);
        addOption(options, null, OPTION_INTERVAL, true);
        addOption(options, null, OPTION_STATEMENT, true);
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
        if (line.hasOption(OPTION_CASCADE)) {
            dbFill.setCascading(true);
        }
        if (line.hasOption(OPTION_INTERVAL)) {
            dbFill.setInterval(Integer.parseInt(line.getOptionValue(OPTION_INTERVAL)));
        }
        if (line.hasOption(OPTION_STATEMENT)) {
            if (line.getOptionValue(OPTION_STATEMENT).contains("u")) {
                dbFill.setStatementType(DbFill.UPDATE);
            } else if (line.getOptionValue(OPTION_STATEMENT).contains("d")) {
                dbFill.setStatementType(DbFill.DELETE);
            }
        }
        if (line.hasOption(OPTION_DEBUG)) {
            dbFill.setDebug(true);
        }
        if (line.hasOption(OPTION_VERBOSE_CONSOLE)) {
            dbFill.setVerbose(true);
        }
        
        String ignore[] = null;
        if (line.hasOption(OPTION_IGNORE_TABLES)) {
            ignore = line.getOptionValue(OPTION_IGNORE_TABLES).split(",");
        }
        // Ignore the Symmetric config tables.
        getSymmetricEngine();
        IParameterService parameterService = engine.getParameterService();
        String cfgPrefix = parameterService.getString(ParameterConstants.RUNTIME_CONFIG_TABLE_PREFIX);
        dbFill.setIgnore((String[])ArrayUtils.add(ignore, cfgPrefix));
        
        Map<String,int[]> tableProperties = parseTableProperties();
        
        // If tables are provided in the property file, ignore the tables provided at the command line.
        String[] tableNames = null;
        if (tableProperties.size() != 0) {
            tableNames = tableProperties.keySet().toArray(new String[0]);
        } else {
            tableNames = line.getArgs();
        }

        dbFill.fillTables(tableNames, tableProperties);

        return true;
    }
    
    private Map<String,int[]> parseTableProperties() {
        Map<String,int[]> tableProperties = new HashMap<String,int[]>();
        Properties properties = engine.getProperties();
        Enumeration<Object> keys = properties.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            String value = (String) properties.get(key);
            if (key.startsWith("fill.")) {
                String tableName = null;
                tableName = key.substring(key.lastIndexOf(".") + 1);
                int[] iudVal = new int[3];
                int i = 0;
                for (String str : value.split(",")) {
                    iudVal[i++] = Integer.valueOf(str).intValue();
                }
                tableProperties.put(tableName, iudVal);
            }
        }
        return tableProperties;
    }

}
