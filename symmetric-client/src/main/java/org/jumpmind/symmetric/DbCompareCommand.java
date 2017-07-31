/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.io.DbCompare;
import org.jumpmind.symmetric.io.DbCompareConfig;
import org.jumpmind.symmetric.io.DbCompareReport;

public class DbCompareCommand extends AbstractCommandLauncher {

    private Properties configProperties;

    public DbCompareCommand() {
        super("dbcompare", "[tablename...]", "DbCompare.Option.");
    }

    @Override
    protected boolean printHelpIfNoOptionsAreProvided() {
        return false;
    }

    @Override
    protected boolean requiresPropertiesFile(CommandLine line) {
        return false;
    }

    @Override
    protected boolean executeWithOptions(CommandLine line) throws Exception {

        String source = line.getOptionValue('s');
        if (source == null) {
            source = line.getOptionValue(OPTION_SOURCE);
        }
        if (StringUtils.isEmpty(source)) {
            throw new ParseException(String.format("-source properties file is required."));   
        }

        File sourceProperies = new File(source);
        if (!sourceProperies.exists()) {
            throw new SymmetricException("Source properties file '" + sourceProperies + "' does not exist."); 
        }

        String target = line.getOptionValue('t');
        if (target == null) {
            target = line.getOptionValue(OPTION_TARGET);
        }
        if (StringUtils.isEmpty(target)) {
            throw new ParseException(String.format("-target properties file is required."));   
        }        

        File targetProperties = new File(target);
        if (!targetProperties.exists()) {
            throw new SymmetricException("Target properties file '" + targetProperties + "' does not exist."); 
        }

        DbCompareConfig config = new DbCompareConfig();

        if (line.hasOption(OPTION_OUTPUT_SQL)) {
            config.setSqlDiffFileName(line.getOptionValue(OPTION_OUTPUT_SQL));
        }
        if (line.hasOption(OPTION_USE_SYM_CONFIG)) {
            config.setUseSymmetricConfig(Boolean.valueOf(line.getOptionValue(OPTION_USE_SYM_CONFIG)));
        }
        if (line.hasOption(OPTION_EXCLUDE)) {
            config.setExcludedTableNames(Arrays.asList(line.getOptionValue(OPTION_EXCLUDE).split(",")));
        }
        if (line.hasOption(OPTION_TARGET_TABLES)) {
            config.setTargetTableNames(Arrays.asList(line.getOptionValue(OPTION_TARGET_TABLES).split(",")));
        }

        config.setWhereClauses(parseWhereClauses(line));
        config.setTablesToExcludedColumns(parseExcludedColumns(line));

        if (!CollectionUtils.isEmpty(line.getArgList())) {
            config.setIncludedTableNames(Arrays.asList(line.getArgList().get(0).toString().split(",")));
        }

        String numericScaleArg = line.getOptionValue(OPTION_NUMERIC_SCALE);
        if (!StringUtils.isEmpty(numericScaleArg)) {            
            try {
                config.setNumericScale(Integer.parseInt(numericScaleArg.trim()));
            } catch (Exception ex) {
                throw new ParseException("Failed to parse arg [" + numericScaleArg + "] " + ex);
            }
        }

        ISymmetricEngine sourceEngine = new ClientSymmetricEngine(sourceProperies);
        ISymmetricEngine targetEngine = new ClientSymmetricEngine(targetProperties);

        DbCompare dbCompare = new DbCompare(sourceEngine, targetEngine, config);
        DbCompareReport report = dbCompare.compare();

        return false;
    }

    public static void main(String[] args) throws Exception {
        new DbCompareCommand().execute(args);
    }

    protected static void initFromServerProperties() {
    }

    private static final String OPTION_SOURCE = "source";

    private static final String OPTION_TARGET = "target";

    private static final String OPTION_EXCLUDE = "exclude";

    private static final String OPTION_TARGET_TABLES = "target-tables";

    private static final String OPTION_USE_SYM_CONFIG = "use-sym-config";

    private static final String OPTION_OUTPUT_SQL = "output-sql";

    private static final String OPTION_NUMERIC_SCALE = "numeric-scale";

    private static final String OPTION_CONFIG_PROPERTIES = "config";

    @Override
    protected void printHelp(CommandLine cmd, Options options) {
        System.out.println(app + " version " + Version.version());
        System.out.println("Compare tables from 2 databases.\n");
        super.printHelp(cmd, options);
    }

    @Override
    protected void buildOptions(Options options) {
        addCommonOption(options, "h", HELP, false);
        addOption(options, "s", OPTION_SOURCE, true);
        addOption(options, "t", OPTION_TARGET, true);
        addOption(options, null, OPTION_EXCLUDE, true);
        addOption(options, null, OPTION_TARGET_TABLES, true);
        addOption(options, null, OPTION_USE_SYM_CONFIG, true);
        addOption(options, null, OPTION_OUTPUT_SQL, true);
        addOption(options, null, OPTION_NUMERIC_SCALE, true);
        addOption(options, null, OPTION_CONFIG_PROPERTIES, true);
    }

    protected Map<String, String> parseWhereClauses(CommandLine line) {
        Properties props = getConfigProperties(line);
        Map<String, String> whereClauses = new HashMap<String, String>();
        if (props != null) {
            for (Object key : props.keySet()) {
                String arg = key.toString();
                if (arg.endsWith(DbCompareConfig.WHERE_CLAUSE)) {
                    whereClauses.put(arg, props.getProperty(arg));
                }
            }
        }

        return whereClauses;
    }

    protected Map<String, List<String>> parseExcludedColumns(CommandLine line) {
        Properties props = getConfigProperties(line);
        Map<String, List<String>> tablesToExcludedColumns = new HashMap<String, List<String>>();
        if (props != null) {
            for (Object key : props.keySet()) {
                String arg = key.toString();
                if (arg.endsWith(DbCompareConfig.EXCLUDED_COLUMN)) {
                    List<String> excludedColumns =  tablesToExcludedColumns.get(key);
                    if (excludedColumns == null) {
                        excludedColumns = new ArrayList<String>();
                        tablesToExcludedColumns.put(key.toString(), excludedColumns);
                    }
                    excludedColumns.addAll(Arrays.asList(props.getProperty(arg).split(",")));
                }
            }
        }

        return tablesToExcludedColumns;

    }

    protected Properties getConfigProperties(CommandLine line) {
        if (configProperties != null) {
            return configProperties;
        } else {            
            String configPropertiesFile = line.getOptionValue(OPTION_CONFIG_PROPERTIES);
            if (!StringUtils.isEmpty(configPropertiesFile)) {
                Properties props = new Properties();
                try {
                    props.load(new FileInputStream(configPropertiesFile));
                    configProperties = props;
                    return configProperties;
                } catch (Exception ex) {
                    String qualifiedFileName = new File(configPropertiesFile).getAbsolutePath();
                    throw new SymmetricException("Could not load config properties file '" + configPropertiesFile + 
                            "' at '" + qualifiedFileName + "' ", ex);
                }    
            }
        }

        return null;
    }

    static String stripLeadingHyphens(String str) {
        if (str == null) {
            return null;
        }
        if (str.startsWith("--")) {
            return str.substring(2, str.length());
        }
        else if (str.startsWith("-")) {
            return str.substring(1, str.length());
        }

        return str;
    }    

}
