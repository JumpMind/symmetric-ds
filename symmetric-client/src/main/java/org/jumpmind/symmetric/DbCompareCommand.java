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
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.DbCompare;
import org.jumpmind.symmetric.io.DbCompareReport;
import org.jumpmind.symmetric.io.DbCompareReport.TableReport;
import org.jumpmind.symmetric.io.data.DbFill;
import org.jumpmind.symmetric.service.IParameterService;

public class DbCompareCommand extends AbstractCommandLauncher {
    
    public DbCompareCommand() {
        super("dbcompare", "[tablename...]", "DbCompare.Option.");
    }

    @Override
    protected boolean printHelpIfNoOptionsAreProvided() {
        return false;
    }

    @Override
    protected boolean requiresPropertiesFile() {
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
        
        ISymmetricEngine sourceEngine = new ClientSymmetricEngine(sourceProperies);
        ISymmetricEngine targetEngine = new ClientSymmetricEngine(targetProperties);
        
        DbCompare dbCompare = new DbCompare(sourceEngine, targetEngine);
        
        if (line.hasOption(OPTION_OUTPUT_SQL)) {
            dbCompare.setSqlDiffFileName(line.getOptionValue(OPTION_OUTPUT_SQL));
        }
        if (line.hasOption(OPTION_EXCLUDE)) {
            dbCompare.setExcludedTableNames(Arrays.asList(line.getOptionValue(OPTION_EXCLUDE).split(",")));
        }
        if (!CollectionUtils.isEmpty(line.getArgList())) {
        	dbCompare.setIncludedTableNames(Arrays.asList(line.getArgList().get(0).toString().split(",")));
        }
        
        String numericScaleArg = line.getOptionValue(OPTION_NUMERIC_SCALE);
        if (!StringUtils.isEmpty(numericScaleArg)) {            
            try {
                dbCompare.setNumericScale(Integer.parseInt(numericScaleArg.trim()));
            } catch (Exception ex) {
                throw new ParseException("Failed to parse arg [" + numericScaleArg + "] " + ex);
            }
        }
        
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

    private static final String OPTION_USE_SYM_CONFIG = "use-sym-config";

    private static final String OPTION_OUTPUT_SQL = "output-sql";
    
    private static final String OPTION_NUMERIC_SCALE = "numeric-scale";

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
        addOption(options, null, OPTION_USE_SYM_CONFIG, false);
        addOption(options, null, OPTION_OUTPUT_SQL, true);
        addOption(options, null, OPTION_NUMERIC_SCALE, true);
    }

}
