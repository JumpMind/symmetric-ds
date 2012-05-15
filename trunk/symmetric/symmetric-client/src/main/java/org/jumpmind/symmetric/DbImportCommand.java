/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.jumpmind.symmetric.DbImport.Format;

/**
 * Import data from file to database tables.
 */
public class DbImportCommand extends AbstractCommandLauncher {

    private static final String OPTION_FORMAT = "format";
    
    private static final String OPTION_CATALOG = "catalog";
    
    private static final String OPTION_SCHEMA = "schema";
    
    private static final String OPTION_TABLE = "table";
    
    private static final String OPTION_USE_VARIABLE_DATES = "use-variable-dates";

    public DbImportCommand() {
        super("dbimport", "[file...]", "DbImport.Option.");
    }
    
    public static void main(String[] args) {
        new DbImportCommand().execute(args);
    }
    
    @Override
    protected boolean printHelpIfNoOptionsAreProvided() {
        return true;
    }
    
    @Override
    protected boolean requiresPropertiesFile() {
        return true;
    }
    
    protected void printHelp(Options options) {
        System.out.println(app + " version " + Version.version());
        System.out.println("Import data from file to database tables.\n");
        super.printHelp(options);
    }

    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
        addOption(options, null, OPTION_FORMAT, true);
        addOption(options, null, OPTION_CATALOG, true);
        addOption(options, null, OPTION_SCHEMA, true);
        addOption(options, null, OPTION_TABLE, true);
        addOption(options, null, OPTION_USE_VARIABLE_DATES, false);
    }
    
    @Override
    protected boolean executeWithOptions(CommandLine line) throws Exception {
        DbImport dbImport = new DbImport(getDatabasePlatform());

        if (line.hasOption(OPTION_FORMAT)) {
            dbImport.setFormat(Format.valueOf(line.getOptionValue(OPTION_FORMAT).toUpperCase()));
        }
        if (line.hasOption(OPTION_CATALOG)) {
            dbImport.setCatalog(line.getOptionValue(OPTION_CATALOG));
        }
        if (line.hasOption(OPTION_SCHEMA)) {
            dbImport.setSchema(line.getOptionValue(OPTION_SCHEMA));
        }
        if (line.hasOption(OPTION_USE_VARIABLE_DATES)) {
            dbImport.setUseVariableForDates(true);
        }

        String[] args = line.getArgs();
        if (args.length == 0) {
            dbImport.importTables(System.in, line.getOptionValue(OPTION_TABLE));
        } else {
            for (String fileName : args) {
                if (! new File(fileName).exists()) {
                    throw new RuntimeException("Cannot find file " + fileName);
                }
            }
            for (String fileName : args) {
                FileInputStream in = new FileInputStream(fileName);
                dbImport.importTables(in, line.getOptionValue(OPTION_TABLE));
                in.close();
            }
        }

        return true;
    }
}
