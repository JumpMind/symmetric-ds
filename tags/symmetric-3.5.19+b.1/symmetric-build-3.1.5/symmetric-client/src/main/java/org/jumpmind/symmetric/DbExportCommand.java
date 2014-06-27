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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.jumpmind.symmetric.DbExport.Compatible;
import org.jumpmind.symmetric.DbExport.Format;

/**
 * Export the structure and data from database tables to file.
 */
public class DbExportCommand extends AbstractCommandLauncher {

    private static final String OPTION_FORMAT = "format";

    private static final String OPTION_COMPATIBLE = "compatible";
    
    private static final String OPTION_ADD_DROP_TABLE = "add-drop-table";
    
    private static final String OPTION_NO_CREATE_INFO = "no-create-info";

    private static final String OPTION_NO_INDICES = "no-indices";

    private static final String OPTION_NO_FOREIGN_KEYS = "no-foreign-keys";

    private static final String OPTION_NO_DATA = "no-data";

    private static final String OPTION_USE_VARIABLE_DATES = "use-variable-dates";

    private static final String OPTION_SQL = "sql";

    private static final String OPTION_COMMENTS = "comments";
    
    private static final String OPTION_SCHEMA = "schema";
    
    private static final String OPTION_CATALOG = "catalog";

    public DbExportCommand() {
        super("dbexport", "[tablename...]", "DbExport.Option.");
    }
    
    public static void main(String[] args) {
        new DbExportCommand().execute(args);
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
        System.out.println("Export the structure and data from database tables to file.\n");
        super.printHelp(cmd, options);
    }

    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
        addOption(options, null, OPTION_FORMAT, true);
        addOption(options, null, OPTION_COMPATIBLE, true);
        addOption(options, null, OPTION_SCHEMA, true);
        addOption(options, null, OPTION_CATALOG, true);
        addOption(options, null, OPTION_ADD_DROP_TABLE, false);
        addOption(options, null, OPTION_NO_CREATE_INFO, false);
        addOption(options, null, OPTION_NO_INDICES, false);
        addOption(options, null, OPTION_NO_FOREIGN_KEYS, false);
        addOption(options, null, OPTION_NO_DATA, false);
        addOption(options, null, OPTION_USE_VARIABLE_DATES, false);
        addOption(options, null, OPTION_SQL, true);
        addOption(options, "i", OPTION_COMMENTS, false);
    }
    
    @Override
    protected boolean executeWithOptions(CommandLine line) throws Exception {
        DbExport dbExport = new DbExport(getDatabasePlatform(false));

        if (line.hasOption(OPTION_FORMAT)) {
            dbExport.setFormat(Format.valueOf(line.getOptionValue(OPTION_FORMAT).toUpperCase()));
        }
        if (line.hasOption(OPTION_COMPATIBLE)) {
            try {
                dbExport.setCompatible(Compatible.valueOf(line.getOptionValue(OPTION_COMPATIBLE).toUpperCase()));
            } catch (IllegalArgumentException ex) {
                throw new SymmetricException("Invalid compatible database option: %s", line.getOptionValue(OPTION_COMPATIBLE));
            }
        }
        if (line.hasOption(OPTION_ADD_DROP_TABLE)) {
            dbExport.setAddDropTable(true);
        }
        if (line.hasOption(OPTION_NO_CREATE_INFO)) {
            dbExport.setNoCreateInfo(true);
        }
        if (line.hasOption(OPTION_NO_INDICES)) {
            dbExport.setNoIndices(true);
        }
        if (line.hasOption(OPTION_NO_FOREIGN_KEYS)) {
            dbExport.setNoForeignKeys(true);
        }
        if (line.hasOption(OPTION_NO_DATA)) {
            dbExport.setNoData(true);
        }
        if (line.hasOption(OPTION_USE_VARIABLE_DATES)) {
            dbExport.setUseVariableForDates(true);
        }
        if (line.hasOption(OPTION_COMMENTS)) {
            dbExport.setComments(true);
        }
        if (line.hasOption(OPTION_SCHEMA)) {
            dbExport.setSchema(line.getOptionValue(OPTION_SCHEMA));
        }
        if (line.hasOption(OPTION_CATALOG)) {
            dbExport.setCatalog(line.getOptionValue(OPTION_CATALOG));
        }        
 
        String[] args = line.getArgs();
        if (args.length == 0) {
            dbExport.exportTables(System.out);
        } else if (line.hasOption(OPTION_SQL)) {
            dbExport.exportTables(System.out, args[0], line.getOptionValue(OPTION_SQL));
        } else {
            dbExport.exportTables(System.out, args);
        }
        return true;
    }

}
