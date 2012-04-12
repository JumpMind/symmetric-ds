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
import org.jumpmind.symmetric.DbDump.Compatible;
import org.jumpmind.symmetric.DbDump.Format;

/**
 * Dump the structure and data from database tables to file.
 */
public class DbDumpCommand extends AbstractCommandLauncher {

    private static final String OPTION_FORMAT = "format";

    private static final String OPTION_COMPATIBLE = "compatible";
    
    private static final String OPTION_ADD_DROP_TABLE = "add-drop-table";
    
    private static final String OPTION_NO_CREATE_INFO = "no-create-info";
    
    private static final String OPTION_NO_DATA = "no-data";

    private static final String OPTION_COMMENTS = "comments";

    public DbDumpCommand() {
        super("dbdump", "DbDump.Option.");
    }
    
    public static void main(String[] args) {
        new DbDumpCommand().execute(args);
    }
    
    protected void printHelp(Options options) {
        System.out.println(commandName + " version " + Version.version());
        System.out.println("Dump the structure and data from database tables to file.\n");
        super.printHelp(options);
    }

    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
        addOption(options, null, OPTION_FORMAT, true);
        addOption(options, null, OPTION_COMPATIBLE, true);
        addOption(options, null, OPTION_ADD_DROP_TABLE, false);
        addOption(options, null, OPTION_NO_CREATE_INFO, false);
        addOption(options, null, OPTION_NO_DATA, false);
        addOption(options, "i", OPTION_COMMENTS, false);
    }
    
    @Override
    protected boolean executeOptions(CommandLine line) throws Exception {
        DbDump dbDump = new DbDump(getDatabasePlatform());

        if (line.hasOption(OPTION_FORMAT)) {
            dbDump.setFormat(Format.valueOf(line.getOptionValue(OPTION_FORMAT).toUpperCase()));
        }
        if (line.hasOption(OPTION_COMPATIBLE)) {
            dbDump.setCompatible(Compatible.valueOf(line.getOptionValue(OPTION_COMPATIBLE).toUpperCase()));
        }
        if (line.hasOption(OPTION_ADD_DROP_TABLE)) {
            dbDump.setAddDropTable(true);
        }
        if (line.hasOption(OPTION_NO_CREATE_INFO)) {
            dbDump.setNoCreateInfo(true);
        }
        if (line.hasOption(OPTION_NO_DATA)) {
            dbDump.setNoData(true);
        }
        if (line.hasOption(OPTION_COMMENTS)) {
            dbDump.setComments(true);
        }

        String[] args = line.getArgs();
        if (args.length == 0) {
            dbDump.dumpTables(System.out);
        } else {
            dbDump.dumpTables(System.out, args);
        }
        return true;
    }

}
