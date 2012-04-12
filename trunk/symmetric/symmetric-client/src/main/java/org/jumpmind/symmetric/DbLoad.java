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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.csv.CsvReader;

/**
 * Load data from file to database tables.
 */
public class DbLoad extends AbstractCommandLauncher {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DbLoad.class);

    private static final String OPTION_XML = "xml";

    private static final String OPTION_CSV = "csv";
    
    public DbLoad(String commandName, String messageKeyPrefix) {
        super(commandName, messageKeyPrefix);
    }

    public static void main(String[] args) throws Exception {
        new DbLoad("dbload", "DbLoad.Option.").execute(args);
    }
    
    protected void printHelp(Options options) {
        System.out.println(commandName + " version " + Version.version());
        System.out.println("Load data from file to database tables.\n");
        super.printHelp(options);
    }

    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
        addOption(options, "x", OPTION_XML, false);
        addOption(options, null, OPTION_CSV, false);
    }
    
    @Override
    protected boolean executeOptions(CommandLine line) throws Exception {
        String[] args = line.getArgs();
        
        if (args.length == 0) {
            executeOption(line, System.in);
        } else {
            for (String fileName : args) {
                if (! new File(fileName).exists()) {
                    throw new RuntimeException("Cannot find file " + fileName);
                }
            }
            for (String fileName : args) {
                executeOption(line, new FileInputStream(fileName));
            }
        }

        return true;
    }

    protected void executeOption(CommandLine line, InputStream in) throws Exception {
        if (line.hasOption(OPTION_XML)) {
            loadTablesFromXml(in);
        } else if (line.hasOption(OPTION_CSV)) {
            loadTablesFromCsv(in);           
        } else {
            loadTablesFromSql(in);
        }
    }

    public void loadTablesFromXml(InputStream in) {
        // TODO: read in data from XML also
        IDatabasePlatform platform = getDatabasePlatform();
        Database database = new DatabaseIO().read(in);
        platform.createDatabase(database, false, true);
    }

    public void loadTablesFromCsv(InputStream in) throws IOException {
        IDatabasePlatform platform = getDatabasePlatform();
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        Table table = platform.readTableFromDatabase(platform.getDefaultCatalog(), platform.getDefaultSchema(), "item");
        if (table == null) {
            throw new RuntimeException("Unable to find table");
        }
        DmlStatement statement = platform.createDmlStatement(DmlType.INSERT, table);

        CsvReader csvReader = new CsvReader(new InputStreamReader(in));
        csvReader.setEscapeMode(CsvReader.ESCAPE_MODE_BACKSLASH);
        csvReader.setSafetySwitch(false);
        csvReader.setUseComments(true);
        csvReader.readHeaders();

        while (csvReader.readRecord()) {
            String[] values = csvReader.getValues();
            Object[] data = platform.getObjectValues(BinaryEncoding.HEX, table, csvReader.getHeaders(), values);
            for (String value : values) {
                System.out.print("|" + value);
            }
            System.out.print("\n");            
            int rows = sqlTemplate.update(statement.getSql(), data);
            System.out.println(rows + " rows updated.");
        }
        csvReader.close();
    }

    public void loadTablesFromSql(InputStream in) throws Exception {
        IDatabasePlatform platform = getDatabasePlatform();

        // TODO: SqlScript should be able to stream from standard input to run large SQL script
        List<String> lines = IOUtils.readLines(in);

        SqlScript script = new SqlScript(lines, platform.getSqlTemplate(), true, SqlScript.QUERY_ENDS, 
                platform.getSqlScriptReplacementTokens());
        script.execute();
    }
}