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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.nio.charset.Charset;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.symmetric.common.SecurityConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.ISecurityService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.JarBuilder;

/**
 * Perform administration tasks with SymmetricDS.
 */
public class SymmetricAdmin extends AbstractCommandLauncher {

    @SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(SymmetricAdmin.class);

    private static final String OPTION_DUMP_BATCH = "dump-batch";

    private static final String OPTION_OPEN_REGISTRATION = "open-registration";

    private static final String OPTION_RELOAD_NODE = "reload-node";

    private static final String OPTION_AUTO_CREATE = "auto-create";

    private static final String OPTION_DDL_GEN = "generate-config-ddl";

    private static final String OPTION_TRIGGER_GEN = "generate-triggers";

    private static final String OPTION_TRIGGER_GEN_ALWAYS = "generate-triggers-always";

    private static final String OPTION_PURGE = "purge";

    private static final String OPTION_RUN_DDL_XML = "run-ddl";

    private static final String OPTION_RUN_SQL = "run-sql";

    private static final String OPTION_PROPERTIES_GEN = "generate-default-properties";

    private static final String OPTION_LOAD_BATCH = "load-batch";

    private static final String OPTION_ENCRYPT_TEXT = "encrypt";

    private static final String OPTION_CREATE_WAR = "create-war";

    public SymmetricAdmin(String commandName, String messageKeyPrefix) {
		super(commandName, messageKeyPrefix);
	}

	public static void main(String[] args) throws Exception {
        new SymmetricAdmin("symadmin", "SymAdmin.Option.").execute(args);
    }
	
    protected void printHelp(Options options) {
    	System.out.println(commandName + " version " + Version.version());
    	System.out.println("Perform administration tasks with SymmetricDS.\n");
    	super.printHelp(options);
    }

	@Override
    protected void buildOptions(Options options) {
		super.buildOptions(options);
        addOption(options, "c", OPTION_DDL_GEN, true);
        addOption(options, "X", OPTION_PURGE, false);
        addOption(options, "g", OPTION_PROPERTIES_GEN, true);
        addOption(options, "r", OPTION_RUN_DDL_XML, true);
        addOption(options, "s", OPTION_RUN_SQL, true);
        addOption(options, "a", OPTION_AUTO_CREATE, false);
        addOption(options, "R", OPTION_OPEN_REGISTRATION, true);
        addOption(options, "l", OPTION_RELOAD_NODE, true);
        addOption(options, "d", OPTION_DUMP_BATCH, true);
        addOption(options, "b", OPTION_LOAD_BATCH, true);
        addOption(options, "t", OPTION_TRIGGER_GEN, true);
        addOption(options, "o", OPTION_TRIGGER_GEN_ALWAYS, false);
        addOption(options, "e", OPTION_ENCRYPT_TEXT, true);
        addOption(options, "w", OPTION_CREATE_WAR, true);
        addOption(options, "y", OPTION_ENCRYPT_TEXT, true);
        buildCryptoOptions(options);
    }
    
	@Override
	protected boolean executeOptions(CommandLine line) throws Exception {

		configureCrypto(line);

        if (line.hasOption(OPTION_PROPERTIES_GEN)) {
            generateDefaultProperties(line.getOptionValue(OPTION_PROPERTIES_GEN));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_CREATE_WAR)) {
            generateWar(line.getOptionValue(OPTION_CREATE_WAR), line.getOptionValue(OPTION_PROPERTIES_FILE));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_DDL_GEN)) {
            generateDDL(getSymmetricEngine(), line.getOptionValue(OPTION_DDL_GEN));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_PURGE)) {
            ISymmetricEngine engine = getSymmetricEngine();
            IPurgeService purgeService = engine.getPurgeService();
            purgeService.purgeOutgoing();
            purgeService.purgeIncoming();
            purgeService.purgeDataGaps();
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_OPEN_REGISTRATION)) {
            String arg = line.getOptionValue(OPTION_OPEN_REGISTRATION);
            openRegistration(getSymmetricEngine(), arg);
            System.out.println(String.format("Opened Registration for %s", arg));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_RELOAD_NODE)) {
            String arg = line.getOptionValue(OPTION_RELOAD_NODE);
            String message = reloadNode(getSymmetricEngine(), arg);
            System.out.println(message);
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_DUMP_BATCH)) {
            String arg = line.getOptionValue(OPTION_DUMP_BATCH);
            dumpBatch(getSymmetricEngine(), arg);
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_TRIGGER_GEN)) {
            String arg = line.getOptionValue(OPTION_TRIGGER_GEN);
            boolean gen_always = line.hasOption(OPTION_TRIGGER_GEN_ALWAYS);
            syncTrigger(getSymmetricEngine(), arg, gen_always);
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_AUTO_CREATE)) {
            autoCreateDatabase(getSymmetricEngine());
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_RUN_DDL_XML)) {
            runDdlXml(getSymmetricEngine(), line.getOptionValue(OPTION_RUN_DDL_XML));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_RUN_SQL)) {
            runSql(getSymmetricEngine(), line.getOptionValue(OPTION_RUN_SQL));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_LOAD_BATCH)) {
            loadBatch(getSymmetricEngine(), line.getOptionValue(OPTION_LOAD_BATCH));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_ENCRYPT_TEXT)) {
            encryptText(getSymmetricEngine(), line.getOptionValue(OPTION_ENCRYPT_TEXT));
            return true;
        }
		return false;
	}

    private void dumpBatch(ISymmetricEngine engine, String batchId) throws Exception {
        IDataExtractorService dataExtractorService = engine.getDataExtractorService();
        OutputStreamWriter writer = new OutputStreamWriter(System.out);
        dataExtractorService.extractBatchRange(writer, Long.valueOf(batchId), Long.valueOf(batchId));
        writer.close();
    }

    private void loadBatch(ISymmetricEngine engine, String fileName) throws Exception {
        IDataLoaderService service = engine.getDataLoaderService();
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            FileInputStream in = new FileInputStream(file);
            service.loadDataFromPush("", in, System.out);
            System.out.flush();
            in.close();

        } else {
            throw new SymmetricException("Launcher.Exception.FileNotFound", fileName);
        }
    }

    private void encryptText(ISymmetricEngine engine, String plainText) {
        ISecurityService service = engine.getSecurityService();
        System.out.println(SecurityConstants.PREFIX_ENC + service.encrypt(plainText));
    }

    private void openRegistration(ISymmetricEngine engine, String argument) {
        argument = argument.replace('\"', ' ');
        int index = argument.trim().indexOf(",");
        if (index < 0) {
            throw new SymmetricException("Please provide a file name to write the trigger SQL to",
                    OPTION_OPEN_REGISTRATION);
        }
        String nodeGroupId = argument.substring(0, index).trim();
        String externalId = argument.substring(index + 1).trim();
        IRegistrationService registrationService = engine.getRegistrationService();
        registrationService.openRegistration(nodeGroupId, externalId);
    }

    private String reloadNode(ISymmetricEngine engine, String argument) {
        IDataService dataService = engine.getDataService();
        return dataService.reloadNode(argument);
    }

    private void syncTrigger(ISymmetricEngine engine, String fileName, boolean gen_always)
            throws IOException {
        if (fileName != null) {
            File file = new File(fileName);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            ITriggerRouterService triggerService = engine.getTriggerRouterService();
            StringBuilder sqlBuffer = new StringBuilder();
            triggerService.syncTriggers(sqlBuffer, gen_always);
            FileUtils.writeStringToFile(file, sqlBuffer.toString(), null);
        } else {
            throw new SymmetricException("MissingFilenameTriggerSQL");
        }
    }

    private void generateWar(String warFileName, String propertiesFileName) throws Exception {
        final File workingDirectory = new File("../.war");
        FileUtils.deleteDirectory(workingDirectory);
        FileUtils.copyDirectory(new File("../web"), workingDirectory);
        FileUtils.copyDirectory(new File("../conf"), new File(workingDirectory, "WEB-INF/classes"));
        if (propertiesFileName != null) {
            File propsFile = new File(propertiesFileName);
            if (propsFile.exists()) {
                FileUtils.copyFile(propsFile, new File(workingDirectory,
                        "WEB-INF/classes/symmetric.properties"));
            }
        }
        JarBuilder builder = new JarBuilder(workingDirectory, new File(warFileName),
                new File[] { workingDirectory });
        builder.build();
        FileUtils.deleteDirectory(workingDirectory);
    }

    private void generateDDL(ISymmetricEngine engine, String fileName) throws IOException {
        File file = new File(fileName);
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        FileWriter os = new FileWriter(file, false);
        os.write(engine.getSymmetricDialect().getCreateSymmetricDDL());
        os.close();
    }

    private void generateDefaultProperties(String fileName) throws IOException {
        File file = new File(fileName);
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        BufferedReader is = new BufferedReader(new InputStreamReader(
                SymmetricAdmin.class.getResourceAsStream("/symmetric-default.properties"),
                Charset.defaultCharset()));
        FileWriter os = new FileWriter(file, false);
        String line = is.readLine();
        while (line != null) {
            os.write(line);
            os.write(System.getProperty("line.separator"));
            line = is.readLine();
        }
        is.close();
        os.close();
    }

    private void autoCreateDatabase(ISymmetricEngine engine) {
        engine.setupDatabase(true);
    }

    private void runDdlXml(ISymmetricEngine engine, String fileName) throws FileNotFoundException {
        ISymmetricDialect dialect = engine.getSymmetricDialect();
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            IDatabasePlatform pf = dialect.getPlatform();
            Database db = new DatabaseIO().read(new File(fileName));
            pf.createDatabase(db, false, true);
        } else {
            throw new SymmetricException("Could not find file %s", fileName);
        }
    }

    private void runSql(ISymmetricEngine engine, String fileName) throws FileNotFoundException,
            MalformedURLException {
        ISymmetricDialect dialect = engine.getSymmetricDialect();
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            SqlScript script = new SqlScript(file.toURI().toURL(), dialect.getPlatform()
                    .getSqlTemplate(), true, SqlScript.QUERY_ENDS, dialect.getPlatform()
                    .getSqlScriptReplacementTokens());
            script.execute();
        } else {
            throw new SymmetricException("Could not find file %s", fileName);
        }
    }

}
