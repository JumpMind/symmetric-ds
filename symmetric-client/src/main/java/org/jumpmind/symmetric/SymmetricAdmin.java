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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.h2.util.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.JarBuilder;

/**
 * Perform administration tasks with SymmetricDS.
 */
public class SymmetricAdmin extends AbstractCommandLauncher {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(SymmetricAdmin.class);

    private static final String CMD_RELOAD_NODE = "reload-node";

    private static final String CMD_RELOAD_TABLE = "reload-table";

    private static final String CMD_EXPORT_BATCH = "export-batch";

    private static final String CMD_IMPORT_BATCH = "import-batch";

    private static final String CMD_RUN_PURGE = "run-purge";

    private static final String CMD_ENCRYPT_TEXT = "encrypt-text";
    
    private static final String CMD_OBFUSCATE_TEXT = "obfuscate-text";

    private static final String CMD_CREATE_WAR = "create-war";

    private static final String CMD_CREATE_SYM_TABLES = "create-sym-tables";

    private static final String CMD_EXPORT_SYM_TABLES = "export-sym-tables";

    private static final String CMD_OPEN_REGISTRATION = "open-registration";

    private static final String CMD_SYNC_TRIGGERS = "sync-triggers";
    
    private static final String CMD_DROP_TRIGGERS = "drop-triggers";

    private static final String CMD_EXPORT_PROPERTIES = "export-properties";
    
    private static final String CMD_UNINSTALL = "uninstall";

    private static final String CMD_SEND_SQL = "send-sql";

    private static final String CMD_SEND_SCRIPT = "send-script";

    private static final String CMD_SEND_SCHEMA = "send-schema";

    private static final String[] NO_ENGINE_REQUIRED = { CMD_EXPORT_PROPERTIES };

    private static final String OPTION_NODE = "node";

    private static final String OPTION_CATALOG = "catalog";

    private static final String OPTION_SCHEMA = "schema";

    private static final String OPTION_WHERE = "where";

    private static final String OPTION_FORCE = "force";
    
    private static final String OPTION_OUT = "out";

    private static final String OPTION_NODE_GROUP = "node-group";

    private static final String OPTION_REVERSE = "reverse";

    private static final int WIDTH = 80;

    private static final int PAD = 3;

    public SymmetricAdmin(String app, String argSyntax, String messageKeyPrefix) {
        super(app, argSyntax, messageKeyPrefix);
    }

    public static void main(String[] args) throws Exception {
        new SymmetricAdmin("symadmin", "<subcommand> [options] [args]", "SymAdmin.Option.")
                .execute(args);
    }

    @Override
    protected boolean printHelpIfNoOptionsAreProvided() {
        return true;
    }

    @Override
    protected boolean requiresPropertiesFile() {
        return true;
    }

    @Override
    protected void printHelp(CommandLine line, Options options) {
        String[] args = line.getArgs();
        if (args.length > 1 && args[0].equals(HELP)) {
            printHelpCommand(line);
        } else {
            System.out.println(app + " version " + Version.version());
            System.out.println("Perform administration tasks with SymmetricDS.\n");
            System.out
                    .println("Usage: symadmin <subcommand> --engine [engine.name] [options] [args]");
            System.out
                    .println("       symadmin <subcommand> --properties [properties file] [options] [args]");
            System.out
                    .println("\nType 'symadmin help <subcommand>' for help on a specific subcommand.\n");
            System.out.println("Available subcommands:");
            PrintWriter pw = new PrintWriter(System.out);
            printHelpLine(pw, CMD_OPEN_REGISTRATION);
            printHelpLine(pw, CMD_RELOAD_NODE);
            printHelpLine(pw, CMD_RELOAD_TABLE);
            printHelpLine(pw, CMD_EXPORT_BATCH);
            printHelpLine(pw, CMD_IMPORT_BATCH);
            printHelpLine(pw, CMD_RUN_PURGE);
            printHelpLine(pw, CMD_ENCRYPT_TEXT);
            printHelpLine(pw, CMD_OBFUSCATE_TEXT);
            printHelpLine(pw, CMD_CREATE_WAR);
            printHelpLine(pw, CMD_CREATE_SYM_TABLES);
            printHelpLine(pw, CMD_EXPORT_SYM_TABLES);
            printHelpLine(pw, CMD_SYNC_TRIGGERS);
            printHelpLine(pw, CMD_DROP_TRIGGERS);
            printHelpLine(pw, CMD_EXPORT_PROPERTIES);
            printHelpLine(pw, CMD_SEND_SQL);
            printHelpLine(pw, CMD_SEND_SCHEMA);
            printHelpLine(pw, CMD_SEND_SCRIPT);
            printHelpLine(pw, CMD_UNINSTALL);
            pw.flush();
        }
    }

    private void printHelpLine(PrintWriter pw, String cmd) {
        String text = StringUtils.pad("   " + cmd, 23, " ", true)
                + Message.get("SymAdmin.Cmd." + cmd);
        new HelpFormatter().printWrapped(pw, 79, 25, text);
    }
    
    private void printHelpCommand(CommandLine line) {
        String[] args = line.getArgs();
        if (args.length > 1) {
            String cmd = args[1];
            HelpFormatter format = new HelpFormatter();
            PrintWriter writer = new PrintWriter(System.out);
            Options options = new Options();

            if (! Message.containsKey("SymAdmin.Usage." + cmd)) {
            	System.err.println("ERROR: no help text for subcommand '" + cmd + "' was found.");
            	System.err.println("For a list of subcommands, use " + app + " --" + HELP + "\n");
            	return;
            }
            format.printWrapped(writer, WIDTH,
                    "Usage: " + app + " " + cmd + " " + Message.get("SymAdmin.Usage." + cmd) + "\n");
            format.printWrapped(writer, WIDTH, Message.get("SymAdmin.Help." + cmd));

            if (cmd.equals(CMD_SEND_SQL) || cmd.equals(CMD_SEND_SCHEMA)
                    || cmd.equals(CMD_RELOAD_TABLE) || cmd.equals(CMD_SEND_SCRIPT)) {
                addOption(options, "n", OPTION_NODE, true);
                addOption(options, "g", OPTION_NODE_GROUP, true);
            }
            if (cmd.equals(CMD_RELOAD_TABLE)) {
                addOption(options, "c", OPTION_CATALOG, true);
                addOption(options, "s", OPTION_SCHEMA, true);
                addOption(options, "w", OPTION_WHERE, true);
            }
            if (cmd.equals(CMD_SYNC_TRIGGERS)) {
                addOption(options, "o", OPTION_OUT, false);
                addOption(options, "f", OPTION_FORCE, false);
            }
            if (cmd.equals(CMD_RELOAD_NODE)) {
                addOption(options, "r", OPTION_REVERSE, false);
            }

            if (options.getOptions().size() > 0) {
                format.printWrapped(writer, WIDTH, "\nOptions:");
                format.printOptions(writer, WIDTH, options, PAD, PAD);
            }

            if (!ArrayUtils.contains(NO_ENGINE_REQUIRED, cmd)) {
                format.printWrapped(writer, WIDTH, "\nEngine options:");
                options = new Options();
                super.buildOptions(options);
                format.printOptions(writer, WIDTH, options, PAD, PAD);

                format.printWrapped(writer, WIDTH, "\nCrypto options:");
                options = new Options();
                buildCryptoOptions(options);
                format.printOptions(writer, WIDTH, options, PAD, PAD);
            }
            writer.flush();
        }
    }

    

    @Override
    protected void buildOptions(Options options) {
        super.buildOptions(options);
        addOption(options, "n", OPTION_NODE, true);
        addOption(options, "g", OPTION_NODE_GROUP, true);
        addOption(options, "c", OPTION_CATALOG, true);
        addOption(options, "s", OPTION_SCHEMA, true);
        addOption(options, "w", OPTION_WHERE, true);
        addOption(options, "f", OPTION_FORCE, false);
        addOption(options, "o", OPTION_OUT, true);
        addOption(options, "r", OPTION_REVERSE, false);
        buildCryptoOptions(options);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean executeWithOptions(CommandLine line) throws Exception {
        List<String> args = line.getArgList();
        String cmd = args.remove(0);
        configureCrypto(line);

        if (cmd.equals(CMD_EXPORT_PROPERTIES)) {
            exportDefaultProperties(line, args);
            return true;
        } else if (cmd.equals(CMD_CREATE_WAR)) {
            generateWar(line, args);
            return true;
        } else if (cmd.equals(CMD_EXPORT_SYM_TABLES)) {
            exportSymTables(line, args);
            return true;
        } else if (cmd.equals(CMD_RUN_PURGE)) {
            runPurge(line, args);
            return true;
        } else if (cmd.equals(CMD_OPEN_REGISTRATION)) {
            openRegistration(line, args);
            return true;
        } else if (cmd.equals(CMD_RELOAD_NODE)) {
            reloadNode(line, args);
            return true;
        } else if (cmd.equals(CMD_EXPORT_BATCH)) {
            exportBatch(line, args);
            return true;
        } else if (cmd.equals(CMD_SYNC_TRIGGERS)) {
            syncTrigger(line, args);
            return true;
        } else if (cmd.equals(CMD_DROP_TRIGGERS)) {
            dropTrigger(line, args);
            return true;
        } else if (cmd.equals(CMD_CREATE_SYM_TABLES)) {
            createSymTables();
            return true;
        } else if (cmd.equals(CMD_IMPORT_BATCH)) {
            importBatch(line, args);
            return true;
        } else if (cmd.equals(CMD_ENCRYPT_TEXT)) {
            encryptText(line, args);
            return true;
        } else if (cmd.equals(CMD_OBFUSCATE_TEXT)) {
            obfuscateText(line, args);
            return true;
        } else if (cmd.equals(CMD_SEND_SQL)) {
            sendSql(line, args);
            return true;
        } else if (cmd.equals(CMD_UNINSTALL)) {
            uninstall(line, args);
            return true;
        } else if (cmd.equals(CMD_RELOAD_TABLE)) {
            reloadTable(line, args);
            return true;
        } else if (cmd.equals(CMD_SEND_SCHEMA)) {
            sendSchema(line, args);
            return true;
        } else if (cmd.equals(CMD_SEND_SCRIPT)) {
            sendScript(line, args);
            return true;
        } else {
        	System.err.println("ERROR: no subcommand '" + cmd + "' was found.");
        	System.err.println("For a list of subcommands, use " + app + " --" + HELP + "\n");
        }

        return false;
    }

    private String popArg(List<String> args, String argName) {
        if (args.size() == 0) {
            System.out.println("ERROR: Expected argument for: " + argName);
            System.exit(1);
        }
        return args.remove(0);
    }

    private void runPurge(CommandLine line, List<String> args) {
        IPurgeService purgeService = getSymmetricEngine().getPurgeService();
        boolean all = args.contains("all") || args.size() == 0;
        if (args.contains("outgoing") || all) {
            purgeService.purgeOutgoing(true);
        }
        if (args.contains("incoming") || all) {
            purgeService.purgeIncoming(true);
        }
    }

    private void exportBatch(CommandLine line, List<String> args) throws Exception {
        IDataExtractorService dataExtractorService = getSymmetricEngine().getDataExtractorService();
        String nodeId = popArg(args, "Node ID");
        String batchId = popArg(args, "Batch ID");
        OutputStreamWriter writer = getWriter(args);
        dataExtractorService.extractBatchRange(writer, nodeId, Long.valueOf(batchId),
                Long.valueOf(batchId));
        writer.close();
    }

    private void importBatch(CommandLine line, List<String> args) throws Exception {
        IDataLoaderService service = getSymmetricEngine().getDataLoaderService();
        InputStream in = null;
        if (args.size() == 0) {
            in = System.in;
        } else {
            in = new FileInputStream(args.get(0));
        }
        service.loadDataFromPush(getSymmetricEngine().getNodeService().findIdentity(), in,
                System.out);
        System.out.flush();
        in.close();
    }

    private void encryptText(CommandLine line, List<String> args) {
        String plainText = popArg(args, "Text");
        ISecurityService service = getSymmetricEngine().getSecurityService();
        System.out.println(SecurityConstants.PREFIX_ENC + service.encrypt(plainText));
    }

    private void obfuscateText(CommandLine line, List<String> args) {
        String plainText = popArg(args, "Text");
        ISecurityService service = getSymmetricEngine().getSecurityService();
        System.out.println(SecurityConstants.PREFIX_OBF + service.obfuscate(plainText));
    }

    private void openRegistration(CommandLine line, List<String> args) {
        String nodeGroupId = popArg(args, "Node Group ID");
        String externalId = popArg(args, "External ID");

        IRegistrationService registrationService = getSymmetricEngine().getRegistrationService();
        registrationService.openRegistration(nodeGroupId, externalId);

        System.out.println(String.format(
                "Opened registration for node group of '%s' external ID of '%s'", nodeGroupId,
                externalId));
    }

    private void reloadNode(CommandLine line, List<String> args) {
        String nodeId = popArg(args, "Node ID");
        boolean reverse = line.hasOption(OPTION_REVERSE);
        IDataService dataService = getSymmetricEngine().getDataService();
        String message = dataService.reloadNode(nodeId, reverse, "symadmin");
        System.out.println(message);
    }

    private void syncTrigger(CommandLine line, List<String> args) throws IOException {
        boolean genAlways = line.hasOption(OPTION_FORCE);
        String filename = line.getOptionValue(OPTION_OUT);
        String catalogName = line.getOptionValue(OPTION_CATALOG);
        String schemaName = line.getOptionValue(OPTION_SCHEMA);
        File file = null;
        if (filename != null) {
            file = new File(filename);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
        }
        ITriggerRouterService triggerService = getSymmetricEngine().getTriggerRouterService();
        StringBuilder sqlBuffer = new StringBuilder();
        if (args.size() == 0) {
            triggerService.syncTriggers(sqlBuffer, genAlways);
        } else {
            for (String tablename : args) {
                Table table = platform.getTableFromCache(catalogName, schemaName, tablename, true);
                if (table != null) {
                    triggerService.syncTriggers(table, genAlways);
                } else {
                    System.out.println("Unable to find table " + tablename);
                }
            }
        }
        if (file != null) {
            FileUtils.writeStringToFile(file, sqlBuffer.toString());
        }
    }

    private void dropTrigger(CommandLine line, List<String> args) throws IOException {
        ITriggerRouterService triggerService = getSymmetricEngine().getTriggerRouterService();
        if (args.size() == 0) {
            System.out.println("Dropping all triggers...");
            triggerService.dropTriggers();
        } else {
            for (String tablename : args) {
                System.out.println("Dropping trigger for table " + tablename);
                Set<String> tables = new HashSet<String>();
                tables.add(tablename);
                triggerService.dropTriggers(tables);
            }
        }
    }

    private void generateWar(CommandLine line, List<String> args) throws Exception {
        String warFileName = popArg(args, "Filename");
        final File workingDirectory = new File(AppUtils.getSymHome() + "/.war");
        FileUtils.deleteDirectory(workingDirectory);
        FileUtils.copyDirectory(new File(AppUtils.getSymHome() + "/web"), workingDirectory);
        FileUtils.copyDirectory(new File(AppUtils.getSymHome() + "/conf"), new File(workingDirectory, "WEB-INF/classes"));
        if (propertiesFile != null && propertiesFile.exists()) {
            FileUtils.copyFile(propertiesFile, new File(workingDirectory,
                    "WEB-INF/classes/symmetric.properties"));
        }
        JarBuilder builder = new JarBuilder(workingDirectory, new File(warFileName),
                new File[] { workingDirectory }, Version.version());
        builder.build();
        FileUtils.deleteDirectory(workingDirectory);
    }

    private void exportSymTables(CommandLine line, List<String> args) throws IOException {
        OutputStreamWriter os = getWriter(args);
        os.write(getSymmetricEngine().getSymmetricDialect().getCreateSymmetricDDL());
        os.close();
    }

    private void exportDefaultProperties(CommandLine line, List<String> args) throws IOException {
        OutputStreamWriter os = getWriter(args);
        BufferedReader is = new BufferedReader(new InputStreamReader(
                SymmetricAdmin.class.getResourceAsStream("/symmetric-default.properties"),
                Charset.defaultCharset()));
        String str = is.readLine();
        while (str != null) {
            os.write(str);
            os.write(System.getProperty("line.separator"));
            str = is.readLine();
        }
        is.close();
        os.close();
    }

    private void createSymTables() {
        getSymmetricEngine().setupDatabase(true);
    }
    
    private void uninstall(CommandLine line, List<String> args) {
        getSymmetricEngine().uninstall();
    }

    private void sendSql(CommandLine line, List<String> args) {
        String tableName = popArg(args, "Table Name");
        String sql = popArg(args, "SQL");
        String catalogName = line.getOptionValue(OPTION_CATALOG);
        String schemaName = line.getOptionValue(OPTION_SCHEMA);
        for (Node node : getNodes(line)) {
            System.out.println("Sending SQL to node '" + node.getNodeId() + "'");
            getSymmetricEngine().getDataService().sendSQL(node.getNodeId(), catalogName,
                    schemaName, tableName, sql);
        }
    }

    private void reloadTable(CommandLine line, List<String> args) {
        String catalogName = line.getOptionValue(OPTION_CATALOG);
        String schemaName = line.getOptionValue(OPTION_SCHEMA);

        if (args.size() == 0) {
            System.out.println("ERROR: Expected argument for: Table Name");
            System.exit(1);
        }
        for (String tableName : args) {
            for (Node node : getNodes(line)) {
                System.out.println("Reloading table '" + tableName + "' to node '" + node.getNodeId() + "'");
    
                if (line.hasOption(OPTION_WHERE)) {
                    System.out.println(getSymmetricEngine().getDataService().reloadTable(node.getNodeId(), catalogName,
                            schemaName, tableName, line.getOptionValue(OPTION_WHERE)));
                } else {
                    System.out.println(getSymmetricEngine().getDataService().reloadTable(node.getNodeId(), catalogName,
                            schemaName, tableName));
                }
            }
        }
    }

    private void sendSchema(CommandLine line, List<String> args) {
        String tableName = popArg(args, "Table Name");
        String catalog = line.getOptionValue(OPTION_CATALOG);
        String schema = line.getOptionValue(OPTION_SCHEMA);
        Collection<Node> nodes = getNodes(line);
        for (Node node : nodes) {
            getSymmetricEngine().getDataService().sendSchema(node.getNodeId(), catalog, schema,
                    tableName, false);
        }
    }

    private void sendScript(CommandLine line, List<String> args) throws Exception {
        String scriptName = popArg(args, "Script Name");
        String scriptData = FileUtils.readFileToString(new File(scriptName));
        for (Node node : getNodes(line)) {
            System.out.println("Sending script to node '" + node.getNodeId() + "'");
            getSymmetricEngine().getDataService().sendScript(node.getNodeId(), scriptData, false);
        }
    }

    private Collection<Node> getNodes(CommandLine line) {
        if (line.hasOption(OPTION_NODE_GROUP)) {
            return getSymmetricEngine().getNodeService().findEnabledNodesFromNodeGroup(
                    line.getOptionValue(OPTION_NODE_GROUP));
        } else if (line.hasOption(OPTION_NODE)) {
            Collection<Node> nodes = new ArrayList<Node>();
            String nodeId = line.getOptionValue(OPTION_NODE);
            Node node = getSymmetricEngine().getNodeService().findNode(nodeId);
            if (node == null) {
                System.out.println("ERROR: Unable to find node '" + nodeId + "'");
                System.exit(1);
            }
            nodes.add(node);
            return nodes;
        } else {
            System.out.println("ERROR: Must specify one option: " + OPTION_NODE + ", "
                    + OPTION_NODE_GROUP);
            System.exit(1);
        }
        return null;
    }

    private OutputStreamWriter getWriter(List<String> args) throws IOException {
        OutputStreamWriter os = null;
        if (args.size() == 0) {
            os = new OutputStreamWriter(System.out);
        } else {
            File file = new File(args.get(0));
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            os = new FileWriter(file);
        }
        return os;
    }
}
