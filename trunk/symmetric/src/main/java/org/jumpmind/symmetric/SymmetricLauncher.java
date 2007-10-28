/*
 * symmetric is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.service.IBootstrapService;

/**
 * Run symmetric utilities and/or launch an embedded version of Symmetric.  If you run this
 * program without any arguments 'help' will print out.
 */
public class SymmetricLauncher {

    private static final String OPTION_AUTO_CREATE = "auto-create";

    private static final String OPTION_PORT_SERVER = "port";

    private static final String OPTION_DDL_GEN = "generate-config-dll";

    private static final String OPTION_RUN_DDL_XML = "run-ddl-xml";

    private static final String OPTION_PROPERTIES_GEN = "generate-default-properties";

    private static final String OPTION_PROPERTIES_FILE = "properties";

    private static final String OPTION_START_SERVER = "server";

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();
        try {
            CommandLine line = parser.parse(options, args);

            int serverPort = 31415;

            if (line.hasOption(OPTION_PORT_SERVER)) {
                serverPort = new Integer(line.getOptionValue(OPTION_PORT_SERVER));
            }

            if (line.hasOption(OPTION_PROPERTIES_GEN)) {
                generateDefaultProperties(line.getOptionValue(OPTION_PROPERTIES_GEN));
                return;
            }

            // validate that block-size has been set
            if (line.hasOption(OPTION_PROPERTIES_FILE)) {
                System.setProperty("symmetric.override.properties.file.1", "file:"
                        + line.getOptionValue(OPTION_PROPERTIES_FILE));
                if (!new File(line.getOptionValue(OPTION_PROPERTIES_FILE)).exists()) {
                    throw new ParseException("Could not find the properties file specified: "
                            + line.getOptionValue(OPTION_PROPERTIES_FILE));
                }
            }

            if (line.hasOption(OPTION_DDL_GEN)) {
                generateDDL(new SymmetricEngine(), line.getOptionValue(OPTION_DDL_GEN));
                return;
            }
            
            if (line.hasOption(OPTION_AUTO_CREATE)) {
                autoCreateDatabase(new SymmetricEngine());
                return;
            }

            if (line.hasOption(OPTION_RUN_DDL_XML)) {
                runDdlXml(new SymmetricEngine(), line.getOptionValue(OPTION_RUN_DDL_XML));
                return;
            }

            if (line.hasOption(OPTION_START_SERVER)) {
                new SymmetricWebServer().start(serverPort);
                return;
            }

            printHelp(options);

        } catch (ParseException exp) {
            System.err.println(exp.getMessage());
            printHelp(options);
        } catch (Exception ex) {
            Throwable root = ExceptionUtils.getRootCause(ex);
            System.err.println(root == null ? ex.getMessage() : root.getMessage());
            printHelp(options);
        }
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp("sym", options);
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("S", OPTION_START_SERVER, false, "Start an embedded instance of symmetric.");
        options.addOption("P", OPTION_PORT_SERVER, true,
                "Optionally pass in the HTTP port number to use for the server instance.");
        options
                .addOption("c", OPTION_DDL_GEN, true,
                        "Output the DDL to create the symmetric tables.  Takes an argument of the name of the file to write the ddl to.");
        options
                .addOption(
                        "p",
                        OPTION_PROPERTIES_FILE,
                        true,
                        "Takes an argument with the path to the properties file that will drive symmetric.  If this is not provided, symmetric will use defaults, then override with the first symmetric.properties in your classpath, then override with symmetric.properties values in your user.home directory.");
        options
                .addOption("g", OPTION_PROPERTIES_GEN, true,
                        "Takes an argument with the path to a file which all the default overrideable properties will be written.");
        options
                .addOption("r", OPTION_RUN_DDL_XML, true,
                        "Takes an argument of a DdlUtils xml file and applies it to the database configured in your symmetric properties file.");
        options
        .addOption("a", OPTION_AUTO_CREATE, false,
                "Attempts to create the symmetric tables in the configured database.");
        return options;
    }

    private static void generateDDL(SymmetricEngine engine, String fileName) throws IOException {
        File file = new File(fileName);
        file.getParentFile().mkdirs();
        FileWriter os = new FileWriter(file, false);
        os.write(((IDbDialect) engine.getApplicationContext().getBean(Constants.DB_DIALECT)).getCreateSymmetricDDL());
        os.close();
    }

    private static void generateDefaultProperties(String fileName) throws IOException {
        File file = new File(fileName);
        file.getParentFile().mkdirs();
        BufferedReader is = new BufferedReader(new InputStreamReader(SymmetricLauncher.class
                .getResourceAsStream("/symmetric-default.properties"), Charset.defaultCharset()));
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
    
    private static void autoCreateDatabase(SymmetricEngine engine) {
        IBootstrapService bootstrapService = (IBootstrapService)engine.getApplicationContext().getBean(Constants.BOOTSTRAP_SERVICE);
        bootstrapService.setAutoConfigureDatabase(true);
        bootstrapService.init();
    }

    private static void runDdlXml(SymmetricEngine engine, String fileName) throws FileNotFoundException {
        IDbDialect dialect = (IDbDialect) engine.getApplicationContext().getBean(Constants.DB_DIALECT);
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
        Platform pf = dialect.getPlatform();
        Database db = new DatabaseIO().read(new File(fileName));
        pf.createTables(db, true, true);
        } else {
            throw new FileNotFoundException("Could not find " + fileName);
        }
    }

}
