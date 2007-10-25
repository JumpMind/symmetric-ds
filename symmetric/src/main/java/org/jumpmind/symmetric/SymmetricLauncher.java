/*
 * SymmetricDS is an open source database synchronization solution.
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * This class is used to run Symmetric utilities and launch an embedded version of Symmetric.
 */
public class SymmetricLauncher {

    private static final String OPTION_PROPERTIES_GEN = "properties-gen";
    private static final String OPTION_PROPERTIES_FILE = "properties-file";

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            
            if (line.hasOption(OPTION_PROPERTIES_GEN)) {
                generateDefaultProperties(line.getOptionValue(OPTION_PROPERTIES_GEN));
                return;
            }

            String propertiesFileArg = "";
            
            // validate that block-size has been set
            if (line.hasOption(OPTION_PROPERTIES_FILE)) {
                propertiesFileArg = "file:" + line.getOptionValue(OPTION_PROPERTIES_FILE);
                if (!new File(line.getOptionValue(OPTION_PROPERTIES_FILE)).exists()) {
                    throw new ParseException("Could not find the properties file specified: " + propertiesFileArg);
                }                
            } 
            
            SymmetricEngine engine = new SymmetricEngine(propertiesFileArg, "");
            
        } catch (ParseException exp) {
            System.err.println(exp.getMessage());
            printHelp(options);
        } catch(Exception ex) {
            System.err.println(ExceptionUtils.getRootCause(ex).getMessage());
        }
    }
    
    private static void printHelp(Options options) {
        new HelpFormatter().printHelp("symmetricds", options);
    }
    
    private static Options buildOptions() {
        Options options = new Options();
        options
                .addOption("d", "ddl-gen", true,
                        "Output the DDL to create the SymmetricDS tables.  Takes an argument of the name of the file to write the ddl to.");
        options.addOption("p", OPTION_PROPERTIES_FILE, true,
                "Takes an argument with the path to the properties file that will drive SymmetricDS.  If this is not provided, SymmetricDS will use defaults, then override with the first symmetric.properties in your classpath, then override with symmetric.properties values in your user.home directory.");
        options.addOption("P", OPTION_PROPERTIES_GEN, true,
        "Takes an argument with the path to a file which all the default overrideable properties will be written.");
        
        return options;
    }
    
    private static void generateDefaultProperties(String fileName) throws IOException {
        File file = new File(fileName);
        file.getParentFile().mkdirs();
        ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] {"classpath:symmetric-properties.xml"});
        Properties properties = (Properties)ctx.getBean("properties");
        FileOutputStream os = new FileOutputStream(file, false);
        properties.store(os, "Auto generated SymmetricDS properties file.");
        os.close();
    }

}
