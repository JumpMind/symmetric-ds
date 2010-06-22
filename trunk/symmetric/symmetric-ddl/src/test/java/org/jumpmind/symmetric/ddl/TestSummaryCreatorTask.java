package org.jumpmind.symmetric.ddl;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.jumpmind.symmetric.ddl.PlatformUtils;
import org.xml.sax.InputSource;

/**
 * Creates a test summary snippet that can be put onto the DdlUtils web site.
 * 
 * @version $Revision: $
 */
public class TestSummaryCreatorTask extends Task
{
    /** The DdlUtils version. */
    private String _version;
    /** The file to write the snippet to. */
    private File _outputFile;
    /** The input files. */
    private ArrayList _fileSets = new ArrayList();

    /**
     * Set the DdlUtils version used to run the tests.
     *
     * @param version The version
     */
    public void setVersion(String version)
    {
        _version = version;
    }

    /**
     * Set the output file.
     *
     * @param outputFile The output file
     */
    public void setOutputFile(File outputFile)
    {
        _outputFile = outputFile;
    }

    /**
     * Adds a fileset.
     * 
     * @param fileset The additional input files
     */
    public void addConfiguredFileset(FileSet fileset)
    {
        _fileSets.add(fileset);
    }

    /**
     * Returns the list of input files ({@link java.io.File} objects) to process.
     * Note that this does not check that the file is a valid and useful XML file.
     * 
     * @return The input files
     */
    private List getInputFiles()
    {
    	ArrayList result = new ArrayList();

        for (Iterator it = _fileSets.iterator(); it.hasNext();)
        {
            FileSet          fileSet    = (FileSet)it.next();
            File             fileSetDir = fileSet.getDir(getProject());
            DirectoryScanner scanner    = fileSet.getDirectoryScanner(getProject());
            String[]         files      = scanner.getIncludedFiles();

            for (int idx = 0; (files != null) && (idx < files.length); idx++)
            {
            	File file = new File(fileSetDir, files[idx]);

                if (file.isFile() && file.canRead())
                {
                	result.add(file);
                }
            }
        }
        return result;
    }

    /**
     * Processes all input files and gathers the relevant data.
     * 
     * @return The Dom4j document object containing the summary
     */
    private Document processInputFiles() throws IOException
    {
    	Document summaryDoc = DocumentHelper.createDocument();

    	summaryDoc.addElement("summary");

    	for (Iterator it = getInputFiles().iterator(); it.hasNext();)
    	{
    		processInputFile(summaryDoc, (File)it.next());
    	}
    	return summaryDoc;
    }

    /**
     * Returns the value of the specified attribute of the node determined by the given xpath.
     * 
     * @param doc      The XML document
     * @param xpath    The xpath selecting the node whose attribute we want
     * @param attrName The name of the attribute
     * @return The attribute value
     */
    private String getAttrValue(Document doc, String xpath, String attrName)
    {
    	Node node = doc.selectSingleNode(xpath);

    	if (node instanceof Attribute)
    	{
    		// we ignore the attribute name then
    		return ((Attribute)node).getValue();
    	}
    	else if (node != null)
    	{
    		return node.valueOf("@" + attrName);
    	}
        else
        {
            return null;
        }
    }
    
    /**
     * Processes the given input file.
     * 
     * @param summaryDoc The document object to add data to
     * @param inputFile  The input file
     */
    private void processInputFile(Document summaryDoc, File inputFile) throws IOException
    {
    	try
    	{
        	// First we check whether it is an XML file
    		SAXReader reader  = new SAXReader();
            Document  testDoc = reader.read(new InputSource(new FileReader(inputFile)));

        	// Then we check whether it is one that we're interested in
            if (testDoc.selectSingleNode("/testsuite") == null)
            {
            	return;
            }

            // Ok, it is, so lets extract the data that we want:
            Element generalElement   = (Element)summaryDoc.selectSingleNode("//general");
            int     totalNumTests    = 0;
            int     totalNumErrors   = 0;
            int     totalNumFailures = 0;

            if (generalElement == null)
            {
            	generalElement = summaryDoc.getRootElement().addElement("general");

            	// General run info (we only need this once)
                generalElement.addAttribute("ddlUtilsVersion",
                                            _version);
            	generalElement.addAttribute("date",
                                            getAttrValue(testDoc, "//property[@name='TODAY']", "value"));
            	generalElement.addAttribute("lang",
                                            getAttrValue(testDoc, "//property[@name='env.LANG']", "value"));
            	generalElement.addAttribute("jre",
                                            getAttrValue(testDoc, "//property[@name='java.runtime.name']", "value") + " " +
                                            getAttrValue(testDoc, "//property[@name='java.runtime.version']", "value") + " (" +
                                            getAttrValue(testDoc, "//property[@name='java.vendor']", "value") + ")");
            	generalElement.addAttribute("os",
                                            getAttrValue(testDoc, "//property[@name='os.name']", "value") + " " +
                                            getAttrValue(testDoc, "//property[@name='os.version']", "value") + ", arch " +
                                            getAttrValue(testDoc, "//property[@name='os.arch']", "value"));
            	addTargetDatabaseInfo(generalElement,
                                      getAttrValue(testDoc, "//property[@name='jdbc.properties.file']", "value"));
            }
            else
            {
                totalNumTests    = Integer.parseInt(generalElement.attributeValue("tests"));
                totalNumErrors   = Integer.parseInt(generalElement.attributeValue("errors"));
                totalNumFailures = Integer.parseInt(generalElement.attributeValue("failures"));
            }

            int numTests    = Integer.parseInt(getAttrValue(testDoc, "/testsuite", "tests"));
            int numErrors   = Integer.parseInt(getAttrValue(testDoc, "/testsuite", "errors"));
            int numFailures = Integer.parseInt(getAttrValue(testDoc, "/testsuite", "failures"));

            totalNumTests    += numTests;
            totalNumErrors   += numErrors;
            totalNumFailures += numFailures;

            generalElement.addAttribute("tests", String.valueOf(totalNumTests));
            generalElement.addAttribute("errors", String.valueOf(totalNumErrors));
            generalElement.addAttribute("failures", String.valueOf(totalNumErrors));

            if ((numErrors > 0) || (numFailures > 0))
            {
                Element testSuiteNode = (Element)testDoc.selectSingleNode("/testsuite");
                String  testSuiteName = testSuiteNode.attributeValue("name");

                // since tests have failed, we add it to the summary
                for (Iterator it = testSuiteNode.selectNodes("testcase[failure or error]").iterator(); it.hasNext();)
                {
                    Element failedTestElement = (Element)it.next();
                    Element newTestElement    = summaryDoc.getRootElement().addElement("failedTest");

                    // Test setup failure, so the test was not actually run ?
                    if (!failedTestElement.attributeValue("classname").startsWith("junit.framework.TestSuite"))
                    {
                        newTestElement.addAttribute("name", failedTestElement.attributeValue("classname") + "#" + failedTestElement.attributeValue("name"));
                    }
                    newTestElement.addAttribute("testsuite", testSuiteName);
                }
            }
    	}
    	catch (DocumentException ex)
    	{
    		// No, apparently it's not an XML document, so we ignore it
    	}
    }

    /**
     * Reads the database properties from the used properties file.
     * 
     * @param element            The element to add the relevant database properties to
     * @param jdbcPropertiesFile The path of the properties file
     */
    protected void addTargetDatabaseInfo(Element element, String jdbcPropertiesFile) throws IOException, BuildException
    {
        if (jdbcPropertiesFile == null)
        {
            return;
        }

        Properties       props    = new Properties();
        Connection       conn     = null;
        DatabaseMetaData metaData = null;

        try
        {
            props.load(getClass().getResourceAsStream(jdbcPropertiesFile));
        }
        catch (Exception ex)
        {
            // not on the classpath ? let's try a file
            File baseDir  = getProject().getBaseDir();
            File propFile = new File(baseDir, jdbcPropertiesFile);

            if (propFile.exists() && propFile.isFile() && propFile.canRead())
            {
                props.load(new FileInputStream(propFile));
            }
            else
            {
                throw new BuildException("Cannot load database properties from file " + jdbcPropertiesFile);
            }
        }

        try
        {
            String     dataSourceClass = props.getProperty(TestDatabaseWriterBase.DATASOURCE_PROPERTY_PREFIX + "class", BasicDataSource.class.getName());
            DataSource dataSource      = (DataSource)Class.forName(dataSourceClass).newInstance();
    
            for (Iterator it = props.entrySet().iterator(); it.hasNext();)
            {
                Map.Entry entry    = (Map.Entry)it.next();
                String    propName = (String)entry.getKey();
    
                if (propName.startsWith(TestDatabaseWriterBase.DATASOURCE_PROPERTY_PREFIX) && !propName.equals(TestDatabaseWriterBase.DATASOURCE_PROPERTY_PREFIX +"class"))
                {
                    BeanUtils.setProperty(dataSource,
                                          propName.substring(TestDatabaseWriterBase.DATASOURCE_PROPERTY_PREFIX.length()),
                                          entry.getValue());
                }
            }

            String platformName = props.getProperty(TestDatabaseWriterBase.DDLUTILS_PLATFORM_PROPERTY);

            if (platformName == null)
            {
                platformName = new PlatformUtils().determineDatabaseType(dataSource);
                if (platformName == null)
                {
                    throw new BuildException("Could not determine platform from datasource, please specify it in the jdbc.properties via the ddlutils.platform property");
                }
            }

            element.addAttribute("platform", platformName);
            element.addAttribute("dataSourceClass", dataSourceClass);

            conn     = dataSource.getConnection();
            metaData = conn.getMetaData();

            try
            {
                element.addAttribute("dbProductName", metaData.getDatabaseProductName());
            }
            catch (Throwable ex)
            {
                // we ignore it
            }
            try
            {
                element.addAttribute("dbProductVersion", metaData.getDatabaseProductVersion());
            }
            catch (Throwable ex)
            {
                // we ignore it
            }
            try
            {
                int databaseMajorVersion = metaData.getDatabaseMajorVersion();
                int databaseMinorVersion = metaData.getDatabaseMinorVersion();

                element.addAttribute("dbVersion", databaseMajorVersion + "." + databaseMinorVersion);
            }
            catch (Throwable ex)
            {
                // we ignore it
            }
            try
            {
                element.addAttribute("driverName", metaData.getDriverName());
            }
            catch (Throwable ex)
            {
                // we ignore it
            }
            try
            {
                element.addAttribute("driverVersion", metaData.getDriverVersion());
            }
            catch (Throwable ex)
            {
                // we ignore it
            }
            try
            {
                int jdbcMajorVersion = metaData.getJDBCMajorVersion();
                int jdbcMinorVersion = metaData.getJDBCMinorVersion();

                element.addAttribute("jdbcVersion", jdbcMajorVersion + "." + jdbcMinorVersion);
            }
            catch (Throwable ex)
            {
                // we ignore it
            }
        }
        catch (Exception ex)
        {
            throw new BuildException(ex);
        }
        finally
        {
            if (conn != null)
            {
                try
                {
                    conn.close();
                }
                catch (SQLException ex)
                {
                    // we ignore it
                }
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    public void execute() throws BuildException
    {
        try
        {
            log("Processing test results", Project.MSG_INFO);

            Document  doc    = processInputFiles();
            XMLWriter writer = null;

            if (_outputFile != null)
            {
                writer = new XMLWriter(new FileWriter(_outputFile), OutputFormat.createPrettyPrint());
            }
            else
            {
                writer = new XMLWriter(System.out, OutputFormat.createPrettyPrint());
            }

            writer.write(doc);
            writer.close();

            log("Processing finished", Project.MSG_INFO);
        }
        catch (Exception ex)
        {
            throw new BuildException("Error while processing the test results: "+ex.getLocalizedMessage(), ex);
        }
    }
}
