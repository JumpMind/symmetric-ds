package org.jumpmind.symmetric.ddl.task;

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
import java.io.FileOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.collections.set.ListOrderedSet;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.dom4j.Document;
import org.dom4j.DocumentFactory;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

/**
 * A simple helper task that dumps information about a database using JDBC.
 * 
 * @version $Revision: 289996 $
 * @ant.task name="dumpMetadata"
 */
public class DumpMetadataTask extends Task
{
    /** Methods that are filtered when enumerating the properties. */
    private static final String[] IGNORED_PROPERTY_METHODS = { "getConnection", "getCatalogs", "getSchemas" };

    /** The data source to use for accessing the database. */
    private BasicDataSource _dataSource;
    /** The file to write the dump to. */
    private File _outputFile = null;
    /** The encoding of the XML output file. */
    private String _outputEncoding = "UTF-8";
    /** The database catalog(s) to read. */
    private String _catalogPattern = "%";
    /** The database schema(s) to read. */
    private String _schemaPattern = "%";
    /** The pattern for reading all tables. */
    private String _tablePattern = "%";
    /** The pattern for reading all procedures. */
    private String _procedurePattern = "%";
    /** The pattern for reading all columns. */
    private String _columnPattern = "%";
    /** The tables types to read; <code>null</code> or an empty list means that we shall read every type. */
    private String[] _tableTypes = null;
    /** Whether to read tables. */
    private boolean _dumpTables = true;
    /** Whether to read procedures. */
    private boolean _dumpProcedures = true;

    /**
     * Adds the data source to use for accessing the database.
     * 
     * @param dataSource The data source
     */
    public void addConfiguredDatabase(BasicDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    /**
     * Specifies the output file to which the database metadata is written to.
     *
     * @param outputFile The output file
     * @ant.required
     */
    public void setOutputFile(File outputFile)
    {
        _outputFile = outputFile;
    }

    /**
     * Specifies the encoding of the output file.
     *
     * @param encoding The encoding
     * @ant.not-required Per default, <code>UTF-8</code> is used.
     */
    public void setOutputEncoding(String encoding)
    {
        _outputEncoding = encoding;
    }

    /**
     * Sets the catalog pattern used when accessing the database.
     *
     * @param catalogPattern The catalog pattern
     * @ant.not-required Per default, no specific catalog is used (value <code>%</code>).
     */
    public void setCatalogPattern(String catalogPattern)
    {
        _catalogPattern = ((catalogPattern == null) || (catalogPattern.length() == 0) ? null : catalogPattern);
    }

    /**
     * Sets the schema pattern used when accessing the database.
     *
     * @param schemaPattern The schema pattern
     * @ant.not-required Per default, no specific schema is used (value <code>%</code>).
     */
    public void setSchemaPattern(String schemaPattern)
    {
        _schemaPattern = ((schemaPattern == null) || (schemaPattern.length() == 0) ? null : schemaPattern);
    }

    /**
     * Specifies the table to be processed. For details see
     * <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/sql/DatabaseMetaData.html#getTables(java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String[])">java.sql.DatabaseMetaData#getTables</a>.
     *
     * @param tablePattern The table pattern
     * @ant.not-required By default, all tables are read (value <code>%</code>).
     */
    public void setTablePattern(String tablePattern)
    {
        _tablePattern = ((tablePattern == null) || (tablePattern.length() == 0) ? null : tablePattern);
    }

    /**
     * Specifies the procedures to be processed. For details and typical table types see
     * <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/sql/DatabaseMetaData.html#getProcedures(java.lang.String,%20java.lang.String,%20java.lang.String)">java.sql.DatabaseMetaData#getProcedures</a>.
     *
     * @param procedurePattern The procedure pattern
     * @ant.not-required By default, all procedures are read (value <code>%</code>).
     */
    public void setProcedurePattern(String procedurePattern)
    {
        _procedurePattern = ((procedurePattern == null) || (procedurePattern.length() == 0) ? null : procedurePattern);
    }

    /**
     * Specifies the columns to be processed. For details and typical table types see
     * <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/sql/DatabaseMetaData.html#getColumns(java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String[])">java.sql.DatabaseMetaData#getColumns</a>.
     *
     * @param columnPattern The column pattern
     * @ant.not-required By default, all columns are read (value <code>%</code>).
     */
    public void setColumnPattern(String columnPattern)
    {
        _columnPattern = ((columnPattern == null) || (columnPattern.length() == 0) ? null : columnPattern);
    }

    /**
     * Specifies the table types to be processed. For details and typical table types see
     * <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/sql/DatabaseMetaData.html#getTables(java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String[])">java.sql.DatabaseMetaData#getTables</a>.
     *
     * @param tableTypes The table types to read
     * @ant.not-required By default, all types of tables are read.
     */
    public void setTableTypes(String tableTypes)
    {
        ArrayList types = new ArrayList();

        if (tableTypes != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(tableTypes, ",");

            while (tokenizer.hasMoreTokens())
            {
                String token = tokenizer.nextToken().trim();

                if (token.length() > 0)
                {
                    types.add(token);
                }
            }
        }
        _tableTypes = (String[])types.toArray(new String[types.size()]);
    }

    /**
     * Specifies whether procedures shall be read from the database.
     *
     * @param readProcedures <code>true</code> if procedures shall be read
     * @ant.not-required By default, procedures are read.
     */
    public void setDumpProcedures(boolean readProcedures)
    {
        _dumpProcedures = readProcedures;
    }

    /**
     * Specifies whether tables shall be read from the database.
     *
     * @param readTables <code>true</code> if tables shall be read
     * @ant.not-required By default, tables are read.
     */
    public void setDumpTables(boolean readTables)
    {
        _dumpTables = readTables;
    }

    /**
     * {@inheritDoc}
     */
    public void execute() throws BuildException
    {
        if (_dataSource == null)
        {
            log("No data source specified, so there is nothing to do.", Project.MSG_INFO);
            return;
        }

        Connection connection = null;
        try
        {
            Document document = DocumentFactory.getInstance().createDocument();
            Element  root     = document.addElement("metadata");

            root.addAttribute("driverClassName", _dataSource.getDriverClassName());
            
            connection = _dataSource.getConnection();
            
            dumpMetaData(root, connection.getMetaData());

            OutputFormat outputFormat = OutputFormat.createPrettyPrint();
            XMLWriter    xmlWriter    = null;

            outputFormat.setEncoding(_outputEncoding);
            if (_outputFile == null)
            {
                xmlWriter = new XMLWriter(System.out, outputFormat);
            }
            else
            {
                xmlWriter = new XMLWriter(new FileOutputStream(_outputFile), outputFormat);
            }
            xmlWriter.write(document);
            xmlWriter.close();
        }
        catch (Exception ex)
        {
            throw new BuildException(ex);
        }
        finally
        {
            if (connection != null)
            {
                try
                {
                    connection.close();
                }
                catch (SQLException ex)
                {}
            }
        }
    }

    /**
     * Dumps the database meta data into XML elements under the given element.
     * 
     * @param element  The XML element
     * @param metaData The meta data
     */
    private void dumpMetaData(Element element, DatabaseMetaData metaData) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, SQLException
    {
        // We rather iterate over the methods because most metadata properties
        // do not follow the bean naming standard
        Method[] methods  = metaData.getClass().getMethods();
        Set      filtered = new HashSet(Arrays.asList(IGNORED_PROPERTY_METHODS));

        for (int idx = 0; idx < methods.length; idx++)
        {
            // only no-arg methods that return something and that are not defined in Object
            // we also filter certain methods
            if ((methods[idx].getParameterTypes().length == 0) && 
                (methods[idx].getReturnType() != null) &&
                (Object.class != methods[idx].getDeclaringClass()) &&
                !filtered.contains(methods[idx].getName()))
            {
                dumpProperty(element, metaData, methods[idx]);
            }
        }
        dumpCatalogsAndSchemas(element, metaData);
        if (_dumpTables)
        {
            dumpTables(element, metaData);
        }
        if (_dumpProcedures)
        {
            dumpProcedures(element, metaData);
        }
    }

    /**
     * Dumps the property represented by the given method.
     * 
     * @param parent     The parent XML element
     * @param obj        The instance we're working on
     * @param propGetter The method for accessing the property
     */
    private void dumpProperty(Element parent, Object obj, Method propGetter)
    {
        try
        {
            addProperty(parent, getPropertyName(propGetter.getName()), propGetter.invoke(obj, null));
        }
        catch (Throwable ex)
        {
            log("Could not dump property "+propGetter.getName()+" because of "+ex.getMessage(), Project.MSG_WARN);
        }
    }

    /**
     * Adds a property to the given element, either as an attribute (primitive value or
     * string) or as a sub element.
     * 
     * @param element The XML element
     * @param name    The name of the property
     * @param value   The value of the property
     */
    private void addProperty(Element element, String name, Object value)
    {
        if (value != null)
        {
            if (value.getClass().isArray())
            {
                addArrayProperty(element, name, (Object[])value);
            }
            else if (value.getClass().isPrimitive() || (value instanceof String))
            {
                element.addAttribute(name, value.toString());
            }
            else if (value instanceof ResultSet)
            {
                addResultSetProperty(element, name, (ResultSet)value);
            }
        }
    }

    /**
     * Adds a property to the given XML element that is represented as an array.
     * 
     * @param element The XML element
     * @param name    The name of the property
     * @param values  The values of the property
     */
    private void addArrayProperty(Element element, String name, Object[] values)
    {
        String propName = name;

        if (propName.endsWith("s"))
        {
            propName = propName.substring(0, propName.length() - 1);
        }

        Element arrayElem = element.addElement(propName + "s");

        for (int idx = 0; idx < values.length; idx++)
        {
            addProperty(arrayElem, "value", values[idx]);
        }
    }
    
    /**
     * Adds a property to the given XML element that is represented as a result set.
     * 
     * @param element The XML element
     * @param name    The name of the property
     * @param result  The values of the property as a result set
     */
    private void addResultSetProperty(Element element, String name, ResultSet result)
    {
        try
        {
            String propName = name;

            if (propName.endsWith("s"))
            {
                propName = propName.substring(0, propName.length() - 1);
            }

            Element           resultSetElem = element.addElement(propName + "s");
            ResultSetMetaData metaData      = result.getMetaData();

            while (result.next())
            {
                Element curRow = resultSetElem.addElement(propName);

                for (int idx = 1; idx <= metaData.getColumnCount(); idx++)
                {
                    Object value = result.getObject(idx);

                    addProperty(curRow, metaData.getColumnLabel(idx), value);
                }
            }
        }
        catch (SQLException ex)
        {
            ex.printStackTrace();
        }
    }

    /**
     * Derives the property name from the given method name.
     * 
     * @param methodName The method name
     * @return The property name
     */
    private String getPropertyName(String methodName)
    {
        if (methodName.startsWith("get"))
        {
            if (Character.isLowerCase(methodName.charAt(4)))
            {
                return Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
            }
            else
            {
                return methodName.substring(3);
            }
        }
        else if (methodName.startsWith("is"))
        {
            if (Character.isLowerCase(methodName.charAt(3)))
            {
                return Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3);
            }
            else
            {
                return methodName.substring(2);
            }
        }
        else
        {
            return methodName;
        }
    }

    /**
     * Dumps the catalogs and schemas of the database.
     * 
     * @param parent   The parent element
     * @param metaData The database meta data
     */
    private void dumpCatalogsAndSchemas(Element parent, DatabaseMetaData metaData) throws SQLException
    {
        // Next we determine and dump the catalogs
        Element   catalogsElem = parent.addElement("catalogs");
        ResultSet result       = metaData.getCatalogs();

        try
        {
            while (result.next())
            {
                String catalogName = getString(result, "TABLE_CAT");
    
                if ((catalogName != null) && (catalogName.length() > 0))
                {
                    Element catalogElem = catalogsElem.addElement("catalog");
    
                    catalogElem.addAttribute("name", catalogName);
                }
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }

        Element schemasElem = parent.addElement("schemas");

        // We also dump the schemas (some dbs only support one of the two)
        result = metaData.getSchemas();

        try
        {
            while (result.next())
            {
                String schemaName = getString(result, "TABLE_SCHEM");
    
                if ((schemaName != null) && (schemaName.length() > 0))
                {
                    Element schemaElem = schemasElem.addElement("schema");
    
                    schemaElem.addAttribute("name", schemaName);
                }
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }
    }

    /**
     * Dumps all tables.
     * 
     * @param parent   The parent element
     * @param metaData The database metadata
     */
    private void dumpTables(Element parent, DatabaseMetaData metaData) throws SQLException
    {
        String[]  tableTypes = _tableTypes;
        ResultSet result     = null;

        if ((tableTypes == null) || (tableTypes.length == 0))
        {
            // First we need the list of supported table types
            ArrayList tableTypeList = new ArrayList();

            result = metaData.getTableTypes();

            try
            {
                while (result.next())
                {
                    tableTypeList.add(getString(result, "TABLE_TYPE"));
                }
            }
            finally
            {
                if (result != null)
                {
                    result.close();
                }
            }
    
            tableTypes = (String[])tableTypeList.toArray(new String[tableTypeList.size()]);
        }

        try
        {
            result = metaData.getTables(_catalogPattern, _schemaPattern, _tablePattern, tableTypes);
        }
        catch (SQLException ex)
        {
            log("Could not determine the tables: "+ex.getMessage(), Project.MSG_ERR);
            return;
        }

        Element tablesElem = parent.addElement("tables");
        Set     columns    = getColumnsInResultSet(result);

        try
        {
            while (result.next())
            {
                String tableName = getString(result, "TABLE_NAME");
    
                if ((tableName == null) || (tableName.length() == 0))
                {
                    continue;
                }

                Element tableElem = tablesElem.addElement("table");
                String  catalog   = getString(result, "TABLE_CAT");
                String  schema    = getString(result, "TABLE_SCHEM");
    
                log("Reading table " + ((schema != null) && (schema.length() > 0) ? schema + "." : "") + tableName, Project.MSG_INFO);

                tableElem.addAttribute("name", tableName);
                if (catalog != null)
                {
                    tableElem.addAttribute("catalog", catalog);
                }
                if (schema != null)
                {
                    tableElem.addAttribute("schema", schema);
                }
                addStringAttribute(result, columns, "TABLE_TYPE", tableElem, "type");
                addStringAttribute(result, columns, "REMARKS", tableElem, "remarks");
                addStringAttribute(result, columns, "TYPE_NAME", tableElem, "typeName");
                addStringAttribute(result, columns, "TYPE_CAT", tableElem, "typeCatalog");
                addStringAttribute(result, columns, "TYPE_SCHEM", tableElem, "typeSchema");
                addStringAttribute(result, columns, "SELF_REFERENCING_COL_NAME", tableElem, "identifierColumn");
                addStringAttribute(result, columns, "REF_GENERATION", tableElem, "identifierGeneration");
    
                dumpColumns(tableElem, metaData, catalog, schema, tableName);
                dumpPKs(tableElem, metaData, catalog, schema, tableName);
                dumpVersionColumns(tableElem, metaData, catalog, schema, tableName);
                dumpFKs(tableElem, metaData, catalog, schema, tableName);
                dumpIndices(tableElem, metaData, catalog, schema, tableName);
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }
    }

    /**
     * Dumps the columns of the indicated table.
     * 
     * @param tableElem   The XML element for the table
     * @param metaData    The database metadata
     * @param catalogName The catalog name
     * @param schemaName  The schema name
     * @param tableName   The table name
     */
    private void dumpColumns(Element tableElem, DatabaseMetaData metaData, String catalogName, String schemaName, String tableName) throws SQLException
    {
        ResultSet result = null;

        try
        {
            result = metaData.getColumns(catalogName, schemaName, tableName, _columnPattern);
        }
        catch (SQLException ex)
        {
            log("Could not determine the columns for table '"+tableName+"': "+ex.getMessage(), Project.MSG_ERR);
            return;
        }

        Set columns = getColumnsInResultSet(result);

        try
        {
            while (result.next())
            {
                String columnName = getString(result, "COLUMN_NAME");
    
                if ((columnName == null) || (columnName.length() == 0))
                {
                    continue;
                }
    
                Element columnElem = tableElem.addElement("column");
    
                columnElem.addAttribute("name", columnName);
                addIntAttribute(result, columns, "DATA_TYPE", columnElem, "typeCode");
                addStringAttribute(result, columns, "TYPE_NAME", columnElem, "type");
                addIntAttribute(result, columns, "COLUMN_SIZE", columnElem, "size");
                addIntAttribute(result, columns, "DECIMAL_DIGITS", columnElem, "digits");
                addIntAttribute(result, columns, "NUM_PREC_RADIX", columnElem, "precision");
                if (columns.contains("NULLABLE"))
                {
                    switch (result.getInt("NULLABLE"))
                    {
                        case DatabaseMetaData.columnNoNulls:
                            columnElem.addAttribute("nullable", "false");
                            break;
                        case DatabaseMetaData.columnNullable:
                            columnElem.addAttribute("nullable", "true");
                            break;
                        default:
                            columnElem.addAttribute("nullable", "unknown");
                            break;
                    }
                }
                addStringAttribute(result, columns, "REMARKS", columnElem, "remarks");
                addStringAttribute(result, columns, "COLUMN_DEF", columnElem, "defaultValue");
                addIntAttribute(result, columns, "CHAR_OCTET_LENGTH", columnElem, "maxByteLength");
                addIntAttribute(result, columns, "ORDINAL_POSITION", columnElem, "index");
                if (columns.contains("IS_NULLABLE"))
                {
                    String value = getString(result, "IS_NULLABLE");
    
                    if ("no".equalsIgnoreCase(value))
                    {
                        columnElem.addAttribute("isNullable", "false");
                    }
                    else if ("yes".equalsIgnoreCase(value))
                    {
                        columnElem.addAttribute("isNullable", "true");
                    }
                    else
                    {
                        columnElem.addAttribute("isNullable", "unknown");
                    }
                }
                addStringAttribute(result, columns, "SCOPE_CATLOG", columnElem, "refCatalog");
                addStringAttribute(result, columns, "SCOPE_SCHEMA", columnElem, "refSchema");
                addStringAttribute(result, columns, "SCOPE_TABLE", columnElem, "refTable");
                addShortAttribute(result, columns, "SOURCE_DATA_TYPE", columnElem, "sourceTypeCode");
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }
    }

    /**
     * Dumps the primary key columns of the indicated table.
     * 
     * @param tableElem   The XML element for the table
     * @param metaData    The database metadata
     * @param catalogName The catalog name
     * @param schemaName  The schema name
     * @param tableName   The table name
     */
    private void dumpPKs(Element tableElem, DatabaseMetaData metaData, String catalogName, String schemaName, String tableName) throws SQLException
    {
        ResultSet result = null;

        try
        {
            result = metaData.getPrimaryKeys(catalogName, schemaName, tableName);
        }
        catch (SQLException ex)
        {
            log("Could not determine the primary key columns for table '"+tableName+"': "+ex.getMessage(), Project.MSG_ERR);
            return;
        }

        Set columns = getColumnsInResultSet(result);

        try
        {
            while (result.next())
            {
                String columnName = getString(result, "COLUMN_NAME");
    
                if ((columnName == null) || (columnName.length() == 0))
                {
                    continue;
                }
    
                Element pkElem = tableElem.addElement("primaryKey");
    
                pkElem.addAttribute("column", columnName);
                addStringAttribute(result, columns, "PK_NAME", pkElem, "name");
                addShortAttribute(result, columns, "KEY_SEQ", pkElem, "sequenceNumberInPK");
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }
    }

    /**
     * Dumps the versioned (auto-updating) columns of the indicated table.
     * 
     * @param tableElem   The XML element for the table
     * @param metaData    The database metadata
     * @param catalogName The catalog name
     * @param schemaName  The schema name
     * @param tableName   The table name
     */
    private void dumpVersionColumns(Element tableElem, DatabaseMetaData metaData, String catalogName, String schemaName, String tableName) throws SQLException
    {
        ResultSet result = null;

        try
        {
            result = metaData.getVersionColumns(catalogName, schemaName, tableName);
        }
        catch (SQLException ex)
        {
            log("Could not determine the versioned columns for table '"+tableName+"': "+ex.getMessage(), Project.MSG_ERR);
            return;
        }

        Set columns = getColumnsInResultSet(result);

        try
        {
            while (result.next())
            {
                String columnName = getString(result, "COLUMN_NAME");
    
                if ((columnName == null) || (columnName.length() == 0))
                {
                    continue;
                }
    
                Element columnElem = tableElem.addElement("versionedColumn");
    
                columnElem.addAttribute("column", columnName);
                addIntAttribute(result, columns, "DATA_TYPE", columnElem, "typeCode");
                addStringAttribute(result, columns, "TYPE_NAME", columnElem, "type");
                addIntAttribute(result, columns, "BUFFER_LENGTH", columnElem, "size");
                addIntAttribute(result, columns, "COLUMN_SIZE", columnElem, "precision");
                addShortAttribute(result, columns, "DECIMAL_DIGITS", columnElem, "scale");
                if (columns.contains("PSEUDO_COLUMN"))
                {
                    switch (result.getShort("PSEUDO_COLUMN"))
                    {
                        case DatabaseMetaData.versionColumnPseudo:
                            columnElem.addAttribute("columnType", "pseudo column");
                            break;
                        case DatabaseMetaData.versionColumnNotPseudo:
                            columnElem.addAttribute("columnType", "real column");
                            break;
                        default:
                            columnElem.addAttribute("columnType", "unknown");
                            break;
                    }
                }
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }
    }

    /**
     * Dumps the foreign key columns of the indicated table to other tables.
     * 
     * @param tableElem   The XML element for the table
     * @param metaData    The database metadata
     * @param catalogName The catalog name
     * @param schemaName  The schema name
     * @param tableName   The table name
     */
    private void dumpFKs(Element tableElem, DatabaseMetaData metaData, String catalogName, String schemaName, String tableName) throws SQLException
    {
        ResultSet result = null;

        try
        {
            result = metaData.getImportedKeys(catalogName, schemaName, tableName);
        }
        catch (SQLException ex)
        {
            log("Could not determine the foreign keys for table '"+tableName+"': "+ex.getMessage(), Project.MSG_ERR);
            return;
        }

        Set columns = getColumnsInResultSet(result);

        try
        {
            while (result.next())
            {
                Element fkElem = tableElem.addElement("foreignKey");
    
                addStringAttribute(result, columns, "FK_NAME", fkElem, "name");
                addStringAttribute(result, columns, "PK_NAME", fkElem, "primaryKeyName");
                addStringAttribute(result, columns, "PKCOLUMN_NAME", fkElem, "column");
                addStringAttribute(result, columns, "FKTABLE_CAT", fkElem, "foreignCatalog");
                addStringAttribute(result, columns, "FKTABLE_SCHEM", fkElem, "foreignSchema");
                addStringAttribute(result, columns, "FKTABLE_NAME", fkElem, "foreignTable");
                addStringAttribute(result, columns, "FKCOLUMN_NAME", fkElem, "foreignColumn");
                addShortAttribute(result, columns, "KEY_SEQ", fkElem, "sequenceNumberInFK");
                if (columns.contains("UPDATE_RULE"))
                {
                    switch (result.getShort("UPDATE_RULE"))
                    {
                        case DatabaseMetaData.importedKeyNoAction:
                            fkElem.addAttribute("updateRule", "no action");
                            break;
                        case DatabaseMetaData.importedKeyCascade:
                            fkElem.addAttribute("updateRule", "cascade PK change");
                            break;
                        case DatabaseMetaData.importedKeySetNull:
                            fkElem.addAttribute("updateRule", "set FK to NULL");
                            break;
                        case DatabaseMetaData.importedKeySetDefault:
                            fkElem.addAttribute("updateRule", "set FK to default");
                            break;
                        default:
                            fkElem.addAttribute("updateRule", "unknown");
                            break;
                    }
                }
                if (columns.contains("DELETE_RULE"))
                {
                    switch (result.getShort("DELETE_RULE"))
                    {
                        case DatabaseMetaData.importedKeyNoAction:
                        case DatabaseMetaData.importedKeyRestrict:
                            fkElem.addAttribute("deleteRule", "no action");
                            break;
                        case DatabaseMetaData.importedKeyCascade:
                            fkElem.addAttribute("deleteRule", "cascade PK change");
                            break;
                        case DatabaseMetaData.importedKeySetNull:
                            fkElem.addAttribute("deleteRule", "set FK to NULL");
                            break;
                        case DatabaseMetaData.importedKeySetDefault:
                            fkElem.addAttribute("deleteRule", "set FK to default");
                            break;
                        default:
                            fkElem.addAttribute("deleteRule", "unknown");
                            break;
                    }
                }
                if (columns.contains("DEFERRABILITY"))
                {
                    switch (result.getShort("DEFERRABILITY"))
                    {
                        case DatabaseMetaData.importedKeyInitiallyDeferred:
                            fkElem.addAttribute("deferrability", "initially deferred");
                            break;
                        case DatabaseMetaData.importedKeyInitiallyImmediate:
                            fkElem.addAttribute("deferrability", "immediately deferred");
                            break;
                        case DatabaseMetaData.importedKeyNotDeferrable:
                            fkElem.addAttribute("deferrability", "not deferred");
                            break;
                        default:
                            fkElem.addAttribute("deferrability", "unknown");
                            break;
                    }
                }
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }
    }

    /**
     * Dumps the indices of the indicated table.
     * 
     * @param tableElem   The XML element for the table
     * @param metaData    The database metadata
     * @param catalogName The catalog name
     * @param schemaName  The schema name
     * @param tableName   The table name
     */
    private void dumpIndices(Element tableElem, DatabaseMetaData metaData, String catalogName, String schemaName, String tableName) throws SQLException
    {
        ResultSet result = null;

        try
        {
            result = metaData.getIndexInfo(catalogName, schemaName, tableName, false, false);
        }
        catch (SQLException ex)
        {
            log("Could not determine the indices for table '"+tableName+"': "+ex.getMessage(), Project.MSG_ERR);
            return;
        }

        Set columns = getColumnsInResultSet(result);

        try
        {
            while (result.next())
            {
                Element indexElem = tableElem.addElement("index");
    
                addStringAttribute(result, columns, "INDEX_NAME", indexElem, "name");
                addBooleanAttribute(result, columns, "NON_UNIQUE", indexElem, "nonUnique");
                addStringAttribute(result, columns, "INDEX_QUALIFIER", indexElem, "indexCatalog");
                if (columns.contains("TYPE"))
                {
                    switch (result.getShort("TYPE"))
                    {
                        case DatabaseMetaData.tableIndexStatistic:
                            indexElem.addAttribute("type", "table statistics");
                            break;
                        case DatabaseMetaData.tableIndexClustered:
                            indexElem.addAttribute("type", "clustered");
                            break;
                        case DatabaseMetaData.tableIndexHashed:
                            indexElem.addAttribute("type", "hashed");
                            break;
                        case DatabaseMetaData.tableIndexOther:
                            indexElem.addAttribute("type", "other");
                            break;
                        default:
                            indexElem.addAttribute("type", "unknown");
                            break;
                    }
                }
                addStringAttribute(result, columns, "COLUMN_NAME", indexElem, "column");
                addShortAttribute(result, columns, "ORDINAL_POSITION", indexElem, "sequenceNumberInIndex");
                if (columns.contains("ASC_OR_DESC"))
                {
                    String value = getString(result, "ASC_OR_DESC");
    
                    if ("A".equalsIgnoreCase(value))
                    {
                        indexElem.addAttribute("sortOrder", "ascending");
                    }
                    else if ("D".equalsIgnoreCase(value))
                    {
                        indexElem.addAttribute("sortOrder", "descending");
                    }
                    else
                    {
                        indexElem.addAttribute("sortOrder", "unknown");
                    }
                }
                addIntAttribute(result, columns, "CARDINALITY", indexElem, "cardinality");
                addIntAttribute(result, columns, "PAGES", indexElem, "pages");
                addStringAttribute(result, columns, "FILTER_CONDITION", indexElem, "filter");
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }
    }

    /**
     * Dumps all procedures.
     * 
     * @param parent   The parent element
     * @param metaData The database metadata
     */
    private void dumpProcedures(Element parent, DatabaseMetaData metaData) throws SQLException
    {
        ResultSet result = null;

        try
        {
            result = metaData.getProcedures(_catalogPattern, _schemaPattern, _procedurePattern);
        }
        catch (SQLException ex)
        {
            log("Could not determine the procedures: "+ex.getMessage(), Project.MSG_ERR);
            return;
        }

        Element proceduresElem = parent.addElement("procedures");
        Set     columns        = getColumnsInResultSet(result);

        try
        {
            while (result.next())
            {
                String procedureName = getString(result, "PROCEDURE_NAME");
    
                if ((procedureName == null) || (procedureName.length() == 0))
                {
                    continue;
                }

                Element procedureElem = proceduresElem.addElement("procedure");
                String  catalog       = getString(result, "PROCEDURE_CAT");
                String  schema        = getString(result, "PROCEDURE_SCHEM");
    
                log("Reading procedure " + ((schema != null) && (schema.length() > 0) ? schema + "." : "") + procedureName, Project.MSG_INFO);

                procedureElem.addAttribute("name", procedureName);
                if (catalog != null)
                {
                    procedureElem.addAttribute("catalog", catalog);
                }
                if (schema != null)
                {
                    procedureElem.addAttribute("schema", schema);
                }
                addStringAttribute(result, columns, "REMARKS", procedureElem, "remarks");
                if (columns.contains("PROCEDURE_TYPE"))
                {
                    switch (result.getShort("PROCEDURE_TYPE"))
                    {
                        case DatabaseMetaData.procedureReturnsResult:
                            procedureElem.addAttribute("type", "returns result");
                            break;
                        case DatabaseMetaData.procedureNoResult:
                            procedureElem.addAttribute("type", "doesn't return result");
                            break;
                        case DatabaseMetaData.procedureResultUnknown:
                            procedureElem.addAttribute("type", "may return result");
                            break;
                        default:
                            procedureElem.addAttribute("type", "unknown");
                            break;
                    }
                }
    
                dumpProcedure(procedureElem, metaData, "%", "%", procedureName);
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }
    }

    /**
     * Dumps the contents of the indicated procedure.
     * 
     * @param procedureElem The XML element for the procedure
     * @param metaData      The database metadata
     * @param catalogName   The catalog name
     * @param schemaName    The schema name
     * @param procedureName The procedure name
     */
    private void dumpProcedure(Element procedureElem, DatabaseMetaData metaData, String catalogName, String schemaName, String procedureName) throws SQLException
    {
        ResultSet result = null;

        try
        {
            result = metaData.getProcedureColumns(catalogName, schemaName, procedureName, _columnPattern);
        }
        catch (SQLException ex)
        {
            log("Could not determine the columns for procedure '"+procedureName+"': "+ex.getMessage(), Project.MSG_ERR);
            return;
        }

        Set columns = getColumnsInResultSet(result);

        try
        {
            while (result.next())
            {
                String columnName = getString(result, "COLUMN_NAME");
    
                if ((columnName == null) || (columnName.length() == 0))
                {
                    continue;
                }
    
                Element columnElem = procedureElem.addElement("column");
    
                columnElem.addAttribute("name", columnName);
                if (columns.contains("COLUMN_TYPE"))
                {
                    switch (result.getShort("COLUMN_TYPE"))
                    {
                        case DatabaseMetaData.procedureColumnIn:
                            columnElem.addAttribute("type", "in parameter");
                            break;
                        case DatabaseMetaData.procedureColumnInOut:
                            columnElem.addAttribute("type", "in/out parameter");
                            break;
                        case DatabaseMetaData.procedureColumnOut:
                            columnElem.addAttribute("type", "out parameter");
                            break;
                        case DatabaseMetaData.procedureColumnReturn:
                            columnElem.addAttribute("type", "return value");
                            break;
                        case DatabaseMetaData.procedureColumnResult:
                            columnElem.addAttribute("type", "result column in ResultSet");
                            break;
                        default:
                            columnElem.addAttribute("type", "unknown");
                            break;
                    }
                }
    
                addIntAttribute(result, columns, "DATA_TYPE", columnElem, "typeCode");
                addStringAttribute(result, columns, "TYPE_NAME", columnElem, "type");
                addIntAttribute(result, columns, "LENGTH", columnElem, "length");
                addIntAttribute(result, columns, "PRECISION", columnElem, "precision");
                addShortAttribute(result, columns, "SCALE", columnElem, "short");
                addShortAttribute(result, columns, "RADIX", columnElem, "radix");
                if (columns.contains("NULLABLE"))
                {
                    switch (result.getInt("NULLABLE"))
                    {
                        case DatabaseMetaData.procedureNoNulls:
                            columnElem.addAttribute("nullable", "false");
                            break;
                        case DatabaseMetaData.procedureNullable:
                            columnElem.addAttribute("nullable", "true");
                            break;
                        default:
                            columnElem.addAttribute("nullable", "unknown");
                            break;
                    }
                }
                addStringAttribute(result, columns, "REMARKS", columnElem, "remarks");
            }
        }
        finally
        {
            if (result != null)
            {
                result.close();
            }
        }
    }

    /**
     * If the result set contains the indicated column, extracts its value and sets an attribute at the given element.
     * 
     * @param result     The result set
     * @param columns    The columns in the result set
     * @param columnName The name of the column in the result set
     * @param element    The element to add the attribute
     * @param attrName   The name of the attribute to set
     * @return The string value or <code>null</code>
     */
    private String addStringAttribute(ResultSet result, Set columns, String columnName, Element element, String attrName) throws SQLException
    {
        String value = null;

        if (columns.contains(columnName))
        {
            value = getString(result, columnName);
            element.addAttribute(attrName, value);
        }
        return value;
    }

    /**
     * If the result set contains the indicated column, extracts its int value and sets an attribute at the given element.
     * 
     * @param result     The result set
     * @param columns    The columns in the result set
     * @param columnName The name of the column in the result set
     * @param element    The element to add the attribute
     * @param attrName   The name of the attribute to set
     * @return The string value or <code>null</code>
     */
    private String addIntAttribute(ResultSet result, Set columns, String columnName, Element element, String attrName) throws SQLException
    {
        String value = null;

        if (columns.contains(columnName))
        {
        	try
        	{
                value = String.valueOf(result.getInt(columnName));
        	}
        	catch (SQLException ex)
        	{
        		// A few databases do not comply with the jdbc spec and return a string (or null),
        		// so lets try this just in case
        		value = result.getString(columnName);

        		if (value != null)
        		{
	        		try
	        		{
	        			Integer.parseInt(value);
	        		}
	        		catch (NumberFormatException parseEx)
	        		{
	        			// its no int returned as a string, so lets re-throw the original exception
	        			throw ex;
	        		}
        		}
        	}
            element.addAttribute(attrName, value);
        }
        return value;
    }

    /**
     * If the result set contains the indicated column, extracts its short value and sets an attribute at the given element.
     * 
     * @param result     The result set
     * @param columns    The columns in the result set
     * @param columnName The name of the column in the result set
     * @param element    The element to add the attribute
     * @param attrName   The name of the attribute to set
     * @return The string value or <code>null</code>
     */
    private String addShortAttribute(ResultSet result, Set columns, String columnName, Element element, String attrName) throws SQLException
    {
        String value = null;

        if (columns.contains(columnName))
        {
        	try
        	{
                value = String.valueOf(result.getShort(columnName));
        	}
        	catch (SQLException ex)
        	{
        		// A few databases do not comply with the jdbc spec and return a string (or null),
        		// so lets try strings this just in case
        		value = result.getString(columnName);

        		if (value != null)
        		{
	        		try
	        		{
	        			Short.parseShort(value);
	        		}
	        		catch (NumberFormatException parseEx)
	        		{
	        			// its no short returned as a string, so lets re-throw the original exception
	        			throw ex;
	        		}
        		}
        	}
            element.addAttribute(attrName, value);
        }
        return value;
    }

    /**
     * If the result set contains the indicated column, extracts its boolean value and sets an attribute at the given element.
     * 
     * @param result     The result set
     * @param columns    The columns in the result set
     * @param columnName The name of the column in the result set
     * @param element    The element to add the attribute
     * @param attrName   The name of the attribute to set
     * @return The string value or <code>null</code>
     */
    private String addBooleanAttribute(ResultSet result, Set columns, String columnName, Element element, String attrName) throws SQLException
    {
        String value = null;

        if (columns.contains(columnName))
        {
            value = String.valueOf(result.getBoolean(columnName));
            element.addAttribute(attrName, value);
        }
        return value;
    }

    /**
     * Extracts a string from the result set.
     * 
     * @param result     The result set
     * @param columnName The name of the column in the result set
     * @return The string value
     */
    private String getString(ResultSet result, String columnName) throws SQLException
    {
        return result.getString(columnName);
    }
    
    /**
     * Determines the columns that are present in the given result set.
     * 
     * @param resultSet The result set
     * @return The columns
     */
    private Set getColumnsInResultSet(ResultSet resultSet) throws SQLException
    {
        ListOrderedSet    result   = new ListOrderedSet();
        ResultSetMetaData metaData = resultSet.getMetaData();

        for (int idx = 1; idx <= metaData.getColumnCount(); idx++)
        {
            result.add(metaData.getColumnName(idx).toUpperCase());
        }
        
        return result;
    }
}
