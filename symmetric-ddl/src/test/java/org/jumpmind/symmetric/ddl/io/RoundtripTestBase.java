package org.jumpmind.symmetric.ddl.io;

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

import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.List;

import junit.framework.TestSuite;

import org.apache.commons.beanutils.DynaBean;
import org.jumpmind.symmetric.ddl.DdlUtilsException;
import org.jumpmind.symmetric.ddl.PlatformFactory;
import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.TestDatabaseWriterBase;
import org.jumpmind.symmetric.ddl.dynabean.SqlDynaBean;
import org.jumpmind.symmetric.ddl.dynabean.SqlDynaClass;
import org.jumpmind.symmetric.ddl.dynabean.SqlDynaProperty;
import org.jumpmind.symmetric.ddl.io.BinaryObjectsHelper;
import org.jumpmind.symmetric.ddl.io.DatabaseIO;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.ForeignKey;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.IndexColumn;
import org.jumpmind.symmetric.ddl.model.Reference;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.model.TypeMap;
import org.jumpmind.symmetric.ddl.platform.DefaultValueHelper;

/**
 * Base class for database roundtrip (creation & reconstruction from the database).
 * 
 * @version $Revision: 289996 $
 */
public abstract class RoundtripTestBase extends TestDatabaseWriterBase
{
    /**
     * Creates the test suite for the given test class which must be a sub class of
     * {@link RoundtripTestBase}. If the platform supports it, it will be tested
     * with both delimited and undelimited identifiers.
     * 
     * @param testedClass The tested class
     * @return The tests
     */
    protected static TestSuite getTests(Class testedClass)
    {
        if (!RoundtripTestBase.class.isAssignableFrom(testedClass) ||
            Modifier.isAbstract(testedClass.getModifiers()))
        {
            throw new DdlUtilsException("Cannot create parameterized tests for class "+testedClass.getName());
        }

        TestSuite suite = new TestSuite();

        try
        {
            Method[]          methods = testedClass.getMethods();
            PlatformInfo      info    = null;
            RoundtripTestBase newTest;
    
            for (int idx = 0; (methods != null) && (idx < methods.length); idx++)
            {
                if (methods[idx].getName().startsWith("test") &&
                    ((methods[idx].getParameterTypes() == null) || (methods[idx].getParameterTypes().length == 0)))
                {
                    newTest = (RoundtripTestBase)testedClass.newInstance();
                    newTest.setName(methods[idx].getName());
                    newTest.setUseDelimitedIdentifiers(false);
                    suite.addTest(newTest);

                    if (info == null)
                    {
                        info = PlatformFactory.createNewPlatformInstance(newTest.getDatabaseName()).getPlatformInfo();
                    }
                    if (info.isDelimitedIdentifiersSupported())
                    {
                        newTest = (RoundtripTestBase)testedClass.newInstance();
                        newTest.setName(methods[idx].getName());
                        newTest.setUseDelimitedIdentifiers(true);
                        suite.addTest(newTest);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            throw new DdlUtilsException(ex);
        }
        
        return suite;
    }

    /** Whether to use delimited identifiers for the test. */
    private boolean _useDelimitedIdentifiers;
    
    /**
     * Specifies whether the test shall use delimited identifiers.
     * 
     * @param useDelimitedIdentifiers Whether to use delimited identifiers
     */
    protected void setUseDelimitedIdentifiers(boolean useDelimitedIdentifiers)
    {
        _useDelimitedIdentifiers = useDelimitedIdentifiers;
    }
    
    /**
     * {@inheritDoc}
     */
    protected void setUp() throws Exception
    {
        super.setUp();
        getPlatform().setDelimitedIdentifierModeOn(_useDelimitedIdentifiers);
    }

    /**
     * Inserts a row into the designated table.
     * 
     * @param tableName    The name of the table (case insensitive)
     * @param columnValues The values for the columns in order of definition
     */
    protected void insertRow(String tableName, Object[] columnValues)
    {
        Table    table = getModel().findTable(tableName);
        DynaBean bean  = getModel().createDynaBeanFor(table);

        for (int idx = 0; (idx < table.getColumnCount()) && (idx < columnValues.length); idx++)
        {
            Column column = table.getColumn(idx);

            bean.set(column.getName(), columnValues[idx]);
        }
        getPlatform().insert(getModel(), bean);
    }

    /**
     * Returns a "SELECT * FROM [table name]" statement. It also takes
     * delimited identifier mode into account if enabled.
     *  
     * @param table       The table
     * @param orderColumn The column to order the rows by (can be <code>null</code>)
     * @return The statement
     */
    protected String getSelectQueryForAllString(Table table, String orderColumn)
    {
        StringBuffer query = new StringBuffer();

        query.append("SELECT * FROM ");
        if (getPlatform().isDelimitedIdentifierModeOn())
        {
            query.append(getPlatformInfo().getDelimiterToken());
        }
        query.append(table.getName());
        if (getPlatform().isDelimitedIdentifierModeOn())
        {
            query.append(getPlatformInfo().getDelimiterToken());
        }
        if (orderColumn != null)
        {
            query.append(" ORDER BY ");
            if (getPlatform().isDelimitedIdentifierModeOn())
            {
                query.append(getPlatformInfo().getDelimiterToken());
            }
            query.append(orderColumn);
            if (getPlatform().isDelimitedIdentifierModeOn())
            {
                query.append(getPlatformInfo().getDelimiterToken());
            }
        }
        return query.toString();
    }

    /**
     * Retrieves all rows from the given table.
     * 
     * @param tableName The table
     * @return The rows
     */
    protected List getRows(String tableName)
    {
        Table table = getModel().findTable(tableName, getPlatform().isDelimitedIdentifierModeOn());
        
        return getPlatform().fetch(getModel(),
                                   getSelectQueryForAllString(table, null),
                                   new Table[] { table });
    }

    /**
     * Retrieves all rows from the given table.
     * 
     * @param tableName   The table
     * @param orderColumn The column to order the rows by
     * @return The rows
     */
    protected List getRows(String tableName, String orderColumn)
    {
        Table table = getModel().findTable(tableName, getPlatform().isDelimitedIdentifierModeOn());
        
        return getPlatform().fetch(getModel(),
                                   getSelectQueryForAllString(table, orderColumn),
                                   new Table[] { table });
    }

    /**
     * Returns the original model adjusted for type changes because of the native type mappings
     * which when read back from the database will map to different types.
     * 
     * @return The adjusted model
     */
    protected Database getAdjustedModel()
    {
        try
        {
            Database model = (Database)getModel().clone();

            for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++)
            {
                Table table = model.getTable(tableIdx);

                for (int columnIdx = 0; columnIdx < table.getColumnCount(); columnIdx++)
                {
                    Column column     = table.getColumn(columnIdx);
                    int    origType   = column.getTypeCode();
                    int    targetType = getPlatformInfo().getTargetJdbcType(origType);

                    // we adjust the column types if the native type would back-map to a
                    // different jdbc type
                    if (targetType != origType)
                    {
                        column.setTypeCode(targetType);
                        // we should also adapt the default value
                        if (column.getDefaultValue() != null)
                        {
                            DefaultValueHelper helper = getPlatform().getSqlBuilder().getDefaultValueHelper();

                            column.setDefaultValue(helper.convert(column.getDefaultValue(), origType, targetType));
                        }
                    }
                    // we also promote the default size if the column has no size
                    // spec of its own
                    if ((column.getSize() == null) && getPlatformInfo().hasSize(targetType))
                    {
                        Integer defaultSize = getPlatformInfo().getDefaultSize(targetType);

                        if (defaultSize != null)
                        {
                            column.setSize(defaultSize.toString());
                        }
                    }
                    // finally the platform might return a synthetic default value if the column
                    // is a primary key column
                    if (getPlatformInfo().isSyntheticDefaultValueForRequiredReturned() &&
                        (column.getDefaultValue() == null) && column.isRequired() && !column.isAutoIncrement())
                    {
                        switch (column.getTypeCode())
                        {
                            case Types.TINYINT:
                            case Types.SMALLINT:
                            case Types.INTEGER:
                            case Types.BIGINT:
                                column.setDefaultValue("0");
                                break;
                            case Types.REAL:
                            case Types.FLOAT:
                            case Types.DOUBLE:
                                column.setDefaultValue("0.0");
                                break;
                            case Types.BIT:
                                column.setDefaultValue("false");
                                break;
                            default:
                                column.setDefaultValue("");
                                break;
                        }
                    }
                }
                // we also add the default names to foreign keys that are initially unnamed
                for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++)
                {
                    ForeignKey fk = table.getForeignKey(fkIdx);

                    if (fk.getName() == null)
                    {
                        fk.setName(getPlatform().getSqlBuilder().getForeignKeyName(table, fk));
                    }
                }
            }
            return model;
        }
        catch (CloneNotSupportedException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Compares the specified attribute value of the given bean with the expected object.
     * 
     * @param expected The expected object
     * @param bean     The bean
     * @param attrName The attribute name
     */
    protected void assertEquals(Object expected, Object bean, String attrName)
    {
        DynaBean dynaBean = (DynaBean)bean;
        Object   value    = dynaBean.get(attrName);

        if ((value instanceof byte[]) && !(expected instanceof byte[]) && (dynaBean instanceof SqlDynaBean))
        {
            SqlDynaClass dynaClass = (SqlDynaClass)((SqlDynaBean)dynaBean).getDynaClass();
            Column       column    = ((SqlDynaProperty)dynaClass.getDynaProperty(attrName)).getColumn();

            if (TypeMap.isBinaryType(column.getTypeCode()))
            {
                value = new BinaryObjectsHelper().deserialize((byte[])value);
            }
        }
        if (expected == null)
        {
            assertNull(value);
        }
        else
        {
            assertEquals(expected, value);
        }
    }

    /**
     * Asserts that the two given database models are equal, and if not, writes both of them
     * in XML form to <code>stderr</code>.
     * 
     * @param expected The expected model
     * @param actual   The actual model
     */
    protected void assertEquals(Database expected, Database actual)
    {
        try
        {
            assertEquals("Model names do not match.",
                         expected.getName(),
                         actual.getName());
            assertEquals("Not the same number of tables.",
                         expected.getTableCount(),
                         actual.getTableCount());
            for (int tableIdx = 0; tableIdx < actual.getTableCount(); tableIdx++)
            {
                assertEquals(expected.getTable(tableIdx),
                             actual.getTable(tableIdx));
            }
        }
        catch (Throwable ex)
        {
            StringWriter writer = new StringWriter();
            DatabaseIO   dbIo   = new DatabaseIO();

            dbIo.write(expected, writer);

            getLog().error("Expected model:\n" + writer.toString());

            writer = new StringWriter();
            dbIo.write(actual, writer);

            getLog().error("Actual model:\n" + writer.toString());

            if (ex instanceof Error)
            {
                throw (Error)ex;
            }
            else
            {
                throw new DdlUtilsException(ex);
            }
        }
    }

    /**
     * Asserts that the two given database tables are equal.
     * 
     * @param expected The expected table
     * @param actual   The actual table
     */
    protected void assertEquals(Table expected, Table actual)
    {
        if (_useDelimitedIdentifiers)
        {
            assertEquals("Table names do not match.",
                         getPlatform().getSqlBuilder().shortenName(expected.getName(), getSqlBuilder().getMaxTableNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName(), getSqlBuilder().getMaxTableNameLength()));
        }
        else
        {
            assertEquals("Table names do not match (ignoring case).",
                         getPlatform().getSqlBuilder().shortenName(expected.getName().toUpperCase(), getSqlBuilder().getMaxTableNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName().toUpperCase(), getSqlBuilder().getMaxTableNameLength()));
        }
        assertEquals("Not the same number of columns in table "+actual.getName()+".",
                     expected.getColumnCount(),
                     actual.getColumnCount());
        for (int columnIdx = 0; columnIdx < actual.getColumnCount(); columnIdx++)
        {
            assertEquals(expected.getColumn(columnIdx),
                         actual.getColumn(columnIdx));
        }
        assertEquals("Not the same number of foreign keys in table "+actual.getName()+".",
                     expected.getForeignKeyCount(),
                     actual.getForeignKeyCount());
        // order is not assumed with the way foreignkeys are returned.
        for (int expectedFkIdx = 0; expectedFkIdx < expected.getForeignKeyCount(); expectedFkIdx++)
        {
            ForeignKey expectedFk   = expected.getForeignKey(expectedFkIdx);
            String     expectedName = getPlatform().getSqlBuilder().shortenName(expectedFk.getName(), getSqlBuilder().getMaxForeignKeyNameLength());

            for (int actualFkIdx = 0; actualFkIdx < actual.getForeignKeyCount(); actualFkIdx++)
            {
                ForeignKey actualFk   = actual.getForeignKey(actualFkIdx);
                String     actualName = getPlatform().getSqlBuilder().shortenName(actualFk.getName(), getSqlBuilder().getMaxForeignKeyNameLength());

                if ((_useDelimitedIdentifiers  && expectedName.equals(actualName)) ||
                    (!_useDelimitedIdentifiers && expectedName.equalsIgnoreCase(actualName)))
                {
                    assertEquals(expectedFk, actualFk);
                }
            }
        }
        assertEquals("Not the same number of indices in table "+actual.getName()+".",
                     expected.getIndexCount(),
                     actual.getIndexCount());
        for (int indexIdx = 0; indexIdx < actual.getIndexCount(); indexIdx++)
        {
            assertEquals(expected.getIndex(indexIdx),
                         actual.getIndex(indexIdx));
        }
    }

    /**
     * Asserts that the two given columns are equal.
     * 
     * @param expected The expected column
     * @param actual   The actual column
     */
    protected void assertEquals(Column expected, Column actual)
    {
        if (_useDelimitedIdentifiers)
        {
            assertEquals("Column names do not match.",
                         getPlatform().getSqlBuilder().shortenName(expected.getName(), getSqlBuilder().getMaxColumnNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName(), getSqlBuilder().getMaxColumnNameLength()));
        }
        else
        {
            assertEquals("Column names do not match (ignoring case).",
                         getPlatform().getSqlBuilder().shortenName(expected.getName().toUpperCase(), getSqlBuilder().getMaxColumnNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName().toUpperCase(), getSqlBuilder().getMaxColumnNameLength()));
        }
        assertEquals("Primary key status not the same for column "+actual.getName()+".",
                     expected.isPrimaryKey(),
                     actual.isPrimaryKey());
        assertEquals("Required status not the same for column "+actual.getName()+".",
                     expected.isRequired(),
                     actual.isRequired());
        if (getPlatformInfo().getIdentityStatusReadingSupported())
        {
        	// we're only comparing this if the platform can actually read the
        	// auto-increment status back from an existing database
	        assertEquals("Auto-increment status not the same for column "+actual.getName()+".",
	                     expected.isAutoIncrement(),
	                     actual.isAutoIncrement());
        }
        assertEquals("Type not the same for column "+actual.getName()+".",
                     expected.getType(),
                     actual.getType());
        assertEquals("Type code not the same for column "+actual.getName()+".",
                     expected.getTypeCode(),
                     actual.getTypeCode());
        assertEquals("Parsed default values do not match for column "+actual.getName()+".",
                     expected.getParsedDefaultValue(),
                     actual.getParsedDefaultValue());

        // comparing the size makes only sense for types where it is relevant
        if ((expected.getTypeCode() == Types.NUMERIC) ||
            (expected.getTypeCode() == Types.DECIMAL))
        {
            assertEquals("Precision not the same for column "+actual.getName()+".",
                         expected.getSizeAsInt(),
                         actual.getSizeAsInt());
            assertEquals("Scale not the same for column "+actual.getName()+".",
                         expected.getScale(),
                         actual.getScale());
        }
        else if ((expected.getTypeCode() == Types.CHAR) ||
                 (expected.getTypeCode() == Types.VARCHAR) ||
                 (expected.getTypeCode() == Types.BINARY) ||
                 (expected.getTypeCode() == Types.VARBINARY))
        {
            assertEquals("Size not the same for column "+actual.getName()+".",
                         expected.getSize(),
                         actual.getSize());
        }
    }

    /**
     * Asserts that the two given foreign keys are equal.
     * 
     * @param expected The expected foreign key
     * @param actual   The actual foreign key
     */
    protected void assertEquals(ForeignKey expected, ForeignKey actual)
    {
        if (_useDelimitedIdentifiers)
        {
            assertEquals("Foreign key names do not match.",
                         getPlatform().getSqlBuilder().shortenName(expected.getName(), getSqlBuilder().getMaxForeignKeyNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName(), getSqlBuilder().getMaxForeignKeyNameLength()));
            assertEquals("Referenced table names do not match.",
                         getPlatform().getSqlBuilder().shortenName(expected.getForeignTableName(), getSqlBuilder().getMaxTableNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getForeignTableName(), getSqlBuilder().getMaxTableNameLength()));
        }
        else
        {
            assertEquals("Foreign key names do not match (ignoring case).",
                         getPlatform().getSqlBuilder().shortenName(expected.getName().toUpperCase(), getSqlBuilder().getMaxForeignKeyNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName().toUpperCase(), getSqlBuilder().getMaxForeignKeyNameLength()));
            assertEquals("Referenced table names do not match (ignoring case).",
                         getPlatform().getSqlBuilder().shortenName(expected.getForeignTableName().toUpperCase(), getSqlBuilder().getMaxTableNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getForeignTableName().toUpperCase(), getSqlBuilder().getMaxTableNameLength()));
        }
        assertEquals("Not the same number of references in foreign key "+actual.getName()+".",
                     expected.getReferenceCount(),
                     actual.getReferenceCount());
        for (int refIdx = 0; refIdx < actual.getReferenceCount(); refIdx++)
        {
            assertEquals(expected.getReference(refIdx),
                         actual.getReference(refIdx));
        }
    }

    /**
     * Asserts that the two given references are equal.
     * 
     * @param expected The expected reference
     * @param actual   The actual reference
     */
    protected void assertEquals(Reference expected, Reference actual)
    {
        if (_useDelimitedIdentifiers)
        {
            assertEquals("Local column names do not match.",
                         getPlatform().getSqlBuilder().shortenName(expected.getLocalColumnName(), getSqlBuilder().getMaxColumnNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getLocalColumnName(), getSqlBuilder().getMaxColumnNameLength()));
            assertEquals("Foreign column names do not match.",
                         getPlatform().getSqlBuilder().shortenName(expected.getForeignColumnName(), getSqlBuilder().getMaxColumnNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getForeignColumnName(), getSqlBuilder().getMaxColumnNameLength()));
        }
        else
        {
            assertEquals("Local column names do not match (ignoring case).",
                         getPlatform().getSqlBuilder().shortenName(expected.getLocalColumnName().toUpperCase(), getSqlBuilder().getMaxColumnNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getLocalColumnName().toUpperCase(), getSqlBuilder().getMaxColumnNameLength()));
            assertEquals("Foreign column names do not match (ignoring case).",
                         getPlatform().getSqlBuilder().shortenName(expected.getForeignColumnName().toUpperCase(), getSqlBuilder().getMaxColumnNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getForeignColumnName().toUpperCase(), getSqlBuilder().getMaxColumnNameLength()));
        }
    }

    /**
     * Asserts that the two given indices are equal.
     * 
     * @param expected The expected index
     * @param actual   The actual index
     */
    protected void assertEquals(Index expected, Index actual)
    {
        if (_useDelimitedIdentifiers)
        {
            assertEquals("Index names do not match.",
                         getPlatform().getSqlBuilder().shortenName(expected.getName(), getSqlBuilder().getMaxConstraintNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName(), getSqlBuilder().getMaxConstraintNameLength()));
        }
        else
        {
            assertEquals("Index names do not match (ignoring case).",
                         getPlatform().getSqlBuilder().shortenName(expected.getName().toUpperCase(), getSqlBuilder().getMaxConstraintNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName().toUpperCase(), getSqlBuilder().getMaxConstraintNameLength()));
        }
        assertEquals("Unique status not the same for index "+actual.getName()+".",
                     expected.isUnique(),
                     actual.isUnique());
        assertEquals("Not the same number of columns in index "+actual.getName()+".",
                     expected.getColumnCount(),
                     actual.getColumnCount());
        for (int columnIdx = 0; columnIdx < actual.getColumnCount(); columnIdx++)
        {
            assertEquals(expected.getColumn(columnIdx),
                         actual.getColumn(columnIdx));
        }
    }

    /**
     * Asserts that the two given index columns are equal.
     * 
     * @param expected The expected index column
     * @param actual   The actual index column
     */
    protected void assertEquals(IndexColumn expected, IndexColumn actual)
    {
        if (_useDelimitedIdentifiers)
        {
            assertEquals("Index column names do not match.",
                         getPlatform().getSqlBuilder().shortenName(expected.getName(), getSqlBuilder().getMaxColumnNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName(), getSqlBuilder().getMaxColumnNameLength()));
        }
        else
        {
            assertEquals("Index column names do not match (ignoring case).",
                         getPlatform().getSqlBuilder().shortenName(expected.getName().toUpperCase(), getSqlBuilder().getMaxColumnNameLength()),
                         getPlatform().getSqlBuilder().shortenName(actual.getName().toUpperCase(), getSqlBuilder().getMaxColumnNameLength()));
        }
        assertEquals("Size not the same for index column "+actual.getName()+".",
                     expected.getSize(),
                     actual.getSize());
    }
}
