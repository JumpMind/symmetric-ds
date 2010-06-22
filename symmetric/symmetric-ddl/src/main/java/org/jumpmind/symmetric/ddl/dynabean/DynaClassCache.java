package org.jumpmind.symmetric.ddl.dynabean;

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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.DynaClass;
import org.jumpmind.symmetric.ddl.model.Table;

/**
 * Provides a cache of dyna class instances for a specific model, as well as
 * helper methods for dealing with these classes.
 *
 * @version $Revision: 231110 $
 */
public class DynaClassCache
{
    /** A cache of the SqlDynaClasses per table name. */
    private Map _dynaClassCache = new HashMap();

    /**
     * Creates a new dyna bean instance for the given table.
     * 
     * @param table The table
     * @return The new empty dyna bean
     */
    public DynaBean createNewInstance(Table table) throws SqlDynaException
    {
        try
        {
            return getDynaClass(table).newInstance();
        }
        catch (InstantiationException ex)
        {
            throw new SqlDynaException("Could not create a new dyna bean for table "+table.getName(), ex);
        }
        catch (IllegalAccessException ex)
        {
            throw new SqlDynaException("Could not create a new dyna bean for table "+table.getName(), ex);
        }
    }

    /**
     * Creates a new dyna bean instance for the given table and copies the values from the
     * given source object. The source object can be a bean, a map or a dyna bean.
     * This method is useful when iterating through an arbitrary dyna bean
     * result set after performing a query, then creating a copy as a DynaBean
     * which is bound to a specific table.
     * This new DynaBean can be kept around, changed and stored back into the database.
     *
     * @param table  The table to create the dyna bean for
     * @param source Either a bean, a {@link java.util.Map} or a dyna bean that will be used
     *               to populate the resultint dyna bean
     * @return A new dyna bean bound to the given table and containing all the properties from
     *         the source object
     */
    public DynaBean copy(Table table, Object source) throws SqlDynaException
    {
        DynaBean answer = createNewInstance(table);

        try
        {
            // copy all the properties from the source
            BeanUtils.copyProperties(answer, source);
        }
        catch (InvocationTargetException ex)
        {
            throw new SqlDynaException("Could not populate the bean", ex);
        }
        catch (IllegalAccessException ex)
        {
            throw new SqlDynaException("Could not populate the bean", ex);
        }

        return answer;
    }

    /**
     * Returns the {@link SqlDynaClass} for the given table. If the it does not
     * exist yet, a new one will be created based on the Table definition.
     * 
     * @param table The table
     * @return The <code>SqlDynaClass</code> for the indicated table
     */
    public SqlDynaClass getDynaClass(Table table)
    {
        SqlDynaClass answer = (SqlDynaClass)_dynaClassCache.get(table.getName());

        if (answer == null)
        {
            answer = createDynaClass(table);
            _dynaClassCache.put(table.getName(), answer);
        }
        return answer;
    }

    /**
     * Returns the {@link SqlDynaClass} for the given bean.
     * 
     * @param dynaBean The bean
     * @return The dyna bean class
     */
    public SqlDynaClass getDynaClass(DynaBean dynaBean) throws SqlDynaException
    {
        DynaClass dynaClass = dynaBean.getDynaClass();

        if (dynaClass instanceof SqlDynaClass)
        {
            return (SqlDynaClass)dynaClass;
        }
        else
        {
            // TODO: we could autogenerate an SqlDynaClass here ?
            throw new SqlDynaException("The dyna bean is not an instance of a SqlDynaClass");
        }
    }

    /**
     * Creates a new {@link SqlDynaClass} instance for the given table based on the table definition.
     * 
     * @param table The table
     * @return The new dyna class
     */
    private SqlDynaClass createDynaClass(Table table)
    {
        return SqlDynaClass.newInstance(table);
    }
}
