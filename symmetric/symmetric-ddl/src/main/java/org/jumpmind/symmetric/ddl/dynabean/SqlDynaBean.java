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

import org.apache.commons.beanutils.BasicDynaBean;
import org.apache.commons.beanutils.DynaClass;
import org.apache.commons.beanutils.DynaProperty;

/**
 * SqlDynaBean is a DynaBean which can be persisted as a single row in 
 * a Database Table.
 *
 * @version $Revision: 463757 $
 */
public class SqlDynaBean extends BasicDynaBean
{
    /** Unique ID for serializaion purposes. */
    private static final long serialVersionUID = -6946514447446174227L;

    /**
     * Creates a new dyna bean of the given class.
     * 
     * @param dynaClass The dyna class
     */
    public SqlDynaBean(DynaClass dynaClass)
    {
        super(dynaClass);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        StringBuffer   result = new StringBuffer();
        DynaClass      type   = getDynaClass();
        DynaProperty[] props  = type.getDynaProperties();

        result.append(type.getName());
        result.append(": ");
        for (int idx = 0; idx < props.length; idx++)
        {
            if (idx > 0)
            {
                result.append(", ");
            }
            result.append(props[idx].getName());
            result.append(" = ");
            result.append(get(props[idx].getName()));
        }
        return result.toString();
    }
}
