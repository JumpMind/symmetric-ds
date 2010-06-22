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

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.digester.Rule;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;
import org.xml.sax.Attributes;

/**
 * A digester rule for creating dyna beans.
 * 
 * @version $Revision: 289996 $
 */
public class DynaSqlCreateRule extends Rule
{
    /** The database model for which we'l be creating beans. */
    private Database _model;
    /** The table that we're creating instances for. */
    private Table    _table;
    /** The object that will receive the read beans. */
    private DataSink _receiver;

    /**
     * Creates a new creation rule that creates dyna bean instances.
     * 
     * @param model    The database model that we're operating on
     * @param table    The table that we're creating instances for
     * @param receiver The object that will receive the read beans
     */
    public DynaSqlCreateRule(Database model, Table table, DataSink receiver)
    {
        _model    = model;
        _table    = table;
        _receiver = receiver;
    }

    /**
     * {@inheritDoc}
     */
    public void begin(String namespace, String name, Attributes attributes) throws Exception
    {
        Object instance = _model.createDynaBeanFor(_table);

        if (digester.getLogger().isDebugEnabled())
        {
            digester.getLogger().debug("[DynaSqlCreateRule]{" + digester.getMatch() + "} New dyna bean '" + _table.getName() + "' created");
        }
        digester.push(instance);
    }

    /**
     * {@inheritDoc}
     */
    public void end(String namespace, String name) throws Exception
    {
        DynaBean top = (DynaBean)digester.pop();

        if (digester.getLogger().isDebugEnabled())
        {
            digester.getLogger().debug("[DynaSqlCreateRule]{" + digester.getMatch() + "} Pop " + top.getDynaClass().getName());
        }
        _receiver.addBean(top);
    }
}
