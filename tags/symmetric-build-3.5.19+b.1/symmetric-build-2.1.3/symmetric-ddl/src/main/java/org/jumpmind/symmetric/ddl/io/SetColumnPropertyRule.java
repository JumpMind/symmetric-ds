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

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.digester.Rule;
import org.jumpmind.symmetric.ddl.io.converters.SqlTypeConverter;
import org.jumpmind.symmetric.ddl.model.Column;
import org.xml.sax.Attributes;

/**
 * A digester rule for setting a bean property that corresponds to a column. 
 * 
 * @version $Revision: 289996 $
 */
public class SetColumnPropertyRule extends Rule
{
    /** The column that this rule shall set. */
    private Column _column;
    /** The converter for generating the property value from a string. */
    private SqlTypeConverter _converter;
    /** Whether to be case sensitive or not when comparing the attribute name. */
    private boolean _caseSensitive;

    /**
     * Creates a new creation rule that sets the property corresponding to the given column.
     * 
     * @param column          The column that this rule shall set
     * @param converter       The converter to be used for this column
     * @param beCaseSensitive Whether the rule shall compare the attribute names case sensitively
     */
    public SetColumnPropertyRule(Column column, SqlTypeConverter converter, boolean beCaseSensitive)
    {
        _column        = column;
        _converter     = converter;
        _caseSensitive = beCaseSensitive;
    }

    /**
     * {@inheritDoc}
     */
    public void begin(Attributes attributes) throws Exception
    {
        Object bean = digester.peek();

        for (int idx = 0; idx < attributes.getLength(); idx++)
        {
            String attrName = attributes.getLocalName(idx);

            if ("".equals(attrName))
            {
                attrName = attributes.getQName(idx);
            }
            if ((_caseSensitive  && attrName.equals(_column.getName())) ||
                (!_caseSensitive && attrName.equalsIgnoreCase(_column.getName())))
            {
                String attrValue = attributes.getValue(idx);
                Object propValue = (_converter != null ? _converter.convertFromString(attrValue, _column.getTypeCode()) : attrValue);

                if (digester.getLogger().isDebugEnabled())
                {
                    digester.getLogger().debug("[SetColumnPropertyRule]{" + digester.getMatch() +
                                               "} Setting property '" + _column.getName() + "' to '" + propValue + "'");
                }

                PropertyUtils.setProperty(bean, _column.getName(), propValue);
            }
        }
    }
}
