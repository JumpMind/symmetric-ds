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
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.digester.Rule;
import org.jumpmind.symmetric.ddl.io.converters.SqlTypeConverter;
import org.jumpmind.symmetric.ddl.model.Column;
import org.xml.sax.Attributes;

/**
 * A digester rule for setting a bean property that corresponds to a column
 * with the value derived from a sub element. 
 * 
 * @version $Revision: 289996 $
 */
public class SetColumnPropertyFromSubElementRule extends Rule
{
    /** The column that this rule shall set. */
    private Column _column;
    /** The converter for generating the property value from a string. */
    private SqlTypeConverter _converter;
    /** Whether the element's content uses Base64. */
    private boolean _usesBase64 = false;

    /**
     * Creates a new creation rule that sets the property corresponding to the given column.
     * 
     * @param column    The column that this rule shall set
     * @param converter The converter to be used for this column
     */
    public SetColumnPropertyFromSubElementRule(Column column, SqlTypeConverter converter)
    {
        _column    = column;
        _converter = converter;
    }

    /**
     * {@inheritDoc}
     */
    public void begin(Attributes attributes) throws Exception
    {
        for (int idx = 0; idx < attributes.getLength(); idx++)
        {
            String attrName = attributes.getLocalName(idx);

            if ("".equals(attrName))
            {
                attrName = attributes.getQName(idx);
            }
            if (DatabaseIO.BASE64_ATTR_NAME.equals(attrName) &&
                "true".equalsIgnoreCase(attributes.getValue(idx)))
            {
                _usesBase64 = true;
                break;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void end() throws Exception
    {
        _usesBase64 = false;
    }

    /**
     * {@inheritDoc}
     */
    public void body(String text) throws Exception
    {
        String attrValue = text.trim();

        if (_usesBase64 && (attrValue != null))
        {
            attrValue = new String(Base64.decodeBase64(attrValue.getBytes()));
        }

        Object propValue = (_converter != null ? _converter.convertFromString(attrValue, _column.getTypeCode()) : attrValue);

        if (digester.getLogger().isDebugEnabled())
        {
            digester.getLogger().debug("[SetColumnPropertyFromSubElementRule]{" + digester.getMatch() +
                                       "} Setting property '" + _column.getName() + "' to '" + propValue + "'");
        }

        PropertyUtils.setProperty(digester.peek(), _column.getName(), propValue);
    }
}
