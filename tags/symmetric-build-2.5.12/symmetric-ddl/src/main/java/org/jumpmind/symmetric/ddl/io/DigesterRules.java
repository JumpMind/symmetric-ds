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

import java.util.List;

import org.apache.commons.digester.Rule;
import org.apache.commons.digester.RulesBase;

/*
 * An extended rules implementation that is able to match case-insensitively. Per default,
 * the rules are matches disregarding the case.
 * 
 * @version $Revision: 289996 $
 */
public class DigesterRules extends RulesBase
{
    /* Whether to be case sensitive or not. */
    private boolean _caseSensitive = false;

    /*
     * Determines whether this rules object matches case sensitively.
     *
     * @return <code>true</code> if the case of the pattern matters
     */
    public boolean isCaseSensitive()
    {
        return _caseSensitive;
    }

    /*
     * Specifies whether this rules object shall match case sensitively.
     *
     * @param beCaseSensitive <code>true</code> if the case of the pattern shall matter
     */
    public void setCaseSensitive(boolean beCaseSensitive)
    {
        _caseSensitive = beCaseSensitive;
    }

    /*
     * {@inheritDoc}
     */
    public void add(String pattern, Rule rule)
    {
        super.add(_caseSensitive ? pattern : pattern.toLowerCase(), rule);
    }

    /*
     * {@inheritDoc}
     */
    protected List lookup(String namespaceURI, String pattern)
    {
        return super.lookup(namespaceURI, _caseSensitive ? pattern : pattern.toLowerCase());
    }
}
