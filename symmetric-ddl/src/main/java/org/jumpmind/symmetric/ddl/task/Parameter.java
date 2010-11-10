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

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.jumpmind.symmetric.ddl.PlatformFactory;

/**
 * Specifies a parameter for the creation of the database. These are usually platform specific.
 * A parameter consists of a name-value pair and an optional list of platforms for which the
 * parameter shall be used.
 * 
 * @version $Revision: 231306 $
 * @ant.type name="parameter"
 */
public class Parameter
{
    /** The name. */
    private String _name;
    /** The value. */
    private String _value;
    /** The platforms for which this parameter is applicable. */
    private Set _platforms = new HashSet();

    /**
     * Returns the name.
     *
     * @return The name
     */
    public String getName()
    {
        return _name;
    }

    /**
     * Specifies the name of the parameter. See the database support documentation
     * for details on the parameters supported by the individual platforms.
     *
     * @param name The name
     * @ant.required
     */
    public void setName(String name)
    {
        _name = name;
    }

    /**
     * Returns the value.
     *
     * @return The value
     */
    public String getValue()
    {
        return _value;
    }

    /**
     * Specifies the parameter value.
     *
     * @param value The value
     * @ant.not-required If none is given, <code>null</code> is used.
     */
    public void setValue(String value)
    {
        _value = value;
    }

    /**
     * Specifies the platforms - a comma-separated list of platform names - for which this parameter
     * shall be used (see the <code>databaseType</code> attribute of the tasks for possible values).
     * For every platform not in this list, the parameter is ignored.
     * 
     * @param platforms The platforms
     * @ant.not-required If not specified then the parameter is processed for every platform.
     */
    public void setPlatforms(String platforms)
    {
        _platforms.clear();
        if (platforms != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(platforms, ",");

            while (tokenizer.hasMoreTokens())
            {
                String platform = tokenizer.nextToken().trim();

                if (PlatformFactory.isPlatformSupported(platform))
                {
                    _platforms.add(platform.toLowerCase());
                }
                else
                {
                    throw new IllegalArgumentException("Platform "+platform+" is not supported");
                }
            }
        }
    }

    /**
     * Determines whether this parameter is applicable for the indicated platform.
     * 
     * @param platformName The platform name
     * @return <code>true</code> if this parameter is defined for the platform
     */
    public boolean isForPlatform(String platformName)
    {
        return _platforms.isEmpty() || _platforms.contains(platformName.toLowerCase());
    }
}
