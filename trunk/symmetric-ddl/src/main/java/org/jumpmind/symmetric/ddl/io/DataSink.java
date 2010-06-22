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

/**
 * Marks classes that can receive dyna beans read by the {@link org.jumpmind.symmetric.ddl.io.DataReader}.
 * 
 * @version $Revision: 289996 $
 */
public interface DataSink
{
    /**
     * Notifies the sink that beans will be added.
     */
    public void start() throws DataSinkException;

    /**
     * Adds a dyna bean.
     * 
     * @param bean The dyna bean to add
     */
    public void addBean(DynaBean bean) throws DataSinkException;

    /**
     * Notifies the sink that all beans have been added.
     */
    public void end() throws DataSinkException;
}
