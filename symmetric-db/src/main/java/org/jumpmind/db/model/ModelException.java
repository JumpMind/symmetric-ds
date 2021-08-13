/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.db.model;

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

import org.jumpmind.db.platform.DdlException;

/**
 * Indicates a model error.
 * 
 * @version $Revision: 289996 $
 */
public class ModelException extends DdlException {
    /** Constant for serializing instances of this class. */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new empty exception object.
     */
    public ModelException() {
        super();
    }

    /**
     * Creates a new exception object.
     * 
     * @param msg
     *            The exception message
     */
    public ModelException(String msg) {
        super(msg);
    }

    /**
     * Creates a new exception object.
     * 
     * @param baseEx
     *            The base exception
     */
    public ModelException(Throwable baseEx) {
        super(baseEx);
    }

    /**
     * Creates a new exception object.
     * 
     * @param msg
     *            The exception message
     * @param baseEx
     *            The base exception
     */
    public ModelException(String msg, Throwable baseEx) {
        super(msg, baseEx);
    }
}
