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

import java.util.Iterator;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.collections.set.ListOrderedSet;

/**
 * Represents an object waiting for insertion into the database. Is used by the
 * {@link org.jumpmind.symmetric.ddl.io.DataToDatabaseSink} to insert the objects in the correct
 * order according to their foreign keys.
 * 
 * @version $Revision: 289996 $
 */
public class WaitingObject
{
    /** The object that is waiting for insertion. */
    private DynaBean _obj;
    /** The original identity of the waiting object. */
    private Identity _objIdentity;
    /** The identities of the waited-for objects. */
    private ListOrderedSet _waitedForIdentites = new ListOrderedSet();

    /**
     * Creates a new <code>WaitingObject</code> instance for the given object.
     * 
     * @param obj         The object that is waiting
     * @param objIdentity The (original) identity of the object
     */
    public WaitingObject(DynaBean obj, Identity objIdentity)
    {
        _obj         = obj;
        _objIdentity = objIdentity;
    }

    /**
     * Returns the waiting object.
     * 
     * @return The object
     */
    public DynaBean getObject()
    {
        return _obj;
    }

    /**
     * Adds the identity of another object that the object is waiting for.
     * 
     * @param fkIdentity The identity of the waited-for object
     */
    public void addPendingFK(Identity fkIdentity)
    {
        _waitedForIdentites.add(fkIdentity);
    }

    /**
     * Returns the identities of the object that this object is waiting for.
     * 
     * @return The identities
     */
    public Iterator getPendingFKs()
    {
        return _waitedForIdentites.iterator();
    }

    /**
     * Removes the specified identity from list of identities of the waited-for objects.
     * 
     * @param fkIdentity The identity to remove
     * @return The removed identity if any
     */
    public Identity removePendingFK(Identity fkIdentity)
    {
        Identity result = null;
        int      idx    = _waitedForIdentites.indexOf(fkIdentity);

        if (idx >= 0)
        {
            result = (Identity)_waitedForIdentites.get(idx);
            _waitedForIdentites.remove(idx);
        }
        return result;
    }

    /**
     * Determines whether there are any identities of waited-for objects
     * registered with this waiting object.
     * 
     * @return <code>true</code> if identities of waited-for objects are registered
     */
    public boolean hasPendingFKs()
    {
        return !_waitedForIdentites.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        StringBuffer result = new StringBuffer();

        result.append(_objIdentity);
        result.append(" waiting for ");
        result.append(_waitedForIdentites.toString());
        return result.toString();
    }
}
