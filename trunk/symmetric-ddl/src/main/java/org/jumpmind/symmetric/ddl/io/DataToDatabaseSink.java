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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.ddl.DatabaseOperationException;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.dynabean.SqlDynaClass;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.ForeignKey;
import org.jumpmind.symmetric.ddl.model.Reference;
import org.jumpmind.symmetric.ddl.model.Table;

/**
 * Data sink that directly inserts the beans into the database. If configured, it will make
 * sure that the beans are inserted in the correct order according to the foreignkeys. Note
 * that this will only work if there are no circles.
 * 
 * @version $Revision: 289996 $
 */
public class DataToDatabaseSink implements DataSink
{
    /** Our log. */
    private final Log _log = LogFactory.getLog(DataToDatabaseSink.class);
 
    /** Generates the sql and writes it to the database. */
    private Platform _platform;
    /** The database model. */
    private Database _model;
    /** The connection to the database. */
    private Connection _connection;
    /** Whether to stop when an error has occurred while inserting a bean into the database. */
    private boolean _haltOnErrors = true;
    /** Whether to delay the insertion of beans so that the beans referenced by it via foreignkeys, are already inserted into the database. */
    private boolean _ensureFkOrder = true;
    /** Whether to use batch mode inserts. */
    private boolean _useBatchMode = false;
    /** The queued objects for batch insertion. */
    private ArrayList _batchQueue = new ArrayList();
    /** The number of beans to insert in one batch. */
    private int _batchSize = 1024;
    /** Stores the tables that are target of a foreign key. */
    private HashSet _fkTables = new HashSet();
    /** Contains the tables that have a self-referencing foreign key to a (partially) identity primary key. */
    private HashSet _tablesWithSelfIdentityReference = new HashSet();
    /** Contains the tables that have a self-referencing foreign key that is required. */
    private HashSet _tablesWithRequiredSelfReference = new HashSet();
    /** Maps original to processed identities. */
    private HashMap _identityMap = new HashMap();
    /** Stores the objects that are waiting for other objects to be inserted. */
    private ArrayList _waitingObjects = new ArrayList();

    /**
     * Creates a new sink instance.
     * 
     * @param platform The database platform
     * @param model    The database model
     */
    public DataToDatabaseSink(Platform platform, Database model)
    {
        _platform = platform;
        _model    = model;
        for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++)
        {
            Table      table     = model.getTable(tableIdx);
            ForeignKey selfRefFk = table.getSelfReferencingForeignKey();

            if (selfRefFk != null)
            {
                Column[] pkColumns = table.getPrimaryKeyColumns();

                for (int idx = 0; idx < pkColumns.length; idx++)
                {
                    if (pkColumns[idx].isAutoIncrement())
                    {
                        _tablesWithSelfIdentityReference.add(table);
                        break;
                    }
                }
                for (int idx = 0; idx < selfRefFk.getReferenceCount(); idx++)
                {
                    if (selfRefFk.getReference(idx).getLocalColumn().isRequired())
                    {
                        _tablesWithRequiredSelfReference.add(table);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Determines whether this sink halts when an error happens during the insertion of a bean
     * into the database. Default is <code>true</code>.
     *
     * @return <code>true</code> if the sink stops when an error occurred
     */
    public boolean isHaltOnErrors()
    {
        return _haltOnErrors;
    }

    /**
     * Specifies whether this sink halts when an error happens during the insertion of a bean
     * into the database.
     *
     * @param haltOnErrors <code>true</code> if the sink shall stop when an error occurred
     */
    public void setHaltOnErrors(boolean haltOnErrors)
    {
        _haltOnErrors = haltOnErrors;
    }

    /**
     * Determines whether the sink delays the insertion of beans so that the beans referenced by it
     * via foreignkeys are already inserted into the database.
     *
     * @return <code>true</code> if beans are inserted after its foreignkey-references
     */
    public boolean isEnsureFkOrder()
    {
        return _ensureFkOrder;
    }

    /**
     * Specifies whether the sink shall delay the insertion of beans so that the beans referenced by it
     * via foreignkeys are already inserted into the database.<br/>
     * Note that you should careful with setting <code>haltOnErrors</code> to false as this might
     * result in beans not inserted at all. The sink will then throw an appropriate exception at the end
     * of the insertion process (method {@link #end()}).
     *
     * @param ensureFkOrder <code>true</code> if beans shall be inserted after its foreignkey-references
     */
    public void setEnsureForeignKeyOrder(boolean ensureFkOrder)
    {
        _ensureFkOrder = ensureFkOrder;
    }

    /**
     * Determines whether batch mode is used for inserting the beans.
     *
     * @return <code>true</code> if batch mode is used (<code>false</code> per default)
     */
    public boolean isUseBatchMode()
    {
        return _useBatchMode;
    }

    /**
     * Specifies whether batch mode is used for inserting the beans. Note that this requires
     * that the primary key values are not defined by the database.
     *
     * @param useBatchMode <code>true</code> if batch mode shall be used
     */
    public void setUseBatchMode(boolean useBatchMode)
    {
        _useBatchMode = useBatchMode;
    }

    /**
     * Returns the (maximum) number of beans to insert in one batch.
     *
     * @return The number of beans
     */
    public int getBatchSize()
    {
        return _batchSize;
    }

    /**
     * Sets the (maximum) number of beans to insert in one batch.
     *
     * @param batchSize The number of beans
     */
    public void setBatchSize(int batchSize)
    {
        _batchSize = batchSize;
    }

    /**
     * {@inheritDoc}
     */
    public void end() throws DataSinkException
    {
        purgeBatchQueue();
        try
        {
            _connection.close();
        }
        catch (SQLException ex)
        {
            throw new DataSinkException(ex);
        }
        if (!_waitingObjects.isEmpty())
        {
            if (_log.isDebugEnabled())
            {
                for (Iterator it = _waitingObjects.iterator(); it.hasNext();)
                {
                    WaitingObject obj   = (WaitingObject)it.next();
                    Table         table = _model.getDynaClassFor(obj.getObject()).getTable();
                    Identity      objId = buildIdentityFromPKs(table, obj.getObject());

                    _log.debug("Row " + objId + " is still not written because it depends on these yet unwritten rows");
                    for (Iterator fkIt = obj.getPendingFKs(); fkIt.hasNext();)
                    {
                        Identity pendingFkId = (Identity)fkIt.next();

                        _log.debug("  " + pendingFkId);
                    }
                        
                }
            }
            if (_waitingObjects.size() == 1)
            {
                throw new DataSinkException("There is one row still not written because of missing referenced rows");
            }
            else
            {
                throw new DataSinkException("There are " + _waitingObjects.size() + " rows still not written because of missing referenced rows");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void start() throws DataSinkException
    {
        _fkTables.clear();
        _waitingObjects.clear();
        if (_ensureFkOrder)
        {
            for (int tableIdx = 0; tableIdx < _model.getTableCount(); tableIdx++)
            {
                Table table = _model.getTable(tableIdx);
    
                for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++)
                {
                    ForeignKey curFk = table.getForeignKey(fkIdx);
    
                    _fkTables.add(curFk.getForeignTable());
                }
            }
        }
        try
        {
            _connection = _platform.borrowConnection();
        }
        catch (DatabaseOperationException ex)
        {
            throw new DataSinkException(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void addBean(DynaBean bean) throws DataSinkException
    {
        Table    table        = _model.getDynaClassFor(bean).getTable();
        Identity origIdentity = buildIdentityFromPKs(table, bean);

        if (_ensureFkOrder && (table.getForeignKeyCount() > 0))
        {
            WaitingObject waitingObj = new WaitingObject(bean, origIdentity);

            for (int idx = 0; idx < table.getForeignKeyCount(); idx++)
            {
                ForeignKey fk         = table.getForeignKey(idx);
                Identity   fkIdentity = buildIdentityFromFK(table, fk, bean);

                if ((fkIdentity != null) && !fkIdentity.equals(origIdentity))
                {
                    Identity processedIdentity = (Identity)_identityMap.get(fkIdentity);

                    if (processedIdentity != null)
                    {
                        updateFKColumns(bean, fkIdentity.getForeignKeyName(), processedIdentity);
                    }
                    else
                    {
                        waitingObj.addPendingFK(fkIdentity);
                    }
                }
            }
            if (waitingObj.hasPendingFKs())
            {
                if (_log.isDebugEnabled())
                {
                    StringBuffer msg = new StringBuffer();

                    msg.append("Defering insertion of row ");
                    msg.append(buildIdentityFromPKs(table, bean).toString());
                    msg.append(" because it is waiting for:");
                    for (Iterator it = waitingObj.getPendingFKs(); it.hasNext();)
                    {
                        msg.append("\n  ");
                        msg.append(it.next().toString());
                    }
                    _log.debug(msg.toString());
                }
                _waitingObjects.add(waitingObj);
                return;
            }
        }

        insertBeanIntoDatabase(table, bean);

        if (_log.isDebugEnabled())
        {
            _log.debug("Inserted bean " + origIdentity);
        }

        if (_ensureFkOrder && _fkTables.contains(table))
        {
            Identity  newIdentity  = buildIdentityFromPKs(table, bean);
            ArrayList finishedObjs = new ArrayList();

            _identityMap.put(origIdentity, newIdentity);

            // we're doing multiple passes so that we can insert as much objects in
            // one go as possible
            ArrayList identitiesToCheck = new ArrayList();

            identitiesToCheck.add(origIdentity);
            while (!identitiesToCheck.isEmpty() && !_waitingObjects.isEmpty())
            {
                Identity curIdentity    = (Identity)identitiesToCheck.get(0);
                Identity curNewIdentity = (Identity)_identityMap.get(curIdentity);

                identitiesToCheck.remove(0);
                finishedObjs.clear();
                for (Iterator waitingObjIt = _waitingObjects.iterator(); waitingObjIt.hasNext();)
                {
                    WaitingObject waitingObj = (WaitingObject)waitingObjIt.next();
                    Identity      fkIdentity = waitingObj.removePendingFK(curIdentity);

                    if (fkIdentity != null)
                    {
                        updateFKColumns(waitingObj.getObject(), fkIdentity.getForeignKeyName(), curNewIdentity);
                    }
                    if (!waitingObj.hasPendingFKs())
                    {
                        waitingObjIt.remove();
                        // we defer handling of the finished objects to avoid concurrent modification exceptions
                        finishedObjs.add(waitingObj.getObject());
                    }
                }
                for (Iterator finishedObjIt = finishedObjs.iterator(); finishedObjIt.hasNext();)
                {
                    DynaBean finishedObj = (DynaBean)finishedObjIt.next();
                    Table    tableForObj = _model.getDynaClassFor(finishedObj).getTable();
                    Identity objIdentity = buildIdentityFromPKs(tableForObj, finishedObj);

                    insertBeanIntoDatabase(tableForObj, finishedObj);
                    
                    Identity newObjIdentity = buildIdentityFromPKs(tableForObj, finishedObj);

                    _identityMap.put(objIdentity, newObjIdentity);
                    identitiesToCheck.add(objIdentity);
                    if (_log.isDebugEnabled())
                    {
                        _log.debug("Inserted deferred row " + objIdentity);
                    }
                }
            }
        }
    }

    /**
     * Inserts the bean into the database or batch queue.
     * 
     * @param table The table
     * @param bean  The bean
     */
    private void insertBeanIntoDatabase(Table table, DynaBean bean) throws DataSinkException
    {
        if (_useBatchMode)
        {
            _batchQueue.add(bean);
            if (_batchQueue.size() >= _batchSize)
            {
                purgeBatchQueue();
            }
        }
        else
        {
            insertSingleBeanIntoDatabase(table, bean);
        }
    }

    /**
     * Purges the batch queue by inserting the objects into the database.
     */
    private void purgeBatchQueue() throws DataSinkException
    {
        if (!_batchQueue.isEmpty())
        {
            try
            {
                _platform.insert(_connection, _model, _batchQueue);
                if (!_connection.getAutoCommit())
                {
                    _connection.commit();
                }
                if (_log.isDebugEnabled())
                {
                    _log.debug("Inserted " + _batchQueue.size() + " rows in batch mode ");
                }
            }
            catch (Exception ex)
            {
                if (_haltOnErrors)
                {
                    _platform.returnConnection(_connection);
                    throw new DataSinkException(ex);
                }
                else
                {
                    _log.warn("Exception while inserting " + _batchQueue.size() + " rows via batch mode into the database", ex);
                }
            }
            _batchQueue.clear();
        }
    }
    
    /**
     * Directly inserts the given bean into the database.
     * 
     * @param table The table of the bean
     * @param bean  The bean
     */
    private void insertSingleBeanIntoDatabase(Table table, DynaBean bean) throws DataSinkException
    {
        try
        {
            boolean    needTwoStepInsert = false;
            ForeignKey selfRefFk         = null;

            if (!_platform.isIdentityOverrideOn() &&
                _tablesWithSelfIdentityReference.contains(table))
            {
                selfRefFk = table.getSelfReferencingForeignKey();

                // in case of a self-reference (fk points to the very row that we're inserting)
                // and (at least) one of the pk columns is an identity column, we first need
                // to insert the row with the fk columns set to null
                Identity pkIdentity = buildIdentityFromPKs(table, bean);
                Identity fkIdentity = buildIdentityFromFK(table, selfRefFk, bean);

                if (pkIdentity.equals(fkIdentity))
                {
                    if (_tablesWithRequiredSelfReference.contains(table))
                    {
                        throw new DataSinkException("Can only insert rows with fk pointing to themselves when all fk columns can be NULL (row pk is " + pkIdentity + ")");
                    }
                    else
                    {
                        needTwoStepInsert = true;
                    }
                }
            }

            if (needTwoStepInsert)
            {
                // we first insert the bean without the fk, then in the second step we update the bean
                // with the row with the identity pk values
                ArrayList fkValues = new ArrayList();

                for (int idx = 0; idx < selfRefFk.getReferenceCount(); idx++)
                {
                    String columnName = selfRefFk.getReference(idx).getLocalColumnName();

                    fkValues.add(bean.get(columnName));
                    bean.set(columnName, null);
                }
                _platform.insert(_connection, _model, bean);
                for (int idx = 0; idx < selfRefFk.getReferenceCount(); idx++)
                {
                    bean.set(selfRefFk.getReference(idx).getLocalColumnName(), fkValues.get(idx));
                }
                _platform.update(_connection, _model, bean);
            }
            else
            {
                _platform.insert(_connection, _model, bean);
            }
            if (!_connection.getAutoCommit())
            {
                _connection.commit();
            }
        }
        catch (Exception ex)
        {
            if (_haltOnErrors)
            {
                _platform.returnConnection(_connection);
                throw new DataSinkException(ex);
            }
            else
            {
                _log.warn("Exception while inserting a row into the database", ex);
            }
        }
    }
    
    /**
     * Returns the name of the given foreign key. If it has no name, then a temporary one
     * is generated from the names of the relevant tables and columns.
     *
     * @param owningTable    The table owning the fk
     * @param fk             The foreign key
     * @return The name
     */
    private String getFKName(Table owningTable, ForeignKey fk)
    {
        if ((fk.getName() != null) && (fk.getName().length() > 0))
        {
            return fk.getName();
        }
        else
        {
            StringBuffer result = new StringBuffer();

            result.append(owningTable.getName());
            result.append("[");
            for (int idx = 0; idx < fk.getReferenceCount(); idx++)
            {
                if (idx > 0)
                {
                    result.append(",");
                }
                result.append(fk.getReference(idx).getLocalColumnName());
            }
            result.append("]->");
            result.append(fk.getForeignTableName());
            result.append("[");
            for (int idx = 0; idx < fk.getReferenceCount(); idx++)
            {
                if (idx > 0)
                {
                    result.append(",");
                }
                result.append(fk.getReference(idx).getForeignColumnName());
            }
            result.append("]");
            return result.toString();
        }
    }
    
    /**
     * Builds an identity object from the primary keys of the specified table using the
     * column values of the supplied bean.
     * 
     * @param table The table
     * @param bean  The bean
     * @return The identity
     */
    private Identity buildIdentityFromPKs(Table table, DynaBean bean)
    {
        Identity identity  = new Identity(table);
        Column[] pkColumns = table.getPrimaryKeyColumns();

        for (int idx = 0; idx < pkColumns.length; idx++)
        {
            identity.setColumnValue(pkColumns[idx].getName(), bean.get(pkColumns[idx].getName()));
        }
        return identity;
    }

    /**
     * Builds an identity object for the specified foreign key using the foreignkey column values
     * of the supplied bean.
     * 
     * @param owningTable The table owning the foreign key
     * @param fk          The foreign key
     * @param bean        The bean
     * @return The identity
     */
    private Identity buildIdentityFromFK(Table owningTable, ForeignKey fk, DynaBean bean)
    {
        Identity identity = new Identity(fk.getForeignTable(), getFKName(owningTable, fk));

        for (int idx = 0; idx < fk.getReferenceCount(); idx++)
        {
            Reference reference = (Reference)fk.getReference(idx);
            Object    value     = bean.get(reference.getLocalColumnName());

            if (value == null)
            {
                return null;
            }
            identity.setColumnValue(reference.getForeignColumnName(), value);
        }
        return identity;
    }

    /**
     * Updates the values of the columns constituting the indicated foreign key with the values
     * of the given identity.
     * 
     * @param bean     The bean whose columns shall be updated
     * @param fkName   The name of the foreign key
     * @param identity The target identity
     */
    private void updateFKColumns(DynaBean bean, String fkName, Identity identity)
    {
        Table      sourceTable = ((SqlDynaClass)bean.getDynaClass()).getTable();
        Table      targetTable = identity.getTable();
        ForeignKey fk          = null;

        for (int idx = 0; idx < sourceTable.getForeignKeyCount(); idx++)
        {
            ForeignKey curFk = sourceTable.getForeignKey(idx);

            if (curFk.getForeignTableName().equalsIgnoreCase(targetTable.getName()))
            {
                if (fkName.equals(getFKName(sourceTable, curFk)))
                {
                    fk = curFk;
                    break;
                }
            }
        }
        if (fk != null)
        {
            for (int idx = 0; idx < fk.getReferenceCount(); idx++)
            {
                Reference curRef       = fk.getReference(idx);
                Column    sourceColumn = curRef.getLocalColumn();
                Column    targetColumn = curRef.getForeignColumn();

                bean.set(sourceColumn.getName(), identity.getColumnValue(targetColumn.getName()));
            }
        }
    }
}
