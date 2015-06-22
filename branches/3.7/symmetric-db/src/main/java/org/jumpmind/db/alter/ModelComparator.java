package org.jumpmind.db.alter;

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

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares two database models and creates change objects that express how to
 * adapt the first model so that it becomes the second one. Neither of the
 * models are changed in the process, however, it is also assumed that the
 * models do not change in between.
 */
public class ModelComparator {

    /** The log for this comparator. */
    private final Logger log = LoggerFactory.getLogger(ModelComparator.class);

    /** The platform information. */
    protected DatabaseInfo platformInfo;

    /** Whether comparison is case sensitive. */
    protected boolean caseSensitive;
    
    protected String databaseName;

    /**
     * Creates a new model comparator object.
     * 
     * @param platformInfo
     *            The platform info
     * @param caseSensitive
     *            Whether comparison is case sensitive
     */
    public ModelComparator(String databaseName, DatabaseInfo platformInfo, boolean caseSensitive) {
        this.databaseName = databaseName;
        this.platformInfo = platformInfo;
        this.caseSensitive = caseSensitive;
    }

    /**
     * Compares the two models and returns the changes necessary to create the
     * second model from the first one.
     * 
     * @param sourceModel
     *            The source model
     * @param targetModel
     *            The target model
     * @return The changes
     */
    public List<IModelChange> compare(Database sourceModel, Database targetModel) {
        ArrayList<IModelChange> changes = new ArrayList<IModelChange>();

        for (int tableIdx = 0; tableIdx < targetModel.getTableCount(); tableIdx++) {
            Table targetTable = targetModel.getTable(tableIdx);
            Table sourceTable = sourceModel.findTable(targetTable.getName(), caseSensitive);

            if (sourceTable == null) {
                log.debug("Table {} needs to be added", targetTable.getName());
                changes.add(new AddTableChange(targetTable));
                if (platformInfo.isForeignKeysSupported()) {
                    for (int fkIdx = 0; fkIdx < targetTable.getForeignKeyCount(); fkIdx++) {
                        // we have to use target table's definition here because the
                        // complete table is new
                        changes.add(new AddForeignKeyChange(targetTable, targetTable
                                .getForeignKey(fkIdx)));
                    }
                }
            } else {
                changes.addAll(compareTables(sourceModel, sourceTable, targetModel, targetTable));
            }
        }

        for (int tableIdx = 0; tableIdx < sourceModel.getTableCount(); tableIdx++) {
            Table sourceTable = sourceModel.getTable(tableIdx);
            Table targetTable = targetModel.findTable(sourceTable.getName(), caseSensitive);

            if ((targetTable == null) && (sourceTable.getName() != null)
                    && (sourceTable.getName().length() > 0)) {
                log.debug("Table {} needs to be removed", sourceTable.getName());
                changes.add(new RemoveTableChange(sourceTable));
                /*
                 * we assume that the target model is sound, ie. that there are
                 * no longer any foreign keys to this table in the target model;
                 * thus we already have removeFK changes for these from the
                 * compareTables method and we only need to create changes for
                 * the fks originating from this table
                 */
                if (platformInfo.isForeignKeysSupported()) {
                    for (int fkIdx = 0; fkIdx < sourceTable.getForeignKeyCount(); fkIdx++) {
                        changes.add(new RemoveForeignKeyChange(sourceTable, sourceTable
                                .getForeignKey(fkIdx)));
                    }
                }
            }
        }
        return changes;
    }

    /**
     * Compares the two tables and returns the changes necessary to create the
     * second table from the first one.
     * 
     * @param sourceModel
     *            The source model which contains the source table
     * @param sourceTable
     *            The source table
     * @param targetModel
     *            The target model which contains the target table
     * @param targetTable
     *            The target table
     * @return The changes
     */
    public List<IModelChange> compareTables(Database sourceModel, Table sourceTable,
            Database targetModel, Table targetTable) {
        ArrayList<IModelChange> changes = new ArrayList<IModelChange>();

        if (platformInfo.isForeignKeysSupported()) {

            for (int fkIdx = 0; fkIdx < sourceTable.getForeignKeyCount(); fkIdx++) {
                ForeignKey sourceFk = sourceTable.getForeignKey(fkIdx);
                ForeignKey targetFk = findCorrespondingForeignKey(targetTable, sourceFk);
    
                if (targetFk == null) {
                    if (log.isDebugEnabled()) {
                        log.debug(sourceFk + " needs to be removed from table "
                                + sourceTable.getName());
                    }
                    changes.add(new RemoveForeignKeyChange(sourceTable, sourceFk));
                }
            }

            for (int fkIdx = 0; fkIdx < targetTable.getForeignKeyCount(); fkIdx++) {
                ForeignKey targetFk = targetTable.getForeignKey(fkIdx);
                ForeignKey sourceFk = findCorrespondingForeignKey(sourceTable, targetFk);
    
                if (sourceFk == null) {
                    if (log.isDebugEnabled()) {
                        log.debug(targetFk + " needs to be created for table "
                                + sourceTable.getName());
                    }
                    /*
                     * we have to use the target table here because the foreign
                     * key might reference a new column
                     */
                    changes.add(new AddForeignKeyChange(targetTable, targetFk));
                }
            }
        }
        if (platformInfo.isIndicesSupported()) {
            for (int indexIdx = 0; indexIdx < sourceTable.getIndexCount(); indexIdx++) {
                IIndex sourceIndex = sourceTable.getIndex(indexIdx);
                IIndex targetIndex = findCorrespondingIndex(targetTable, sourceIndex);
    
                if (targetIndex == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Index " + sourceIndex.getName() + " needs to be removed from table "
                                + sourceTable.getName());
                    }
                    changes.add(new RemoveIndexChange(sourceTable, sourceIndex));
                }
            }
            for (int indexIdx = 0; indexIdx < targetTable.getIndexCount(); indexIdx++) {
                IIndex targetIndex = targetTable.getIndex(indexIdx);
                IIndex sourceIndex = findCorrespondingIndex(sourceTable, targetIndex);
    
                if (sourceIndex == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Index " + targetIndex.getName() + " needs to be created for table "
                                + sourceTable.getName());
                    }
                    // we have to use the target table here because the index might
                    // reference a new column
                    changes.add(new AddIndexChange(targetTable, targetIndex));
                }
            }
        }

        HashMap<Column, TableChange> addColumnChanges = new HashMap<Column, TableChange>();

        for (int columnIdx = 0; columnIdx < targetTable.getColumnCount(); columnIdx++) {
            Column targetColumn = targetTable.getColumn(columnIdx);
            Column sourceColumn = sourceTable.findColumn(targetColumn.getName(), caseSensitive);

            if (sourceColumn == null) {
                log.debug("Column {} needs to be created for table {}",
                        new Object[] { targetColumn.getName(), sourceTable.getName() });

                AddColumnChange change = new AddColumnChange(sourceTable, targetColumn,
                        columnIdx > 0 ? targetTable.getColumn(columnIdx - 1) : null,
                        columnIdx < targetTable.getColumnCount() - 1 ? targetTable
                                .getColumn(columnIdx + 1) : null);

                changes.add(change);
                addColumnChanges.put(targetColumn, change);
            } else {
                changes.addAll(compareColumns(sourceTable, sourceColumn, targetTable, targetColumn));
            }
        }
        // if the last columns in the target table are added, then we note this
        // at the changes
        for (int columnIdx = targetTable.getColumnCount() - 1; columnIdx >= 0; columnIdx--) {
            Column targetColumn = targetTable.getColumn(columnIdx);
            AddColumnChange change = (AddColumnChange) addColumnChanges.get(targetColumn);

            if (change == null) {
                // column was not added, so we can ignore any columns before it
                // that were added
                break;
            } else {
                change.setAtEnd(true);
            }
        }

        Column[] sourcePK = sourceTable.getPrimaryKeyColumns();
        Column[] targetPK = targetTable.getPrimaryKeyColumns();

        if ((sourcePK.length == 0) && (targetPK.length > 0)) {
            if (log.isDebugEnabled()) {
                log.debug("A primary key needs to be added to the table " + sourceTable.getName());
            }
            // we have to use the target table here because the primary key
            // might
            // reference a new column
            changes.add(new AddPrimaryKeyChange(targetTable, targetPK));
        } else if ((targetPK.length == 0) && (sourcePK.length > 0)) {
            if (log.isDebugEnabled()) {
                log.debug("The primary key needs to be removed from the table "
                        + sourceTable.getName());
            }
            changes.add(new RemovePrimaryKeyChange(sourceTable, sourcePK));
        } else if ((sourcePK.length > 0) && (targetPK.length > 0)) {
            boolean changePK = false;

            if (sourcePK.length != targetPK.length) {
                changePK = true;
            } else {
                for (int pkColumnIdx = 0; (pkColumnIdx < sourcePK.length) && !changePK; pkColumnIdx++) {
                    if ((caseSensitive && !sourcePK[pkColumnIdx].getName().equals(
                            targetPK[pkColumnIdx].getName()))
                            || (!caseSensitive && !sourcePK[pkColumnIdx].getName()
                                    .equalsIgnoreCase(targetPK[pkColumnIdx].getName()))) {
                        changePK = true;
                    }
                }
            }
            if (changePK) {
                if (log.isDebugEnabled()) {
                    log.debug("The primary key of table " + sourceTable.getName()
                            + " needs to be changed");
                }
                changes.add(new PrimaryKeyChange(sourceTable, sourcePK, targetPK));
            }
        }

        for (int columnIdx = 0; columnIdx < sourceTable.getColumnCount(); columnIdx++) {
            Column sourceColumn = sourceTable.getColumn(columnIdx);
            Column targetColumn = targetTable.findColumn(sourceColumn.getName(), caseSensitive);

            if (targetColumn == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Column " + sourceColumn.getName()
                            + " needs to be removed from table " + sourceTable.getName());
                }
                changes.add(new RemoveColumnChange(sourceTable, sourceColumn));
            }
        }

        return changes;
    }

    /**
     * Compares the two columns and returns the changes necessary to create the
     * second column from the first one.
     * 
     * @param sourceTable
     *            The source table which contains the source column
     * @param sourceColumn
     *            The source column
     * @param targetTable
     *            The target table which contains the target column
     * @param targetColumn
     *            The target column
     * @return The changes
     */
    public List<TableChange> compareColumns(Table sourceTable, Column sourceColumn,
            Table targetTable, Column targetColumn) {
        ArrayList<TableChange> changes = new ArrayList<TableChange>();
        
        int actualTypeCode = sourceColumn.getMappedTypeCode();
        int desiredTypeCode = targetColumn.getMappedTypeCode();
        boolean sizeMatters = platformInfo.hasSize(targetColumn.getMappedTypeCode());
        boolean scaleMatters = platformInfo.hasPrecisionAndScale(targetColumn.getMappedTypeCode());

        boolean compatible = 
                (actualTypeCode == Types.NUMERIC || actualTypeCode == Types.DECIMAL) && 
                (desiredTypeCode == Types.INTEGER || desiredTypeCode == Types.BIGINT);
        
        if (sourceColumn.isAutoIncrement() && targetColumn.isAutoIncrement() &&
                (desiredTypeCode == Types.NUMERIC || desiredTypeCode == Types.DECIMAL) && 
                (actualTypeCode == Types.INTEGER || actualTypeCode == Types.BIGINT)) {
            compatible = true;
            
            // This is the rare case where size doesnt matter!
            sizeMatters = false;
            scaleMatters = false;
        }
        
        if (!compatible && targetColumn.getMappedTypeCode() != sourceColumn.getMappedTypeCode()
                && platformInfo.getTargetJdbcType(targetColumn.getMappedTypeCode()) != sourceColumn
                        .getMappedTypeCode()) {
            log.debug(
                    "The {} column on the {} table changed type codes from {} to {} ",
                    new Object[] { sourceColumn.getName(), sourceTable.getName(),
                            sourceColumn.getMappedTypeCode(), targetColumn.getMappedTypeCode() });
            changes.add(new ColumnDataTypeChange(sourceTable, sourceColumn, targetColumn
                    .getMappedTypeCode()));
        }

        String targetSize = targetColumn.getSize();
        if (targetSize == null) {
            Integer defaultSize = platformInfo.getDefaultSize(platformInfo
                    .getTargetJdbcType(targetColumn.getMappedTypeCode()));
            if (defaultSize != null) {
                targetSize = defaultSize.toString();
            } else {
                targetSize = "0";
            }
        }
        if (sizeMatters && !StringUtils.equals(sourceColumn.getSize(), targetSize)) {
            log.debug("The {} column on the {} table changed size from ({}) to ({})", new Object[] {
                    sourceColumn.getName(), sourceTable.getName(), sourceColumn.getSizeAsInt(),
                    targetColumn.getSizeAsInt() });

            changes.add(new ColumnSizeChange(sourceTable, sourceColumn,
                    targetColumn.getSizeAsInt(), targetColumn.getScale()));
        } else if (scaleMatters && (!StringUtils.equals(sourceColumn.getSize(), targetSize) ||
        // ojdbc6.jar returns -127 for the scale of NUMBER that was not given a
        // size or precision
                (!(sourceColumn.getScale() < 0 && targetColumn.getScale() == 0) && sourceColumn
                        .getScale() != targetColumn.getScale()))) {
            log.debug(
                    "The {} column on the {} table changed scale from ({},{}) to ({},{})",
                    new Object[] { sourceColumn.getName(), sourceTable.getName(),
                            sourceColumn.getSizeAsInt(), sourceColumn.getScale(),
                            targetColumn.getSizeAsInt(), targetColumn.getScale() });
            changes.add(new ColumnSizeChange(sourceTable, sourceColumn,
                    targetColumn.getSizeAsInt(), targetColumn.getScale()));
        }

        Object sourceDefaultValue = sourceColumn.getParsedDefaultValue();
        Object targetDefaultValue = targetColumn.getParsedDefaultValue();

        if ((sourceDefaultValue == null && targetDefaultValue != null)
                || (sourceDefaultValue != null && targetDefaultValue == null)
                || (sourceDefaultValue != null && targetDefaultValue != null && !sourceDefaultValue
                        .toString().equals(targetDefaultValue.toString()))) {
            log.debug(
                    "The {} column on the {} table changed default value from {} to {} ",
                    new Object[] { sourceColumn.getName(), sourceTable.getName(),
                            sourceColumn.getDefaultValue(), targetColumn.getDefaultValue() });
            changes.add(new ColumnDefaultValueChange(sourceTable, sourceColumn, targetColumn
                    .getDefaultValue()));
        }

        if (sourceColumn.isRequired() != targetColumn.isRequired()) {
            log.debug(
                    "The {} column on the {} table changed required status from {} to {}",
                    new Object[] { sourceColumn.getName(), sourceTable.getName(),
                            sourceColumn.isRequired(), targetColumn.isRequired() });
            changes.add(new ColumnRequiredChange(sourceTable, sourceColumn));
        }

        if (sourceColumn.isAutoIncrement() != targetColumn.isAutoIncrement()) {
            log.debug(
                    "The {} column on the {} table changed auto increment status from {} to {} ",
                    new Object[] { sourceColumn.getName(), sourceTable.getName(),
                            sourceColumn.isAutoIncrement(), targetColumn.isAutoIncrement() });
            changes.add(new ColumnAutoIncrementChange(sourceTable, sourceColumn));
        }

        return changes;
    }

    /**
     * Searches in the given table for a corresponding foreign key. If the given
     * key has no name, then a foreign key to the same table with the same
     * columns (but not necessarily in the same order) is searched. If the given
     * key has a name, then the corresponding key also needs to have the same
     * name, or no name at all, but not a different one.
     * 
     * @param table
     *            The table to search in
     * @param fk
     *            The original foreign key
     * @return The corresponding foreign key if found
     */
    private ForeignKey findCorrespondingForeignKey(Table table, ForeignKey fk) {
        for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++) {
            ForeignKey curFk = table.getForeignKey(fkIdx);

            if ((caseSensitive && fk.equals(curFk))
                    || (!caseSensitive && fk.equalsIgnoreCase(curFk))) {
                return curFk;
            }
        }
        return null;
    }

    /**
     * Searches in the given table for a corresponding index. If the given index
     * has no name, then a index to the same table with the same columns in the
     * same order is searched. If the given index has a name, then the a
     * corresponding index also needs to have the same name, or no name at all,
     * but not a different one.
     * 
     * @param table
     *            The table to search in
     * @param index
     *            The original index
     * @return The corresponding index if found
     */
    private IIndex findCorrespondingIndex(Table table, IIndex index) {
        for (int indexIdx = 0; indexIdx < table.getIndexCount(); indexIdx++) {
            IIndex curIndex = table.getIndex(indexIdx);

            if ((caseSensitive && index.equals(curIndex))
                    || (!caseSensitive && index.equalsIgnoreCase(curIndex))) {
                return curIndex;
            }
        }
        return null;
    }
}
