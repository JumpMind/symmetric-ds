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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabasePlatformInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares two database models and creates change objects that express how to
 * adapt the first model so that it becomes the second one. Neither of the
 * models are changed in the process, however, it is also assumed that the
 * models do not change in between.
 * 
 * TODO: Add support and tests for the change of the column order
 */
public class ModelComparator {
    
    /** The log for this comparator. */
    private final Logger log = LoggerFactory.getLogger(ModelComparator.class);

    /** The platform information. */
    private DatabasePlatformInfo platformInfo;
    
    /** Whether comparison is case sensitive. */
    private boolean caseSensitive;

    /**
     * Creates a new model comparator object.
     * 
     * @param platformInfo
     *            The platform info
     * @param caseSensitive
     *            Whether comparison is case sensitive
     */
    public ModelComparator(DatabasePlatformInfo platformInfo, boolean caseSensitive) {
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
                if (log.isDebugEnabled()) {
                    log.debug("Table " + targetTable.getName() + " needs to be added");
                }
                changes.add(new AddTableChange(targetTable));
                for (int fkIdx = 0; fkIdx < targetTable.getForeignKeyCount(); fkIdx++) {
                    // we have to use target table's definition here because the
                    // complete table is new
                    changes.add(new AddForeignKeyChange(targetTable, targetTable
                            .getForeignKey(fkIdx)));
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
                if (log.isDebugEnabled()) {
                    log.debug("Table " + sourceTable.getName() + " needs to be removed");
                }
                changes.add(new RemoveTableChange(sourceTable));
                // we assume that the target model is sound, ie. that there are
                // no longer any foreign
                // keys to this table in the target model; thus we already have
                // removeFK changes for
                // these from the compareTables method and we only need to
                // create changes for the fks
                // originating from this table
                for (int fkIdx = 0; fkIdx < sourceTable.getForeignKeyCount(); fkIdx++) {
                    changes.add(new RemoveForeignKeyChange(sourceTable, sourceTable
                            .getForeignKey(fkIdx)));
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

        for (int fkIdx = 0; fkIdx < sourceTable.getForeignKeyCount(); fkIdx++) {
            ForeignKey sourceFk = sourceTable.getForeignKey(fkIdx);
            ForeignKey targetFk = findCorrespondingForeignKey(targetTable, sourceFk);

            if (targetFk == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Foreign key " + sourceFk + " needs to be removed from table "
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
                    log.debug("Foreign key " + targetFk + " needs to be created for table "
                            + sourceTable.getName());
                }
                // we have to use the target table here because the foreign key
                // might
                // reference a new column
                changes.add(new AddForeignKeyChange(targetTable, targetFk));
            }
        }

        for (int indexIdx = 0; indexIdx < sourceTable.getIndexCount(); indexIdx++) {
            IIndex sourceIndex = sourceTable.getIndex(indexIdx);
            IIndex targetIndex = findCorrespondingIndex(targetTable, sourceIndex);

            if (targetIndex == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Index " + sourceIndex.getName()
                            + " needs to be removed from table " + sourceTable.getName());
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

        HashMap addColumnChanges = new HashMap();

        for (int columnIdx = 0; columnIdx < targetTable.getColumnCount(); columnIdx++) {
            Column targetColumn = targetTable.getColumn(columnIdx);
            Column sourceColumn = sourceTable.findColumn(targetColumn.getName(), caseSensitive);

            if (sourceColumn == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Column " + targetColumn.getName()
                            + " needs to be created for table " + sourceTable.getName());
                }

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
    public List compareColumns(Table sourceTable, Column sourceColumn, Table targetTable,
            Column targetColumn) {
        ArrayList changes = new ArrayList();

        if (targetColumn.getTypeCode() != sourceColumn.getTypeCode()
                && platformInfo.getTargetJdbcType(targetColumn.getTypeCode()) != sourceColumn
                        .getTypeCode()) {
            if (log.isDebugEnabled()) {
                log.debug("The " + sourceColumn.getName() + " column on the "
                        + sourceTable.getName() + " table changed type codes from "
                        + sourceColumn.getTypeCode() + " to " + targetColumn.getTypeCode());
            }
            changes.add(new ColumnDataTypeChange(sourceTable, sourceColumn, targetColumn
                    .getTypeCode()));
        }

        boolean sizeMatters = platformInfo.hasSize(targetColumn.getTypeCode());
        boolean scaleMatters = platformInfo.hasPrecisionAndScale(targetColumn.getTypeCode());

        String targetSize = targetColumn.getSize();
        if (targetSize == null) {
            Integer defaultSize = platformInfo.getDefaultSize(platformInfo
                    .getTargetJdbcType(targetColumn.getTypeCode()));
            if (defaultSize != null) {
                targetSize = defaultSize.toString();
            } else {
                targetSize = "0";
            }
        }
        if (sizeMatters && !StringUtils.equals(sourceColumn.getSize(), targetSize)) {
            if (log.isDebugEnabled()) {
                log.debug("The " + sourceColumn.getName() + " column on the "
                        + sourceTable.getName() + " table changed size from ("
                        + sourceColumn.getSizeAsInt() + ") to (" + targetColumn.getSizeAsInt()
                        + ")");
            }
            changes.add(new ColumnSizeChange(sourceTable, sourceColumn,
                    targetColumn.getSizeAsInt(), targetColumn.getScale()));
        } else if (scaleMatters && (!StringUtils.equals(sourceColumn.getSize(), targetSize) ||
        // ojdbc6.jar returns -127 for the scale of NUMBER that was not given a
        // size or precision
                (!(sourceColumn.getScale() < 0 && targetColumn.getScale() == 0) && sourceColumn
                        .getScale() != targetColumn.getScale()))) {
            if (log.isDebugEnabled()) {
                log.debug("The " + sourceColumn.getName() + " column on the "
                        + sourceTable.getName() + " table changed scale from ("
                        + sourceColumn.getSizeAsInt() + "," + sourceColumn.getScale() + ") to ("
                        + targetColumn.getSizeAsInt() + "," + targetColumn.getScale() + ")");
            }
            changes.add(new ColumnSizeChange(sourceTable, sourceColumn,
                    targetColumn.getSizeAsInt(), targetColumn.getScale()));
        }

        Object sourceDefaultValue = sourceColumn.getParsedDefaultValue();
        Object targetDefaultValue = targetColumn.getParsedDefaultValue();

        if ((sourceDefaultValue == null && targetDefaultValue != null)
                || (sourceDefaultValue != null && targetDefaultValue == null)
                || (sourceDefaultValue != null && targetDefaultValue != null && !sourceDefaultValue
                        .toString().equals(targetDefaultValue.toString()))) {
            if (log.isDebugEnabled()) {
                log.debug("The " + sourceColumn.getName() + " column on the "
                        + sourceTable.getName() + " table changed default value from "
                        + sourceColumn.getDefaultValue() + " to " + targetColumn.getDefaultValue());
            }
            changes.add(new ColumnDefaultValueChange(sourceTable, sourceColumn, targetColumn
                    .getDefaultValue()));
        }

        if (sourceColumn.isRequired() != targetColumn.isRequired()) {
            if (log.isDebugEnabled()) {
                log.debug("The " + sourceColumn.getName() + " column on the "
                        + sourceTable.getName() + " table changed required status from "
                        + sourceColumn.isRequired() + " to " + targetColumn.isRequired());
            }
            changes.add(new ColumnRequiredChange(sourceTable, sourceColumn));
        }

        if (sourceColumn.isAutoIncrement() != targetColumn.isAutoIncrement()) {
            if (log.isDebugEnabled()) {
                log.debug("The " + sourceColumn.getName() + " column on the "
                        + sourceTable.getName() + " table changed auto increment status from "
                        + sourceColumn.isAutoIncrement() + " to " + targetColumn.isAutoIncrement());
            }
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
