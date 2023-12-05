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
package org.jumpmind.db.alter;

import java.math.BigDecimal;

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

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.platform.ase.AseDdlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares two database models and creates change objects that express how to adapt the first model so that it becomes the second one. Neither of the models
 * are changed in the process, however, it is also assumed that the models do not change in between.
 */
public class ModelComparator {
    /** The log for this comparator. */
    private final Logger log = LoggerFactory.getLogger(ModelComparator.class);
    /** The platform information. */
    protected DatabaseInfo platformInfo;
    /** Whether comparison is case sensitive. */
    protected boolean caseSensitive;
    protected IDdlBuilder ddlBuilder;

    /**
     * Creates a new model comparator object.
     * 
     * @param platformInfo
     *            The platform info
     * @param caseSensitive
     *            Whether comparison is case sensitive
     */
    public ModelComparator(IDdlBuilder ddlBuilder, DatabaseInfo platformInfo, boolean caseSensitive) {
        this.ddlBuilder = ddlBuilder;
        this.platformInfo = platformInfo;
        this.caseSensitive = caseSensitive;
    }
    
	private boolean supportsDefaultValues() {
		if (ddlBuilder instanceof AseDdlBuilder && ((AseDdlBuilder) ddlBuilder).isUsingJtds()) {
			return false;
		}
		return true;
	}

    /**
     * Compares the two models and returns the changes necessary to create the second model from the first one.
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
                 * we assume that the target model is sound, ie. that there are no longer any foreign keys to this table in the target model; thus we already
                 * have removeFK changes for these from the compareTables method and we only need to create changes for the fks originating from this table
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
     * Compares the two tables and returns the changes necessary to create the second table from the first one.
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
                     * we have to use the target table here because the foreign key might reference a new column
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
        Column[] sourcePK = sourceTable.getPrimaryKeyColumnsInIndexOrder();
        Column[] targetPK = targetTable.getPrimaryKeyColumnsInIndexOrder();
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
     * Compares the two columns and returns the changes necessary to create the second column from the first one.
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
        boolean compatible = (actualTypeCode == Types.NUMERIC || actualTypeCode == Types.DECIMAL) &&
                (desiredTypeCode == Types.INTEGER || desiredTypeCode == Types.BIGINT);
        if (sourceColumn.isAutoIncrement() && targetColumn.isAutoIncrement() &&
                (desiredTypeCode == Types.NUMERIC || desiredTypeCode == Types.DECIMAL) &&
                (actualTypeCode == Types.INTEGER || actualTypeCode == Types.BIGINT)) {
            compatible = true;
            // This is the rare case where size doesnt matter!
            sizeMatters = false;
            scaleMatters = false;
        }
        if (sourceColumn.getMappedTypeCode() == Types.BLOB && targetColumn.getMappedTypeCode() == Types.LONGVARCHAR) {
            // This is probably a conversion from CLOB to BLOB because the database collation is set to binary
            compatible = true;
        }
        if (!compatible && !ddlBuilder.areMappedTypesTheSame(sourceColumn, targetColumn)
                && platformInfo.getTargetJdbcType(targetColumn.getMappedTypeCode()) != platformInfo.getTargetJdbcType(sourceColumn.getMappedTypeCode())) {
            log.debug(
                    "The {} column on the {} table changed type codes from {} to {} ",
                    new Object[] { sourceColumn.getName(), sourceTable.getName(),
                            sourceColumn.getMappedTypeCode(), targetColumn.getMappedTypeCode() });
            changes.add(new ColumnDataTypeChange(sourceTable, sourceColumn, targetColumn
                    .getMappedTypeCode()));
        }
        if (!ddlBuilder.areColumnSizesTheSame(sourceColumn, targetColumn)) {
            if (sizeMatters) {
                int targetSize = targetColumn.getSizeAsInt();
                if (targetColumn.getSize() == null) {
                    Integer defaultSize = platformInfo.getDefaultSize(platformInfo.getTargetJdbcType(targetColumn.getMappedTypeCode()));
                    if (defaultSize != null) {
                        targetSize = defaultSize;
                    } else {
                        targetSize = 0;
                    }
                }
                log.debug("The {} column on the {} table changed size from ({}) to ({})", new Object[] { sourceColumn.getName(),
                        sourceTable.getName(), sourceColumn.getSizeAsInt(), targetSize });
                changes.add(new ColumnSizeChange(sourceTable, sourceColumn, targetSize, targetColumn.getScale()));
            } else if (scaleMatters) {
                log.debug("The {} column on the {} table changed scale from ({},{}) to ({},{})",
                        new Object[] { sourceColumn.getName(), sourceTable.getName(), sourceColumn.getSizeAsInt(), sourceColumn.getScale(),
                                targetColumn.getSizeAsInt(), targetColumn.getScale() });
                changes.add(new ColumnSizeChange(sourceTable, sourceColumn, targetColumn.getSizeAsInt(), targetColumn.getScale()));
            }
        }
		if (supportsDefaultValues() && !defaultValuesAreEqual(sourceColumn, targetColumn)) {
			log.debug("The {} column on the {} table changed default value from {} to {} ",
					new Object[] { sourceColumn.getName(), sourceTable.getName(), sourceColumn.getDefaultValue(),
							targetColumn.getDefaultValue() });
			changes.add(new ColumnDefaultValueChange(sourceTable, sourceColumn, targetColumn.getDefaultValue()));
		}
        if (!targetColumn.isGenerated() && sourceColumn.isRequired() != targetColumn.isRequired()) {
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
        if (sourceColumn.isGenerated() != targetColumn.isGenerated()) {
            log.debug(
                    "The {} column on the {} table changed generated status from {} to {} ",
                    new Object[] { sourceColumn.getName(), sourceTable.getName(),
                            sourceColumn.isGenerated(), targetColumn.isGenerated() });
            changes.add(new ColumnGeneratedChange(sourceTable, sourceColumn, targetColumn.getDefaultValue()));
        } else if (Boolean.valueOf(System.getProperty("compare.generated.column.definitions", "true"))
                && sourceColumn.isGenerated() && sourceColumn.getDefaultValue() != null
                && !sourceColumn.getDefaultValue().equals(targetColumn.getDefaultValue())) {
            log.debug(
                    "The {} generated column on the {} table changed definition from {} to {} ",
                    new Object[] { sourceColumn.getName(), sourceTable.getName(),
                            sourceColumn.getDefaultValue(), targetColumn.getDefaultValue() });
            changes.add(new GeneratedColumnDefinitionChange(sourceTable, sourceColumn, targetColumn
                    .getDefaultValue()));
        }
        return changes;
    }

    /**
     * Searches in the given table for a corresponding foreign key. If the given key has no name, then a foreign key to the same table with the same columns
     * (but not necessarily in the same order) is searched. If the given key has a name, then the corresponding key also needs to have the same name, or no name
     * at all, but not a different one.
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
     * Searches in the given table for a corresponding index. If the given index has no name, then a index to the same table with the same columns in the same
     * order is searched. If the given index has a name, then the a corresponding index also needs to have the same name, or no name at all, but not a different
     * one.
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

    private boolean defaultValuesAreEqual(Column sourceColumn, Column targetColumn) {
        Object sourceDefaultValue = sourceColumn.getParsedDefaultValue();
        Object targetDefaultValue = targetColumn.getParsedDefaultValue();
        if (!targetColumn.isGenerated() && !(sourceDefaultValue == null && targetDefaultValue == null)) {
            if ((sourceDefaultValue == null && targetDefaultValue != null)
                    || (sourceDefaultValue != null && targetDefaultValue == null)) {
                return false;
            }
            boolean isBigDecimal = sourceDefaultValue instanceof BigDecimal && targetDefaultValue instanceof BigDecimal;
            if ((isBigDecimal && ((BigDecimal) sourceDefaultValue).compareTo((BigDecimal) targetDefaultValue) != 0)) {
                return false;
            }
            if (!isBigDecimal) {
                String sourceDefaultValueString = sourceDefaultValue.toString();
                String targetDefaultValueString = ddlBuilder.mapDefaultValue(targetDefaultValue, targetColumn).toString();
                if (!sourceDefaultValueString.equals(targetDefaultValueString)) {
                    int typeCode = targetColumn.getMappedTypeCode();
                    if (typeCode == Types.TIMESTAMP || typeCode == ColumnTypes.TIMESTAMPTZ || typeCode == ColumnTypes.TIMESTAMPLTZ) {
                        if (targetColumn.anyPlatformColumnNameContains("mysql") || targetColumn.anyPlatformColumnNameContains("maria")) {
                            while (targetDefaultValueString.startsWith("(") && targetDefaultValueString.endsWith(")")) {
                                targetDefaultValueString = targetDefaultValueString.substring(1, targetDefaultValueString.length() - 1);
                            }
                        }
                        if (targetColumn.anyPlatformColumnNameContains("postgres")) {
                            sourceDefaultValueString = sourceDefaultValueString.replace("::text", "");
                        }
                        return sourceDefaultValueString.equalsIgnoreCase(targetDefaultValueString);
                    }
                    return false;
                }
            }
        }
        return true;
    }
}
