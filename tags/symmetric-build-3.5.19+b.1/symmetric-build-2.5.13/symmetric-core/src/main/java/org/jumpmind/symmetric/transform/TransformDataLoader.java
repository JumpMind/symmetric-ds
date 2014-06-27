/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.symmetric.transform;

import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IMissingTableHandler;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.load.TableTemplate;

public class TransformDataLoader extends AbstractTransformer implements IBuiltInExtensionPoint,
        IDataLoaderFilter, IMissingTableHandler {

    public TransformDataLoader() {
    }

    @Override
    protected TransformPoint getTransformPoint() {
        return TransformPoint.LOAD;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        boolean processRow = true;
        if (isEligibleForTransform(context.getCatalogName(), context.getSchemaName(),
                context.getTableName())) {
            List<TransformedData> transformedData = transform(DmlType.INSERT, context,
                    context.getNodeGroupLink(), context.getCatalogName(), context.getSchemaName(),
                    context.getTableName(), context.getColumnNames(), columnValues, null, null,
                    null);
            if (transformedData != null) {
                apply(context, transformedData);
                processRow = false;
            }
        }
        return processRow;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues,
            String[] keyValues) {
        boolean processRow = true;
        if (isEligibleForTransform(context.getCatalogName(), context.getSchemaName(),
                context.getTableName())) {
            List<TransformedData> transformedData = transform(DmlType.UPDATE, context,
                    context.getNodeGroupLink(), context.getCatalogName(), context.getSchemaName(),
                    context.getTableName(), context.getColumnNames(), columnValues,
                    context.getKeyNames(), keyValues, context.getOldData());
            if (transformedData != null) {
                apply(context, transformedData);
                processRow = false;
            }
        }
        return processRow;
    }

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        String[] columnNames = context.getKeyNames();
        String[] columnValues = keyValues;
        if (context.getOldData() != null) {
            columnNames = context.getColumnNames();
            columnValues = context.getOldData();
        }
        boolean processRow = true;
        if (isEligibleForTransform(context.getCatalogName(), context.getSchemaName(),
                context.getTableName())) {
            List<TransformedData> transformedData = transform(DmlType.DELETE, context,
                    context.getNodeGroupLink(), context.getCatalogName(), context.getSchemaName(),
                    context.getTableName(), columnNames, columnValues, context.getKeyNames(),
                    keyValues, context.getOldData());
            if (transformedData != null) {
                apply(context, transformedData);
                processRow = false;
            }
        }
        return processRow;
    }

    protected void apply(IDataLoaderContext context,
            List<TransformedData> dataThatHasBeenTransformed) {
        for (TransformedData data : dataThatHasBeenTransformed) {
            TableTemplate tableTemplate = new TableTemplate(context.getJdbcTemplate(), dbDialect,
                    data.getTableName(), null, false, data.getSchemaName(), data.getCatalogName());
            tableTemplate.setColumnNames(data.getColumnNames());
            tableTemplate.setKeyNames(data.getKeyNames());
            switch (data.getTargetDmlType()) {
            case INSERT:
                Table table = tableTemplate.getTable();
                boolean attemptFallbackUpdate = false;
                RuntimeException insertException = null;
                try {
                    try {
                        if (data.isGeneratedIdentityNeeded()) {
                            if (log.isDebugEnabled()) {
                                log.debug("TransformEnablingGeneratedIdentity", table.getName());
                            }
                            dbDialect.revertAllowIdentityInserts(context.getJdbcTemplate(), table);
                        } else if (table.hasAutoIncrementColumn()) {
                            dbDialect.allowIdentityInserts(context.getJdbcTemplate(), table);
                        }

                        if (tableTemplate.insert(context, data.getColumnValues(),
                                data.getKeyValues()) == 0) {
                            attemptFallbackUpdate = parameterService
                                    .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_UPDATE);
                        }
                    } catch (RuntimeException ex) {
                        insertException = ex;
                        if (dbDialect.isPrimaryKeyViolation(ex)
                                && parameterService
                                        .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_UPDATE)) {
                            attemptFallbackUpdate = true;
                        } else {
                            throw ex;
                        }
                    }
                    if (attemptFallbackUpdate) {
                        List<TransformedData> newlyTransformedDatas = transform(DmlType.UPDATE,
                                context, data.getTransformation(), data.getSourceKeyValues(),
                                data.getOldSourceValues(), data.getSourceValues());
                        for (TransformedData newlyTransformedData : newlyTransformedDatas) {
                            if (newlyTransformedData.hasSameKeyValues(data.getKeyValues())
                                    || data.isGeneratedIdentityNeeded()) {
                                if (newlyTransformedData.getKeyNames() != null
                                        && newlyTransformedData.getKeyNames().length > 0) {
                                    tableTemplate.setColumnNames(newlyTransformedData
                                            .getColumnNames());
                                    tableTemplate.setKeyNames(newlyTransformedData.getKeyNames());
                                    if (0 == tableTemplate.update(context,
                                            newlyTransformedData.getColumnValues(),
                                            newlyTransformedData.getKeyValues())) {
                                        throw new SymmetricException("LoaderFallbackUpdateFailed",
                                                insertException, tableTemplate.getTable()
                                                        .toVerboseString(),
                                                ArrayUtils.toString(data.getColumnValues()),
                                                ArrayUtils.toString(data.getKeyValues()));
                                    }
                                } else {
                                    // If not keys are specified we are going to
                                    // assume that this is intentional and we
                                    // will simply log a warning and not fail.
                                    log.warn("Message", insertException.getMessage());
                                    log.warn("TransformNoPrimaryKeyDefinedNoUpdate",
                                            newlyTransformedData.getTransformation()
                                                    .getTransformId());
                                }
                            } else {
                                log.debug("TransformMatchingFallbackNotFound",
                                        DmlType.UPDATE.name());
                            }
                        }

                    }

                } finally {
                    if (table.hasAutoIncrementColumn()) {
                        dbDialect.revertAllowIdentityInserts(context.getJdbcTemplate(), table);
                    }
                }
                break;
            case UPDATE:
                boolean enableFallbackInsert = parameterService
                        .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_INSERT);
                if (data.getKeyNames() != null && data.getKeyNames().length > 0) {
                    if (0 == tableTemplate.update(context, data.getColumnValues(),
                            data.getKeyValues())
                            && (data.getSourceDmlType() != DmlType.DELETE)) {
                        if (enableFallbackInsert) {
                            List<TransformedData> newlyTransformedDatas = transform(DmlType.INSERT,
                                    context, data.getTransformation(), data.getSourceKeyValues(),
                                    data.getOldSourceValues(), data.getSourceValues());
                            for (TransformedData newlyTransformedData : newlyTransformedDatas) {
                                if (newlyTransformedData.hasSameKeyValues(data.getKeyValues()) ||
                                		newlyTransformedData.isGeneratedIdentityNeeded()) {
                                    if (newlyTransformedData.isGeneratedIdentityNeeded()) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("TransformEnablingGeneratedIdentity", tableTemplate.getTable().getName());
                                        }
                                        dbDialect.revertAllowIdentityInserts(context.getJdbcTemplate(), tableTemplate.getTable());
                                    } else if (tableTemplate.getTable().hasAutoIncrementColumn()) {
                                        dbDialect.allowIdentityInserts(context.getJdbcTemplate(), tableTemplate.getTable());
                                    }
                                    
                                    newlyTransformedData.setTargetDmlType(DmlType.INSERT);
                                    tableTemplate.setColumnNames(newlyTransformedData
                                            .getColumnNames());
                                    tableTemplate.setKeyNames(newlyTransformedData.getKeyNames());
                                    tableTemplate.insert(context, 
                                            newlyTransformedData.getColumnValues(), newlyTransformedData.getKeyValues());
                                } else {
                                	log.debug("TransformMatchingFallbackNotFound", DmlType.INSERT.name());
                                }
                            }
                        } else {
                            throw new SymmetricException("LoaderUpdatingFailed",
                                    context.getTableName(), ArrayUtils.toString(data
                                            .getColumnValues()));
                        }
                    }
                } else {
                    // If not keys are specified we are going to assume
                    // that this is intentional and we will simply log a
                    // warning and not fail.
                    log.warn("TransformNoPrimaryKeyDefinedNoUpdate", data.getTransformation()
                            .getTransformId());
                }
                break;
            case DELETE:
                boolean allowMissingDelete = parameterService
                        .is(ParameterConstants.DATA_LOADER_ALLOW_MISSING_DELETE);
                int rows = tableTemplate.delete(context, data.getKeyValues());
                if (rows == 0) {
                    if (allowMissingDelete) {
                        log.warn("LoaderDeleteMissing", context.getTableName(),
                                ArrayUtils.toString(data.getColumnValues()));
                    } else {
                        throw new SymmetricException("LoaderDeleteMissing", context.getTableName(),
                                ArrayUtils.toString(data.getColumnValues()));
                    }
                }
                break;
            }
        }
    }

	public boolean isHandlingMissingTable(IDataLoaderContext context) {
		if (isEligibleForTransform(context.getCatalogName(),
				context.getSchemaName(), context.getTableName())) {
			List<TransformTable> transformationsToPerform = findTablesToTransform(
					context.getNodeGroupLink(), context.getTableTemplate()
							.getFullyQualifiedTableName(true));
			return transformationsToPerform != null
					&& transformationsToPerform.size() > 0;
		} else {
			return false;
		}
	}

}
