package org.jumpmind.symmetric.transform;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.impl.AbstractService;
import org.jumpmind.symmetric.transform.TransformColumn.IncludeOnType;
import org.springframework.jdbc.core.RowMapper;

public class TransformService extends AbstractService implements ITransformService {

    private Map<String, Map<String, List<TransformTable>>> transformCacheByNodeGroupId;

    private long lastCacheTimeInMs;

    public Map<String, List<TransformTable>> findTransformsFor(String nodeGroupId, boolean useCache) {
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRANSFORM_IN_MS);

        if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                || transformCacheByNodeGroupId == null) {
            synchronized (this) {
                if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                        || transformCacheByNodeGroupId == null) {
                    if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                            || transformCacheByNodeGroupId == null) {
                        transformCacheByNodeGroupId = new HashMap<String, Map<String, List<TransformTable>>>();
                        List<TransformColumn> columns = jdbcTemplate.query(
                                getSql("selectTransformColumn"), new TransformColumnMapper());
                        List<TransformTable> transforms = jdbcTemplate.query(
                                getSql("selectTransformTable"), new TransformTableMapper());
                        for (TransformTable transformTable : transforms) {
                            Map<String, List<TransformTable>> map = transformCacheByNodeGroupId
                                    .get(transformTable.getTargetNodeGroupId());
                            if (map == null) {
                                map = new HashMap<String, List<TransformTable>>();
                                transformCacheByNodeGroupId.put(
                                        transformTable.getTargetNodeGroupId(), map);
                            }
                            List<TransformTable> tables = map.get(transformTable
                                    .getFullyQualifiedSourceTableName());
                            if (tables == null) {
                                tables = new ArrayList<TransformTable>();
                                map.put(transformTable.getFullyQualifiedSourceTableName(), tables);
                            }
                            tables.add(transformTable);

                            for (TransformColumn column : columns) {
                                if (column.getTransformId().equals(transformTable.getTransformId())) {
                                    transformTable.addTransformColumn(column);
                                }
                            }
                        }
                        lastCacheTimeInMs = System.currentTimeMillis();
                    }
                }
            }
        }

        return transformCacheByNodeGroupId != null ? transformCacheByNodeGroupId.get(nodeGroupId)
                : null;
    }

    class TransformTableMapper implements RowMapper<TransformTable> {
        public TransformTable mapRow(ResultSet rs, int rowNum) throws SQLException {
            TransformTable table = new TransformTable();
            table.setTransformId(rs.getString(1));
            table.setSourceCatalogName(rs.getString(2));
            table.setSourceSchemaName(rs.getString(3));
            table.setSourceTableName(rs.getString(4));
            table.setTargetCatalogName(rs.getString(5));
            table.setTargetSchemaName(rs.getString(6));
            table.setTargetTableName(rs.getString(7));
            table.setTargetNodeGroupId(rs.getString(8));
            table.setTransformOrder(rs.getInt(9));
            table.setUpdateFirst(rs.getBoolean(10));
            table.setDeleteAction(DeleteAction.valueOf(rs.getString(11)));
            return table;
        }
    }

    class TransformColumnMapper implements RowMapper<TransformColumn> {
        public TransformColumn mapRow(ResultSet rs, int rowNum) throws SQLException {
            TransformColumn col = new TransformColumn();
            col.setTransformId(rs.getString(1));
            col.setIncludeOn(IncludeOnType.decode(rs.getString(2)));
            col.setTargetColumnName(rs.getString(3));
            col.setSourceColumnName(rs.getString(4));
            col.setPk(rs.getBoolean(5));
            col.setTransformType(rs.getString(6));
            col.setTransformExpression(rs.getString(7));
            col.setTransformOrder(rs.getInt(8));
            return col;
        }
    }

}
