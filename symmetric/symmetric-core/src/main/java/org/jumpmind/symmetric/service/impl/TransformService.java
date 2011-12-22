package org.jumpmind.symmetric.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.transform.DeleteAction;
import org.jumpmind.symmetric.io.data.transform.TransformColumn;
import org.jumpmind.symmetric.io.data.transform.TransformColumn.IncludeOnType;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.ITransformService;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

public class TransformService extends AbstractService implements ITransformService {

    private Map<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>> transformsCacheByNodeGroupLinkByTransformPoint;

    private long lastCacheTimeInMs;

    public List<TransformTableNodeGroupLink> findTransformsFor(NodeGroupLink nodeGroupLink,
            TransformPoint transformPoint, boolean useCache) {

        // get the cache timeout
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRANSFORM_IN_MS);

        // if the cache is expired or the caller doesn't want to use the cache,
        // pull the data and refresh the cache
        synchronized (this) {
            if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                    || transformsCacheByNodeGroupLinkByTransformPoint == null || useCache == false) {
                refreshCache();
            }
        }

        if (transformsCacheByNodeGroupLinkByTransformPoint != null) {
            Map<TransformPoint, List<TransformTableNodeGroupLink>> byTransformPoint = transformsCacheByNodeGroupLinkByTransformPoint
                    .get(nodeGroupLink);
            if (byTransformPoint != null) {
                return byTransformPoint.get(transformPoint);
            }
        }
        return null;
    }

    public void resetCache() {
        synchronized (this) {
            this.transformsCacheByNodeGroupLinkByTransformPoint = null;
        }
    }

    protected void refreshCache() {

        // get the cache timeout
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRANSFORM_IN_MS);

        synchronized (this) {
            if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                    || transformsCacheByNodeGroupLinkByTransformPoint == null) {

                transformsCacheByNodeGroupLinkByTransformPoint = new HashMap<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>>();

                List<TransformTableNodeGroupLink> transforms = getTransformTablesFromDB();

                for (TransformTableNodeGroupLink transformTable : transforms) {
                    NodeGroupLink nodeGroupLink = transformTable.getNodeGroupLink();
                    Map<TransformPoint, List<TransformTableNodeGroupLink>> byTransformPoint = transformsCacheByNodeGroupLinkByTransformPoint
                            .get(nodeGroupLink);
                    if (byTransformPoint == null) {
                        byTransformPoint = new HashMap<TransformPoint, List<TransformTableNodeGroupLink>>();
                        transformsCacheByNodeGroupLinkByTransformPoint.put(nodeGroupLink,
                                byTransformPoint);
                    }

                    List<TransformTableNodeGroupLink> byTableName = byTransformPoint
                            .get(transformTable.getTransformPoint());
                    if (byTableName == null) {
                        byTableName = new ArrayList<TransformTableNodeGroupLink>();
                        byTransformPoint.put(transformTable.getTransformPoint(), byTableName);
                    }

                    byTableName.add(transformTable);
                }
                lastCacheTimeInMs = System.currentTimeMillis();
            }
        }
    }

    private List<TransformTableNodeGroupLink> getTransformTablesFromDB() {
        List<TransformTableNodeGroupLink> transforms = jdbcTemplate.query(
                getSql("selectTransformTable"), new TransformTableMapper());
        List<TransformColumn> columns = getTransformColumnsFromDB();
        for (TransformTableNodeGroupLink transformTable : transforms) {
            for (TransformColumn column : columns) {
                if (column.getTransformId().equals(transformTable.getTransformId())) {
                    transformTable.addTransformColumn(column);
                }

            }
        }
        return transforms;
    }

    private List<TransformColumn> getTransformColumnsFromDB() {
        List<TransformColumn> columns = jdbcTemplate.query(getSql("selectTransformColumn"),
                new TransformColumnMapper());
        return columns;
    }

    public List<TransformTableNodeGroupLink> getTransformTables() {
        return this.getTransformTablesFromDB();
    }

    public List<TransformColumn> getTransformColumns() {
        return this.getTransformColumnsFromDB();
    }

    public List<TransformColumn> getTransformColumnsForTable() {
        List<TransformColumn> columns = jdbcTemplate.query(getSql("selectTransformColumnForTable"),
                new TransformColumnMapper());
        return columns;
    }

    @Transactional
    public void saveTransformTable(TransformTableNodeGroupLink transformTable) {
        if (jdbcTemplate.update(getSql("updateTransformTableSql"), transformTable
                .getNodeGroupLink().getSourceNodeGroupId(), transformTable.getNodeGroupLink()
                .getTargetNodeGroupId(), transformTable.getSourceCatalogName(), transformTable
                .getSourceSchemaName(), transformTable.getSourceTableName(), transformTable
                .getTargetCatalogName(), transformTable.getTargetSchemaName(), transformTable
                .getTargetTableName(), transformTable.getTransformPoint().toString(),
                transformTable.isUpdateFirst(), transformTable.getDeleteAction().toString(),
                transformTable.getTransformOrder(), transformTable.getTransformId()) == 0) {
            jdbcTemplate.update(getSql("insertTransformTableSql"), transformTable
                    .getNodeGroupLink().getSourceNodeGroupId(), transformTable.getNodeGroupLink()
                    .getTargetNodeGroupId(), transformTable.getSourceCatalogName(), transformTable
                    .getSourceSchemaName(), transformTable.getSourceTableName(), transformTable
                    .getTargetCatalogName(), transformTable.getTargetSchemaName(), transformTable
                    .getTargetTableName(), transformTable.getTransformPoint().toString(),
                    transformTable.isUpdateFirst(), transformTable.getDeleteAction().toString(),
                    transformTable.getTransformOrder(), transformTable.getTransformId());
        }
        deleteTransformColumns(transformTable.getTransformId());
        List<TransformColumn> columns = transformTable.getTransformColumns();
        if (columns != null) {
            for (TransformColumn transformColumn : columns) {
                saveTransformColumn(transformColumn);
            }
        }
        refreshCache();
    }

    public void deleteTransformColumns(String transformTableId) {
        jdbcTemplate.update(getSql("deleteTransformColumnsSql"), transformTableId);
    }

    public void deleteTransformTable(String transformTableId) {
        deleteTransformColumns(transformTableId);
        jdbcTemplate.update(getSql("deleteTransformTableSql"), transformTableId);
        refreshCache();
    }

    public void saveTransformColumn(TransformColumn transformColumn) {
        if (jdbcTemplate.update(getSql("updateTransformColumnSql"),
                transformColumn.getSourceColumnName(), transformColumn.isPk(),
                transformColumn.getTransformType(), transformColumn.getTransformExpression(),
                transformColumn.getTransformOrder(), transformColumn.getTransformId(),
                transformColumn.getIncludeOn().toDbValue(), transformColumn.getTargetColumnName()) == 0) {
            jdbcTemplate.update(getSql("insertTransformColumnSql"),
                    transformColumn.getTransformId(), transformColumn.getIncludeOn().toDbValue(),
                    transformColumn.getTargetColumnName(), transformColumn.getSourceColumnName(),
                    transformColumn.isPk(), transformColumn.getTransformType(),
                    transformColumn.getTransformExpression(), transformColumn.getTransformOrder());
        }
    }

    public void deleteTransformColumn(String transformTableId, Boolean includeOn,
            String targetColumnName) {

        String includeOnAsChar = null;
        // TODO: is this a "Y" or "N" or "1" or "0"
        if (includeOn)
            includeOnAsChar = "Y";
        else
            includeOnAsChar = "N";

        jdbcTemplate.update(getSql("deleteTransformColumnSql"), transformTableId, includeOnAsChar,
                targetColumnName);
        refreshCache();
    }

    class TransformTableMapper implements RowMapper<TransformTableNodeGroupLink> {
        public TransformTableNodeGroupLink mapRow(ResultSet rs, int rowNum) throws SQLException {
            TransformTableNodeGroupLink table = new TransformTableNodeGroupLink();
            table.setTransformId(rs.getString(1));
            table.setNodeGroupLink(new NodeGroupLink(rs.getString(2), rs.getString(3)));
            table.setSourceCatalogName(rs.getString(4));
            table.setSourceSchemaName(rs.getString(5));
            table.setSourceTableName(rs.getString(6));
            table.setTargetCatalogName(rs.getString(7));
            table.setTargetSchemaName(rs.getString(8));
            table.setTargetTableName(rs.getString(9));
            table.setTransformPoint(TransformPoint.valueOf(rs.getString(10)));
            table.setTransformOrder(rs.getInt(11));
            table.setUpdateFirst(rs.getBoolean(12));
            table.setDeleteAction(DeleteAction.valueOf(rs.getString(13)));
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

    public static class TransformTableNodeGroupLink extends TransformTable {

        protected NodeGroupLink nodeGroupLink;

        public void setNodeGroupLink(NodeGroupLink nodeGroupLink) {
            this.nodeGroupLink = nodeGroupLink;
        }

        public NodeGroupLink getNodeGroupLink() {
            return nodeGroupLink;
        }
    }
}
