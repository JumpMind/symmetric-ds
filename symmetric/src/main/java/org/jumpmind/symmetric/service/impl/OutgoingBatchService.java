package org.jumpmind.symmetric.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IOutgoingBatchHistoryService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

public class OutgoingBatchService extends AbstractService implements
        IOutgoingBatchService {

    IConfigurationService configurationService;

    private String selectEventsToBatchSql;

    private String updateBatchedEventsSql;

    private String createBatchSql;

    private String selectOutgoingBatchSql;

    private String changeBatchStatusSql;
    
    private IOutgoingBatchHistoryService historyService;

    @Transactional
    public void buildOutgoingBatches(final String locationId) {

        // TODO clean this up (maybe break up into a couple different methods) and unit test still ...
        // This is very important code.  It will be called during the staggered pull and must scale.
        
        // TODO should channels be cached?
        final List<Channel> channels = configurationService
                .getChannelsFor(runtimeConfiguration
                        .getNodeGroupId(), true);
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException,
                    DataAccessException {
                PreparedStatement update = conn
                        .prepareStatement(updateBatchedEventsSql);

                for (Channel channel : channels) {
                    if (channel.isEnabled()) {
                        // determine which transactions will be part of this batch on this channel
                        PreparedStatement select = conn
                                .prepareStatement(selectEventsToBatchSql);
                        select.setString(1, locationId);
                        select.setString(2, channel.getId());
                        ResultSet results = select.executeQuery();

                        int count = 0;
                        boolean stopOnNextTxIdChange = false;
                        String lastTrxId = null;
                        
                        OutgoingBatch newBatch = new OutgoingBatch();
                        newBatch.setBatchType(BatchType.EVENTS);
                        newBatch.setChannelId(channel.getId());
                        newBatch.setClientId(locationId);
                        
                        if (results.next()) {          
                            
                            insertOutgoingBatch(conn, newBatch);
                            
                            do {
                                String trxId = results.getString(1);

                                if (stopOnNextTxIdChange
                                        && (lastTrxId == null || !lastTrxId
                                                .equals(trxId))) {
                                    break;
                                }

                                int dataId = results.getInt(2);

                                update.clearParameters();
                                update.setString(1, newBatch.getBatchId());
                                update.setString(2, locationId);
                                update.setInt(3, dataId);
                                update.addBatch();

                                count++;

                                if (count > channel.getMaxBatchSize()) {
                                    stopOnNextTxIdChange = true;
                                    count--;
                                }

                                lastTrxId = trxId;
                            } while (results.next());
                            historyService.created(new Integer(newBatch.getBatchId()), count);                            
                        } 
                    } 
                    update.executeBatch();
                }
                return null;
            }
        });
    }
    
    public void insertOutgoingBatch(final OutgoingBatch outgoingBatch) {
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException,
                    DataAccessException {
                insertOutgoingBatch(conn, outgoingBatch);
                return null;
            }
        });
    }
    
    private void insertOutgoingBatch(Connection conn, OutgoingBatch outgoingBatch) throws SQLException {
        // TODO: move generated key retrieval to DbDialect
        PreparedStatement insert = conn.prepareStatement(createBatchSql, new int[] { 1 });
        insert.setString(1, outgoingBatch.getClientId());
        insert.setString(2, outgoingBatch.getChannelId());
        insert.setString(3, outgoingBatch.getBatchType().getCode());
        insert.execute();
        ResultSet rs = insert.getGeneratedKeys();
        if (rs.next()) {
            outgoingBatch.setBatchId(rs.getString(1));
        } else {
            throw new RuntimeException(
                    "Unable to get batch id");
        }
        historyService.created(new Integer(outgoingBatch.getBatchId()), -1);
    }

    @SuppressWarnings("unchecked")
    public List<OutgoingBatch> getOutgoingBatches(String clientId) {
        return (List<OutgoingBatch>) jdbcTemplate.query(selectOutgoingBatchSql,
                new Object[] { clientId }, new RowMapper() {
                    public Object mapRow(ResultSet rs, int index)
                            throws SQLException {
                        OutgoingBatch batch = new OutgoingBatch();
                        batch.setBatchId(rs.getString(1));
                        batch.setClientId(rs.getString(2));
                        batch.setChannelId(rs.getString(3));
                        batch.setStatus(rs.getString(4));
                        batch.setBatchType(rs.getString(5));
                        return batch;
                    }
                });
    }

    public void markOutgoingBatchSent(OutgoingBatch batch) {
       setBatchStatus(batch.getBatchId(), Status.SE);
    }
    
    public void setBatchStatus(String batchId, Status status) {
        jdbcTemplate.update(changeBatchStatusSql, new Object[] {
                status.name(), batchId });
        
        if (status == Status.SE)
        {
            historyService.sent(new Integer(batchId));
        }
        else if (status == Status.ER) 
        {
            historyService.error(new Integer(batchId), 0L);
        } 
        else if (status == Status.OK)
        {
            historyService.ok(new Integer(batchId));
        }
        
    }

    public void setConfigurationService(
            IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setCreateBatchSql(String createBatchSql) {
        this.createBatchSql = createBatchSql;
    }

    public void setSelectEventsToBatchSql(String selectEventsToBatchSql) {
        this.selectEventsToBatchSql = selectEventsToBatchSql;
    }

    public void setUpdateBatchedEventsSql(String updateBatchedEventsSql) {
        this.updateBatchedEventsSql = updateBatchedEventsSql;
    }

    public void setSelectOutgoingBatchSql(String selectOutgoingBatch) {
        this.selectOutgoingBatchSql = selectOutgoingBatch;
    }

    public void setChangeBatchStatusSql(String markBatchSentSql) {
        this.changeBatchStatusSql = markBatchSentSql;
    }

    public void setHistoryService(IOutgoingBatchHistoryService historyService)
    {
        this.historyService = historyService;
    }

}
