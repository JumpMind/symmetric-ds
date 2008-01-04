/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.LockAction;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class PurgeService extends AbstractService implements IPurgeService
{

    private final static Log logger = LogFactory.getLog(PurgeService.class);

    private int maxNumOfDataIdsToPurgeInTx = 5000;

    private IDbDialect dbDialect;

    private String[] otherPurgeSql;

    private int retentionInMinutes = 7200;

    private String selectOutgoingBatchIdsToPurgeSql;

    private String deleteFromOutgoingBatchHistSql;

    private String deleteFromOutgoingBatchSql;

    private String selectEventDataIdToPurgeSql;

    private String deleteDataEventSql;

    private String selectDataIdToPurgeSql;

    private String deleteDataSql;

    private TransactionTemplate transactionTemplate;

    private IClusterService clusterService;

    @SuppressWarnings("unchecked")
    public void purge()
    {
        if (clusterService.lock(LockAction.PURGE))
        {
            try
            {
                logger.info("The purge process is about to run.");
                final Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.MINUTE, -retentionInMinutes);

                purgeBatchesOlderThan(calendar);
                purgeDataRows();

                for (final String sql : otherPurgeSql)
                {
                    final int count = jdbcTemplate.update(sql, new Object[] {calendar.getTime()});
                    if (count > 0)
                    {
                        logger.info("Purged " + count + " rows after running: " + cleanSql(sql));
                    }
                }
            }
            finally
            {
                clusterService.unlock(LockAction.PURGE);
                logger.info("The purge process has completed.");
            }

        }
        else
        {
            logger.info("Could not get a lock to run a purge.");
        }
    }

    private void purgeDataRows()
    {
        int dataIdCount = 0;
        do
        {
            dataIdCount = (Integer) transactionTemplate.execute(new TransactionCallback()
            {
                public Object doInTransaction(final TransactionStatus s)
                {
                    int count = 0;
                    List<Integer> dataIds = null;
                    dataIds = getNextDataIds(selectDataIdToPurgeSql, null, maxNumOfDataIdsToPurgeInTx);
                    for (final Integer dataId : dataIds)
                    {
                        count += jdbcTemplate.update(deleteDataSql, new Object[] {dataId});
                    }
                    if (count > 0)
                    {
                        logger.info("Purged " + count + " data rows.");
                    }
                    return dataIds.size();
                }
            });
        }
        while (dataIdCount > 0);

    }

    @SuppressWarnings("unchecked")
    private void purgeBatchesOlderThan(final Calendar time)
    {
        // Iterate over batch ids and data events to access by primary key so we prevent lock escalation
        final List<Integer> batchIds = jdbcTemplate.queryForList(selectOutgoingBatchIdsToPurgeSql, new Object[] {time
            .getTime()}, Integer.class);
        int eventRowCount = 0;
        int dataIdCount = 0;
        for (final Integer batchId : batchIds)
        {
            do
            {
                dataIdCount = (Integer) transactionTemplate.execute(new TransactionCallback()
                {
                    public Object doInTransaction(final TransactionStatus s)
                    {
                        jdbcTemplate.update(deleteFromOutgoingBatchHistSql, new Object[] {batchId});

                        int eventCount = 0;
                        List<Integer> dataIds = null;
                        dataIds = getNextDataIds(selectEventDataIdToPurgeSql, new Object[] {batchId},
                            maxNumOfDataIdsToPurgeInTx);

                        for (final Integer dataId : dataIds)
                        {
                            eventCount += jdbcTemplate.update(deleteDataEventSql, new Object[] {dataId, batchId});
                        }

                        jdbcTemplate.update(deleteFromOutgoingBatchSql, new Object[] {batchId});
                        return eventCount;
                    }
                });
                eventRowCount += dataIdCount;
            }
            while (dataIdCount > 0);

        }

        if (batchIds.size() > 0)
        {
            logger.info("Purged " + batchIds.size() + " batches and " + eventRowCount + " data_events.");
        }
    }

    /**
     * Select data ids using a streaming results set so we don't pull too much data into memory.
     */
    @SuppressWarnings("unchecked")
    private List<Integer> getNextDataIds(final String sql, final Object[] args, final int maxNumberToReturn)
    {
        return (List<Integer>) jdbcTemplate.execute(new ConnectionCallback()
        {
            public Object doInConnection(final Connection conn) throws SQLException, DataAccessException
            {
                final List<Integer> dataIds = new ArrayList<Integer>();
                final PreparedStatement st = conn.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
                if (args != null)
                {
                    for (int i = 1; i <= args.length; i++)
                    {
                        st.setObject(i, args[i - 1]);
                    }
                }
                st.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                final ResultSet rs = st.executeQuery();
                for (int i = 0; i < 10000 && rs.next(); i++)
                {
                    dataIds.add(rs.getInt(1));
                }
                JdbcUtils.closeResultSet(rs);
                JdbcUtils.closeStatement(st);

                return dataIds;
            }
        });
    }

    private String cleanSql(final String sql)
    {
        return StringUtils.replace(StringUtils.replace(StringUtils.replace(sql, "\r", " "), "\n", " "), "  ", "");
    }

    public void setOtherPurgeSql(final String[] purgeSql)
    {
        this.otherPurgeSql = purgeSql;
    }

    public void setRetentionInMinutes(final int retentionInMinutes)
    {
        this.retentionInMinutes = retentionInMinutes;
    }

    public void setDbDialect(final IDbDialect dbDialect)
    {
        this.dbDialect = dbDialect;
    }

    public void setSelectOutgoingBatchIdsToPurgeSql(final String selectOutgoingBatchIdsToPurgeSql)
    {
        this.selectOutgoingBatchIdsToPurgeSql = selectOutgoingBatchIdsToPurgeSql;
    }

    public void setDeleteFromOutgoingBatchHistSql(final String deleteFromOutgoingBatchHistSql)
    {
        this.deleteFromOutgoingBatchHistSql = deleteFromOutgoingBatchHistSql;
    }

    public void setDeleteFromOutgoingBatchSql(final String deleteFromOutgoingBatchSql)
    {
        this.deleteFromOutgoingBatchSql = deleteFromOutgoingBatchSql;
    }

    public void setSelectEventDataIdToPurgeSql(final String selectDataIdToPurgeSql)
    {
        this.selectEventDataIdToPurgeSql = selectDataIdToPurgeSql;
    }

    public void setDeleteDataEventSql(final String deleteDataEventSql)
    {
        this.deleteDataEventSql = deleteDataEventSql;
    }

    public void setDeleteDataSql(final String deleteDataSql)
    {
        this.deleteDataSql = deleteDataSql;
    }

    public void setTransactionTemplate(final TransactionTemplate transactionTemplate)
    {
        this.transactionTemplate = transactionTemplate;
    }

    public void setSelectDataIdToPurgeSql(final String selectDataIdToDeleteSql)
    {
        this.selectDataIdToPurgeSql = selectDataIdToDeleteSql;
    }

    public void setClusterService(final IClusterService clusterService)
    {
        this.clusterService = clusterService;
    }

    public void setMaxNumOfDataIdsToPurgeInTx(final int maxNumOfDataIdsToPurgeInTx)
    {
        this.maxNumOfDataIdsToPurgeInTx = maxNumOfDataIdsToPurgeInTx;
    }

}
