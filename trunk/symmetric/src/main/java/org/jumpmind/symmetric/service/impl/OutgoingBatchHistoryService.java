package org.jumpmind.symmetric.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.jumpmind.symmetric.service.IOutgoingBatchHistoryService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

public class OutgoingBatchHistoryService extends AbstractService implements IOutgoingBatchHistoryService
{
    private String createdSql;
    private String errorSql;
    private String okSql;
    private String sentSql;
    
    public void created(final int batchId, final int eventCount)
    {
        jdbcTemplate.execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException
            {
                PreparedStatement s = conn.prepareStatement(createdSql);
                s.setInt(1, batchId);
                s.setInt(2, eventCount);
                s.executeUpdate();
                return null;
            }
        });
    }

    public void error(final int batchId, final long failedDataId)
    {
        jdbcTemplate.execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException
            {
                PreparedStatement s = conn.prepareStatement(errorSql);
                s.setInt(1, batchId);
                s.setLong(2, failedDataId);
                s.executeUpdate();
                return null;
            }
        });
    }

    public void ok(final int batchId)
    {
        jdbcTemplate.execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException
            {
                PreparedStatement s = conn.prepareStatement(okSql);
                s.setInt(1, batchId);
                s.executeUpdate();
                return null;
            }
        });
    }

    public void sent(final int batchId)
    {
        jdbcTemplate.execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException
            {
                PreparedStatement s = conn.prepareStatement(sentSql);
                s.setInt(1, batchId);
                s.executeUpdate();
                return null;
            }
        });
    }
    

    public void setCreatedSql(String createdSql)
    {
        this.createdSql = createdSql;
    }

    public void setErrorSql(String errorSql)
    {
        this.errorSql = errorSql;
    }

    public void setOkSql(String okSql)
    {
        this.okSql = okSql;
    }

    public void setSentSql(String sentSql)
    {
        this.sentSql = sentSql;
    }

}
