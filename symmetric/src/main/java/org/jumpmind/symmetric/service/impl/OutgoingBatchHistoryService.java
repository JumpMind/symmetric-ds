package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.service.IOutgoingBatchHistoryService;

public class OutgoingBatchHistoryService extends AbstractService implements
        IOutgoingBatchHistoryService {
    
    private String createdSql;

    private String errorSql;

    private String okSql;

    private String sentSql;

    public void created(final int batchId, final int eventCount) {
        jdbcTemplate.update(createdSql, new Object[] { batchId, eventCount });
    }

    public void error(final int batchId, final long failedDataId) {
        jdbcTemplate.update(errorSql, new Object[] { batchId, failedDataId });
    }

    public void ok(final int batchId) {
        jdbcTemplate.update(okSql, new Object[] { batchId });
    }

    public void sent(final int batchId) {
        jdbcTemplate.update(sentSql, new Object[] { batchId });
    }

    public void setCreatedSql(String createdSql) {
        this.createdSql = createdSql;
    }

    public void setErrorSql(String errorSql) {
        this.errorSql = errorSql;
    }

    public void setOkSql(String okSql) {
        this.okSql = okSql;
    }

    public void setSentSql(String sentSql) {
        this.sentSql = sentSql;
    }

}
