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
