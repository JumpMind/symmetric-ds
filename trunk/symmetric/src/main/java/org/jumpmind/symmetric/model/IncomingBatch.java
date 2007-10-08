/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
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

package org.jumpmind.symmetric.model;

import java.io.Serializable;

import org.jumpmind.symmetric.load.IDataLoaderContext;

public class IncomingBatch implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        OK, ER;
    }

    private String batchId;

    private String clientId;

    private Status status;

    private boolean isRetry;

    public IncomingBatch() {
    }

    public IncomingBatch(IDataLoaderContext context) {
        batchId = context.getBatchId();
        clientId = context.getClientId();
        status = Status.OK;
    }
    
    public String getClientBatchId() {
        return clientId + "-" + batchId;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public boolean isRetry() {
        return isRetry;
    }

    public void setRetry(boolean isRetry) {
        this.isRetry = isRetry;
    }

}
