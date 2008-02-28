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
package org.jumpmind.symmetric;

import java.io.Serializable;

import org.jumpmind.symmetric.model.DataEventAction;

public class DataEvent implements Serializable {

    private static final long serialVersionUID = -7068281685538754464L;

    DataEventAction eventSource;
    
    private int batchId;

    private String channelId;

    private boolean error;
    
    private boolean offline;

    private int numberOfRowsAffected;

    public DataEvent() {
    }

    public DataEvent(DataEventAction eventSource, int batchId, String channelId, boolean error, int numberOfRowsAffected) {
        this.eventSource = eventSource;
        this.batchId = batchId;
        this.channelId = channelId;
        this.error = error;
        this.numberOfRowsAffected = numberOfRowsAffected;
    }

    public int getBatchId() {
        return batchId;
    }

    public void setBatchId(int batchId) {
        this.batchId = batchId;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    public int getNumberOfRowsAffected() {
        return numberOfRowsAffected;
    }

    public void setNumberOfRowsAffected(int numberOfRowsAffected) {
        this.numberOfRowsAffected = numberOfRowsAffected;
    }

    public DataEventAction getEventSource() {
        return eventSource;
    }

    public void setEventSource(DataEventAction eventSource) {
        this.eventSource = eventSource;
    }

    public boolean isOffline() {
        return offline;
    }

    public void setOffline(boolean offline) {
        this.offline = offline;
    }
}
