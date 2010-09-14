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
package org.jumpmind.symmetric.model;

import java.sql.Time;
import java.util.Date;

import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.symmetric.util.AppUtils;

public class NodeGroupChannelWindow {

    private String nodeGroupId;
    private String channelId;
    private Time startTime;
    private Time endTime;
    private boolean enabled;
    final FastDateFormat HHmmss = FastDateFormat.getInstance("HH:mm:ss");

    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(String nodeGroupId) {
        this.nodeGroupId = nodeGroupId;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public Time getStartTime() {
        return startTime;
    }

    public void setStartTime(Time startTime) {
        this.startTime = startTime;
    }

    public Time getEndTime() {
        return endTime;
    }

    public void setEndTime(Time endTime) {
        this.endTime = endTime;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean inTimeWindow(Date date) {
        Time time = Time.valueOf(HHmmss.format(date));
        return inTimeWindow(time);
    }

    public boolean inTimeWindow(Time time) {
        if (enabled) {
            return (startTime.before(time) && endTime.after(time))
                    || (startTime.before(time) && endTime.before(startTime))
                    || (endTime.after(time) && startTime.after(endTime));
        } else {
            return true;
        }
    }

    public boolean inTimeWindow(String timezoneOffset) {
        return inTimeWindow(AppUtils.getLocalDateForOffset(timezoneOffset));
    }

}
