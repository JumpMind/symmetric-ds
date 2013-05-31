package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.sql.Time;
import java.util.Date;

import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.util.AppUtils;

/**
 * 
 */
public class NodeGroupChannelWindow implements Serializable {

    private static final long serialVersionUID = 1L;

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