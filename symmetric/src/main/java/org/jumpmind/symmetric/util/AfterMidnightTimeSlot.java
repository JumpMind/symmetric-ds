package org.jumpmind.symmetric.util;

import java.util.Calendar;

import org.apache.commons.lang.time.DateUtils;

public class AfterMidnightTimeSlot {

    int minutesAfterMidnight;

    public long getMillisecondsUntilTargetTime() {
        Calendar midnight = DateUtils.truncate(Calendar.getInstance(),
                Calendar.DATE);
        midnight.add(Calendar.DATE, 1);
        return midnight.getTime().getTime() - System.currentTimeMillis()
                + minutesAfterMidnight * DateUtils.MILLIS_PER_MINUTE;
    }

    public void setMinutesAfterMidnight(int minutesAfterMidnight) {
        this.minutesAfterMidnight = minutesAfterMidnight;
    }
}
