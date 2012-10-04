package org.jumpmind.util;

import java.util.HashMap;
import java.util.Map;

public class Statistics {

    Map<String, Long> stats = new HashMap<String, Long>();

    Map<String, Long> timers = new HashMap<String, Long>();

    public void increment(String category) {
        increment(category, 1);
    }
    
    public long get(String category) {
        Long value = stats.get(category);
        if (value != null) {
            return value;
        } else {
            return 0l;
        }
    } 
    
    public void set(String category, long value) {
        stats.put(category, value);
    }

    public void increment(String category, long increment) {
        Long value = stats.get(category);
        if (value == null) {
            value = increment;
        } else {
            value = value + increment;
        }
        stats.put(category, value);
    }

    public void startTimer(String category) {
        timers.put(category, System.currentTimeMillis());
    }

    public long stopTimer(String category) {
        long time = 0;
        Long startTime = timers.get(category);
        if (startTime != null) {
            time = System.currentTimeMillis() - startTime;
            increment(category, time);
        }
        timers.remove(category);
        return time;
    }

}
