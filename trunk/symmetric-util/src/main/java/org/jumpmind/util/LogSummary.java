package org.jumpmind.util;

import org.apache.log4j.Level;

public class LogSummary implements Comparable<LogSummary> {

    private Level level;

    private String mostRecentThreadName;

    private Throwable throwable;

    private long firstOccurranceTime;

    private long mostRecentTime;

    private int count;

    private String message;

    public void setLevel(Level level) {
        this.level = level;
    }

    public Level getLevel() {
        return level;
    }

    public long getFirstOccurranceTime() {
        return firstOccurranceTime;
    }

    public void setFirstOccurranceTime(long firstOccurranceDate) {
        this.firstOccurranceTime = firstOccurranceDate;
    }

    public long getMostRecentTime() {
        return mostRecentTime;
    }

    public void setMostRecentTime(long mostRecentDate) {
        this.mostRecentTime = mostRecentDate;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setMostRecentThreadName(String mostRecentThreadName) {
        this.mostRecentThreadName = mostRecentThreadName;
    }

    public String getMostRecentThreadName() {
        return mostRecentThreadName;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public int compareTo(LogSummary other) {
        if (mostRecentTime == other.mostRecentTime) {
            return 0;
        } else {
            return mostRecentTime > other.mostRecentTime ? 1 : -1;
        }
    }
}
