package org.jumpmind.util;

import java.util.LinkedList;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class BufferedLogAppender extends AppenderSkeleton {

    protected LinkedList<LoggingEvent> events = new LinkedList<LoggingEvent>();
    
    protected int size = 100;
    
    @Override
    protected void append(LoggingEvent event) {
        events.addLast(event);
        if (events.size() > size) {
            events.removeFirst();
        }
    }

    public void close() {
    }

    public boolean requiresLayout() {
        return false;
    }
    
    public void setSize(int size) {
        this.size = size;
    }
    
    public int getSize() {
        return size;
    }
    
    public LinkedList<LoggingEvent> getEvents() {
        return events;
    }
}
