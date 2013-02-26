package org.jumpmind.util;

import java.util.LinkedList;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class BufferedLogAppender extends AppenderSkeleton {

    protected LinkedList<LoggingEvent> events = new LinkedList<LoggingEvent>();

    protected int size = 100;

    protected String filterText;

    @Override
    protected void append(LoggingEvent event) {
        boolean addEvent = true;
        if (filterText != null) {
            String message = (String) event.getMessage();
            addEvent = message.contains(filterText);
            addEvent |= event.getLoggerName().contains(filterText);
            Object mdc = event.getMDC("engineName");
            if (mdc != null) {
                addEvent |= mdc.toString().contains(filterText);
            }
        }
        if (addEvent) {
            events.addLast(event);
            if (events.size() > size) {
                events.removeFirst();
            }
        }
    }

    public void close() {
    }

    public boolean requiresLayout() {
        return false;
    }

    public void setSize(int size) {
        this.size = size;
        while (events.size() > size) {
            events.remove(0);
        }
    }

    public int getSize() {
        return size;
    }

    public void setFilterText(String filterText) {
        if (StringUtils.isBlank(filterText)) {
            this.filterText = null;
        } else {
            this.filterText = filterText;
        }
    }

    public String getFilterText() {
        return filterText;
    }

    public LinkedList<LoggingEvent> getEvents() {
        return events;
    }
}
