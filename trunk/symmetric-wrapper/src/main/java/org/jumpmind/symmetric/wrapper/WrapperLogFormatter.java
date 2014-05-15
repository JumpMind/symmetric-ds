package org.jumpmind.symmetric.wrapper;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class WrapperLogFormatter extends Formatter {

    protected static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,S");

    protected static final String NEWLINE = System.getProperty("line.separator");

    @Override
    public String format(LogRecord record) {
        String[] classname = record.getSourceClassName().split("\\.");
        return DATE_FORMATTER.format(new Date(record.getMillis())) + " "
                + record.getLevel().getName() + " [" + classname[classname.length - 1] + "] ["
                + record.getThreadID() + "] " + record.getMessage() + NEWLINE;
    }

}