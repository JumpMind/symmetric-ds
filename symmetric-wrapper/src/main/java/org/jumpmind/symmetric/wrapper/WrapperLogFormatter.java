package org.jumpmind.symmetric.wrapper;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class WrapperLogFormatter extends Formatter {

    protected static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected static final String NEWLINE = System.getProperty("line.separator");

    @Override
    public String format(LogRecord record) {
        Object[] parms = record.getParameters();
        String source = "wrapper";
        if (parms != null && parms.length > 0) {
            source = parms[0].toString();
        }
        return DATE_FORMATTER.format(new Date(record.getMillis())) + " ["
                + String.format("%-7s", record.getLevel().getName()) + "] [" + String.format("%-7s", source) + "] "
                + record.getMessage() + NEWLINE;
    }

}