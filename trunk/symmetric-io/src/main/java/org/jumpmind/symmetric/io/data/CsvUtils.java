package org.jumpmind.symmetric.io.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvUtils {

    static final Logger log = LoggerFactory.getLogger(CsvUtils.class);

    public static final String DELIMITER = ", ";

    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static CsvReader getCsvReader(Reader reader) {
        CsvReader csvReader = new CsvReader(reader);
        csvReader.setEscapeMode(CsvReader.ESCAPE_MODE_BACKSLASH);
        csvReader.setSafetySwitch(false);
        csvReader.setCaptureRawRecord(false);
        return csvReader;
    }

    public static String[] tokenizeCsvData(String csvData) {
        String[] tokens = null;
        if (csvData != null) {
            CsvReader csvReader = getCsvReader(new StringReader(csvData));
            try {
                if (csvReader.readRecord()) {
                    tokens = csvReader.getValues();
                }
            } catch (IOException e) {
            }
        }
        return tokens;
    }

    /**
     * This escapes backslashes but doesn't wrap the data in a text qualifier.
     */
    public static String escapeCsvData(String data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
        writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
        try {
            writer.write(data);
            writer.close();
            out.close();
        } catch (IOException e) {
        }
        return out.toString();
    }

    public static String escapeCsvData(String[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        
        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
        writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
        writer.setTextQualifier('\"');
        writer.setUseTextQualifier(true);
        writer.setForceQualifier(true);
        for (String s : data) {
            try {
                writer.write(s, true);
            } catch (IOException e) {
                throw new IoException();
            }
        }
        writer.close();
        return out.toString();
    }
    
    public static String escapeCsvData(String[] data, char recordDelimiter, char textQualifier) {
        return escapeCsvData(data, recordDelimiter, textQualifier, CsvWriter.ESCAPE_MODE_BACKSLASH);
    }
    
    public static String escapeCsvData(String[] data, char recordDelimiter, char textQualifier, int escapeMode) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
        writer.setEscapeMode(escapeMode);
        writer.setRecordDelimiter(recordDelimiter);
        writer.setTextQualifier(textQualifier);
        writer.setUseTextQualifier(true);
        writer.setForceQualifier(true);
        try {
            writer.writeRecord(data);
        } catch (IOException e) {
            throw new IoException();
        }
        writer.close();
        return out.toString();
    }    

    public static int write(Writer writer, String... data) {
        try {
            StringBuilder buffer = new StringBuilder();
            for (String string : data) {
                buffer.append(string);
            }

            writer.write(buffer.toString());
            if (log.isDebugEnabled()) {
                log.debug(buffer.toString());
            }
            return buffer.length();
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public static void writeSql(String sql, Writer writer) {
        write(writer, CsvConstants.SQL, DELIMITER, sql);
        writeLineFeed(writer);
    }

    public static void writeBsh(String script, Writer writer) {
        write(writer, CsvConstants.BSH, DELIMITER, script);
        writeLineFeed(writer);
    }

    public static void writeLineFeed(Writer writer) {
        try {
            writer.write(LINE_SEPARATOR);
        } catch (IOException ex) {
            throw new IoException(ex);
        }

    }

}
