package org.jumpmind.symmetric.util;

import java.io.Reader;

import com.csvreader.CsvReader;

public class CsvUtils {

    public static CsvReader getCsvReader(Reader reader) {
        CsvReader csvReader = new CsvReader(reader);
        csvReader.setEscapeMode(CsvReader.ESCAPE_MODE_BACKSLASH);
        csvReader.setSafetySwitch(false);
        return csvReader;
    }
}
