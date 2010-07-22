package org.jumpmind.symmetric.common.csv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class CsvUtil {

    public static String[] tokenizeCsvData(String csvData) {
        String[] tokens = null;
        if (csvData != null) {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(csvData.getBytes()));
            CsvReader csvReader = new CsvReader(reader);
            csvReader.setEscapeMode(CsvReader.ESCAPE_MODE_BACKSLASH);
            try {
                if (csvReader.readRecord()) {
                    tokens = csvReader.getValues();
                }
            } catch (IOException e) {
            }
        }
        return tokens;
    }

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
}
