package org.jumpmind.symmetric.model;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.util.CsvUtils;

import com.csvreader.CsvReader;

abstract class AbstractCsvData {

    Map<String, String[]> parsedCsvData = new HashMap<String, String[]>(2);

    // TODO This could probably become more efficient
    protected String[] getData(String key, String data) {
        if (data != null) {
            try {
                if (parsedCsvData.containsKey(key)) {
                    return parsedCsvData.get(key);
                } else {
                    CsvReader csvReader = CsvUtils.getCsvReader(new StringReader(data));
                    if (csvReader.readRecord()) {
                        String[] values = csvReader.getValues();
                        parsedCsvData.put(key, values);
                        return values;
                    } else {
                        throw new IllegalStateException(String.format("Could not parse the data passed in: %s", data));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return null;
        }
    }

}
