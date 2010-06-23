/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.model;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.util.CsvUtils;


abstract class AbstractCsvData {

    Map<String, String[]> parsedCsvData = new HashMap<String, String[]>(2);

    // TODO This could probably become more efficient
    protected String[] getData(String key, String data) {
        if (!StringUtils.isBlank(data)) {
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
