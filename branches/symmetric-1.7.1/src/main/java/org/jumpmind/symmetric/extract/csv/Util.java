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

package org.jumpmind.symmetric.extract.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.csv.CsvConstants;

public class Util {

    static final Log logger = LogFactory.getLog(Util.class);
    
    public static final String DELIMITER = ", ";
    
    public static void write(BufferedWriter writer, String... data) throws IOException {
        StringBuilder buffer = new StringBuilder();
        for (String string : data) {
            buffer.append(string);
        }

        writer.write(buffer.toString());
        if (logger.isDebugEnabled()) {
            logger.debug("writing: " + buffer);
        }
    }
    
    public static void writeSql(String sql, BufferedWriter writer) throws IOException {
        Util.write(writer, CsvConstants.SQL, DELIMITER, sql);
        writer.newLine();
    }
}
