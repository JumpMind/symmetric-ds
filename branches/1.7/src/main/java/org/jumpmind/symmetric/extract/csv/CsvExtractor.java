/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 * Copyright (C) Andrew Wilcox <andrewbwilcox@users.sourceforge.net>
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

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.model.OutgoingBatch;

public class CsvExtractor extends CsvExtractor16 {

    public void init(BufferedWriter writer, DataExtractorContext context) throws IOException {
        super.init(writer, context);
        Util.write(writer, CsvConstants.BINARY, Util.DELIMITER, dbDialect.getBinaryEncoding().name());
        writer.newLine();
    }

    @Override
    public void begin(OutgoingBatch batch, BufferedWriter writer) throws IOException {
        Util.write(writer, CsvConstants.CHANNEL, Util.DELIMITER, batch.getChannelId());
        writer.newLine();
        Util.write(writer, CsvConstants.BATCH, Util.DELIMITER, Long.toString(batch.getBatchId()));
        writer.newLine();
    }

}
