/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Andrew Wilcox <andrewbwilcox@users.sourceforge.net>
 *               Chris Henson <chenson42@users.sourceforge.net>
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

import java.io.Writer;
import java.io.IOException;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.util.CsvUtils;

class StreamUpdateDataCommand extends AbstractStreamDataCommand {

    public void execute(Writer out, Data data, String routerId, DataExtractorContext context) throws IOException {
        context.incrementByteCount(CsvUtils.write(out, CsvConstants.UPDATE, DELIMITER, data.getRowData(), DELIMITER, data.getPkData()));
        CsvUtils.writeLineFeed(out);
        context.incrementDataEventCount();
    }
    
    public boolean isTriggerHistoryRequired() {
        return true;
    }
}
