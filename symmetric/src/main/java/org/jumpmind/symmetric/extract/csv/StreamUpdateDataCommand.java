package org.jumpmind.symmetric.extract.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.model.Data;

class StreamUpdateDataCommand extends AbstractStreamDataCommand {

    public void execute(BufferedWriter out, Data data) throws IOException {
        Util.write(out, CsvConstants.UPDATE, DELIMITER, data.getRowData(), DELIMITER, data.getPkData());
        out.newLine();
    }
}
