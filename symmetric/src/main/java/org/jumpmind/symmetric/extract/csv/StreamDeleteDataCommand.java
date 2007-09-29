package org.jumpmind.symmetric.extract.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.model.Data;

class StreamDeleteDataCommand extends AbstractStreamDataCommand {

    public void execute(BufferedWriter out, Data data) throws IOException {
        Util.write(out, CsvConstants.DELETE, DELIMITER, data.getPkData());
        out.newLine();
    }
}
