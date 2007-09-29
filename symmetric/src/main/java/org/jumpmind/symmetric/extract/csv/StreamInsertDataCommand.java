package org.jumpmind.symmetric.extract.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.model.Data;

class StreamInsertDataCommand extends AbstractStreamDataCommand {

    public void execute(BufferedWriter writer, Data data) throws IOException {
        Util.write(writer, CsvConstants.INSERT, DELIMITER,data.getRowData());
        writer.newLine();
    }
}