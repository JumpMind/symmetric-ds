package org.jumpmind.symmetric.extract.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.model.Data;

public class StreamCreateDataCommand extends AbstractStreamDataCommand {

    public void execute(BufferedWriter writer, Data data, DataExtractorContext context) throws IOException {
        Util.write(writer, CsvConstants.CREATE, DELIMITER, data.getRowData());
        writer.newLine();
    }
}
