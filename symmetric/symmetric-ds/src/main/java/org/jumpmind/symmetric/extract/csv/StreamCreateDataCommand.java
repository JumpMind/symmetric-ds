package org.jumpmind.symmetric.extract.csv;

import java.io.Writer;
import java.io.IOException;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.util.CsvUtils;

public class StreamCreateDataCommand extends AbstractStreamDataCommand {

    public void execute(Writer writer, Data data, String routerId, DataExtractorContext context) throws IOException {
        CsvUtils.write(writer, CsvConstants.CREATE, DELIMITER, data.getRowData());
        CsvUtils.writeLineFeed(writer);
        context.incrementDataEventCount();
    }
    
    public boolean isTriggerHistoryRequired() {
        return false;
    }
}
