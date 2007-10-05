package org.jumpmind.symmetric.extract.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.service.IDataExtractorService;

class StreamReloadDataCommand extends AbstractStreamDataCommand {
        
    @SuppressWarnings("unused")
    private IDataExtractorService dataExtractorService;
    
    public void execute(BufferedWriter out, Data data) throws IOException {
        
//        TriggerHistory hist = data.getAudit();
//        dataExtractorService.extractInitialLoadFor(client, config, new InternalOutgoingTransport(out));
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }
    
}