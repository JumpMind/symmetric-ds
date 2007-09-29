package org.jumpmind.symmetric.extract;

import java.io.BufferedWriter;
import java.io.IOException;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.OutgoingBatch;

public interface IDataExtractor {

    public void init(BufferedWriter writer, DataExtractorContext context) throws IOException;
    
    public void begin(OutgoingBatch batch, BufferedWriter writer)
            throws IOException;
    
    public void preprocessTable(Data data, BufferedWriter out, DataExtractorContext context)
    throws IOException;

    public void commit(OutgoingBatch batch, BufferedWriter writer)
            throws IOException;

    public void write(BufferedWriter writer, Data data, DataExtractorContext context)
            throws IOException;

}
