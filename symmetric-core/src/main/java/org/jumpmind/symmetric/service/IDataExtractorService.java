package org.jumpmind.symmetric.service;

import java.io.OutputStream;
import java.io.Writer;
import java.util.List;

import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

/**
 * This service provides an API to extract and stream data from a source database.
 */
public interface IDataExtractorService {

    public void extractConfigurationStandalone(Node node, OutputStream out);

    public void extractConfigurationStandalone(Node node, Writer out, String... tablesToIgnore);
    
    /**
     * @param processInfo TODO
     * @return a list of batches that were extracted
     */
    public List<OutgoingBatch> extract(ProcessInfo processInfo, Node node, IOutgoingTransport transport);

    public boolean extractBatchRange(Writer writer, String nodeId, long startBatchId, long endBatchId);
    
    public OutgoingBatch extractOutgoingBatch(ProcessInfo processInfo, Node targetNode,
            IDataWriter dataWriter, OutgoingBatch currentBatch,
            boolean streamToFileEnabled);

}