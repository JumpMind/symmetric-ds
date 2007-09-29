package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.OutgoingBatch;

public interface IExtractListener {
    public void init() throws Exception;

    public void startBatch(OutgoingBatch batch) throws Exception;

    public void endBatch(OutgoingBatch batch) throws Exception;

    public void dataExtracted(Data data) throws Exception;

    public void done() throws Exception;
}
