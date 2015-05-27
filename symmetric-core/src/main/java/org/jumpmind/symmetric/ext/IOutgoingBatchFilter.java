package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;

public interface IOutgoingBatchFilter extends IExtensionPoint {

    public List<OutgoingBatch> filter(NodeChannel channel, List<OutgoingBatch> batches);
    
}
