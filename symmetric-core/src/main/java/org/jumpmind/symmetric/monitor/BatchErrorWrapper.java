package org.jumpmind.symmetric.monitor;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch;

public class BatchErrorWrapper implements Comparable {
    List<OutgoingBatch> outgoingErrors;
    List<IncomingBatch> incomingErrors;
    
    
    public BatchErrorWrapper() {
        this.outgoingErrors = new ArrayList<OutgoingBatch>();
        this.incomingErrors = new ArrayList<IncomingBatch>();
    }
    
    public BatchErrorWrapper(List<OutgoingBatch> outgoingBatch, List<IncomingBatch> incomingBatch) {
        this.outgoingErrors = outgoingBatch;
        this.incomingErrors = incomingBatch;
    }
    
    public List<OutgoingBatch> getOutgoingErrors() {
        return outgoingErrors;
    }
    public void setOutgoingErrors(List<OutgoingBatch> outgoingErrors) {
        this.outgoingErrors = outgoingErrors;
    }
    public List<IncomingBatch> getIncomingErrors() {
        return incomingErrors;
    }
    public void setIncomingErrors(List<IncomingBatch> incomingErrors) {
        this.incomingErrors = incomingErrors;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
    
    
}
