package org.jumpmind.symmetric.web.rest.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BatchSummaries implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<BatchSummary> batchSummaries = new ArrayList<BatchSummary>();

    private String nodeId;

    public void setBatchSummaries(List<BatchSummary> batchSummaries) {
        this.batchSummaries = batchSummaries;
    }

    public List<BatchSummary> getBatchSummaries() {
        return batchSummaries;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeId() {
        return nodeId;
    }
}
