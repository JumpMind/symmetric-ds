package org.jumpmind.symmetric.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class OutgoingBatches {

    List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>();
    Set<Channel> channels = new TreeSet<Channel>();

    public List<OutgoingBatch> getBatches() {
        return batches;
    }

    public void setBatches(List<OutgoingBatch> batches) {
        this.batches = batches;
    }

    public Set<Channel> getChannels() {
        return channels;
    }

    public void setChannels(Set<Channel> channels) {
        this.channels = channels;
    }

}
