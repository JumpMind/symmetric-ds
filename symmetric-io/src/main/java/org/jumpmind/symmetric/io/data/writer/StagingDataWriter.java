package org.jumpmind.symmetric.io.data.writer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.io.stage.IStagingManager;

public class StagingDataWriter extends AbstractProtocolDataWriter {

    protected IStagingManager stagingManager;
    
    protected String category;

    public StagingDataWriter(String sourceNodeId, String category, IStagingManager stagingManager,
            IProtocolDataWriterListener... listeners) {
        this(sourceNodeId, category, stagingManager, toList(listeners));
    }

    public StagingDataWriter(String sourceNodeId, String category, IStagingManager stagingManager,
            List<IProtocolDataWriterListener> listeners) {
        super(sourceNodeId, listeners);
        this.category = category;
        this.stagingManager = stagingManager;
    }

    public static List<IProtocolDataWriterListener> toList(IProtocolDataWriterListener... listeners) {
        ArrayList<IProtocolDataWriterListener> list = new ArrayList<IProtocolDataWriterListener>(
                listeners.length);
        for (IProtocolDataWriterListener l : listeners) {
            list.add(l);
        }
        return list;
    }

    @Override
    protected void notifyEndBatch(Batch batch, IProtocolDataWriterListener listener) {
        listener.end(context, batch, getStagedResource(batch));
    }

    protected IStagedResource getStagedResource(Batch batch) {
        String location = batch.getStagedLocation();
        IStagedResource resource = stagingManager.find(category, location, batch.getBatchId());
        if (resource == null || resource.getState() == State.DONE) {
            resource = stagingManager.create(category, location, batch.getBatchId());
        }
        return resource;
    }

    @Override
    protected void endBatch(Batch batch) {
        IStagedResource resource = getStagedResource(batch);
        resource.close();
        resource.setState(State.READY);
        flushNodeId = true;
        processedTables.clear();
        table = null;        
    }

    @Override
    protected void print(Batch batch, String data) {
        IStagedResource resource = getStagedResource(batch);
        BufferedWriter writer = resource.getWriter();
        try {
            writer.append(data);
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

}
