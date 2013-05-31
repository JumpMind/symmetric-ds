package org.jumpmind.symmetric.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.jumpmind.symmetric.load.ILoadSyncLifecycleListener;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingError;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.service.impl.DataLoaderService.ConflictNodeGroupLink;

/**
 * This service provides an API to load data into a SymmetricDS node's database
 * from a transport
 */
public interface IDataLoaderService {
    
    public boolean refreshFromDatabase();

    public RemoteNodeStatus loadDataFromPull(Node remote) throws IOException;

    public void loadDataFromPull(Node sourceNode, RemoteNodeStatus status) throws IOException;

    public void loadDataFromPush(Node sourceNode, InputStream in, OutputStream out) throws IOException;
    
    public void addDataLoaderFactory(IDataLoaderFactory factory);
    
    public List<String> getAvailableDataLoaderFactories();
    
    public void addDatabaseWriterErrorHandler(IDatabaseWriterErrorHandler handler);
    
    public void removeDatabaseWriterErrorHandler(IDatabaseWriterErrorHandler handler);

    public void addDatabaseWriterFilter(IDatabaseWriterFilter filter);            

    public void removeDatabaseWriterFilter(IDatabaseWriterFilter filter);
    
    public void addLoadSyncLifecycleListener(ILoadSyncLifecycleListener listener);
    
    public void removeLoadSyncLifecycleListener(ILoadSyncLifecycleListener listener);
    
    public List<IncomingBatch> loadDataBatch(String batchData);
    
    public List<ConflictNodeGroupLink> getConflictSettingsNodeGroupLinks(NodeGroupLink link, boolean refreshCache);
    
    public List<ConflictNodeGroupLink> getConflictSettingsNodeGroupLinks();
    
    public void delete(ConflictNodeGroupLink settings);
    
    public void save(ConflictNodeGroupLink settings);
    
    public void clearCache();

    public List<IncomingError> getIncomingErrors(long batchId, String nodeId);

    public IncomingError getCurrentIncomingError(long batchId, String nodeId);
    
    public void insertIncomingError(IncomingError incomingError);
    
    public void updateIncomingError(IncomingError incomingError);

}