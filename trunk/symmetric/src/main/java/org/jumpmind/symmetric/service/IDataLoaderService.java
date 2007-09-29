package org.jumpmind.symmetric.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.springframework.transaction.annotation.Transactional;

public interface IDataLoaderService {

    @Transactional
    public void loadData(Node remote, Node local) throws IOException;

    @Transactional
    public boolean loadData(IIncomingTransport reader);
    
    @Transactional
    public void loadData(InputStream in, OutputStream out) throws IOException;
    
    public void addDataLoaderFilter(IDataLoaderFilter filter);

    public void setDataLoaderFilters(List<IDataLoaderFilter> filters);
    
    public void removeDataLoaderFilter(IDataLoaderFilter filter);

    public void setTransportManager(ITransportManager transportManager);
}
