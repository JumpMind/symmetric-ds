package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractUriHandler implements IUriHandler {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    private String uriPattern;
    
    private List<IInterceptor> interceptors;
    
    protected IParameterService parameterService;
    
    private boolean enabled = true;
    
    public AbstractUriHandler(String uriPattern, IParameterService parameterService, 
            IInterceptor... interceptors) {
        this.uriPattern = uriPattern;
        this.interceptors = new ArrayList<IInterceptor>(interceptors.length);
        for (IInterceptor i : interceptors) {
            this.interceptors.add(i);
        }
        this.parameterService = parameterService;
    }

    public void setUriPattern(String uriPattern) {
        this.uriPattern = uriPattern;
    }
    
    public String getUriPattern() {
        return uriPattern;
    }

    public void setInterceptors(List<IInterceptor> interceptors) {
        this.interceptors = interceptors;
    }
    
    public List<IInterceptor> getInterceptors() {
        return interceptors;
    }
    
    protected IOutgoingTransport createOutgoingTransport(OutputStream outputStream, String encoding, ChannelMap map) throws IOException {
        return new InternalOutgoingTransport(outputStream, map, encoding);
    }

    protected IOutgoingTransport createOutgoingTransport(OutputStream outputStream, String encoding) throws IOException {
        return new InternalOutgoingTransport(outputStream, new ChannelMap(), encoding);
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isEnabled() {
        return enabled;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
}
