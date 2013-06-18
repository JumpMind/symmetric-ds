package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.FtpDataWriter;
import org.jumpmind.symmetric.io.data.writer.FtpDataWriter.Format;
import org.jumpmind.symmetric.io.data.writer.FtpDataWriter.Protocol;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.springframework.beans.factory.BeanNameAware;

public class FtpDataLoaderFactory implements IDataLoaderFactory, ISymmetricEngineAware,
        IBuiltInExtensionPoint, BeanNameAware {
    
    protected ISymmetricEngine engine;

    protected String server;
    
    protected String username;
    
    protected String password;

    protected FtpDataWriter.Protocol protocol = Protocol.FTP;
    
    protected FtpDataWriter.Format format = Format.CSV;
    
    protected String stagingDir;
    
    protected String remoteDir;

    protected String clazzName = FtpDataWriter.class.getName();
    
    protected String beanName;

    public void setBeanName(String name) {
this.beanName = name;       
    }
    
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public String getTypeName() {
        return this.beanName;
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
            TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {
        try {
            FtpDataWriter ftpWriter = (FtpDataWriter) Class.forName(clazzName).newInstance();
            ftpWriter.setFormat(format);
            ftpWriter.setProtocol(protocol);
            ftpWriter.setServer(server);
            ftpWriter.setStagingDir(stagingDir);
            ftpWriter.setUsername(username);
            ftpWriter.setRemoteDir(remoteDir);            
            ftpWriter.setPassword(password);
            return ftpWriter;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return true;
    }

    public void setClazzName(String clazzName) {
        this.clazzName = clazzName;
    }
    
    public void setFormat(FtpDataWriter.Format format) {
        this.format = format;
    }
    
    public void setProtocol(FtpDataWriter.Protocol protocol) {
        this.protocol = protocol;
    }
    
    public void setServer(String server) {
        this.server = server;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public void setStagingDir(String stagingDir) {
        this.stagingDir = stagingDir;
    }
    
    public void setRemoteDir(String remoteDir) {
        this.remoteDir = remoteDir;
    }
    
}
