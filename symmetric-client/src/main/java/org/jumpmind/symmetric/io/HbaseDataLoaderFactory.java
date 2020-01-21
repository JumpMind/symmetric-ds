package org.jumpmind.symmetric.io;

import java.util.List;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.DefaultDataLoaderFactory;

public class HbaseDataLoaderFactory extends DefaultDataLoaderFactory implements
    ISymmetricEngineAware, IBuiltInExtensionPoint {

    protected ISymmetricEngine engine;

    protected String typeName = "hbase";

    protected String hbaseSiteXmlPath;
    
    protected IDataWriter hbaseDataWriter;
    
    public HbaseDataLoaderFactory() {
        super();
    }

    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
        this.parameterService = engine.getParameterService();
    }

    @Override
    public String getTypeName() {
        return typeName;
    }
    
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }


    @Override
    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
                TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {
        
        if (hbaseDataWriter == null) {
            this.hbaseSiteXmlPath = parameterService.getString(ParameterConstants.HBASE_SITE_XML_PATH);
            this.hbaseDataWriter = new HbaseDatabaseWriter(this.hbaseSiteXmlPath);
        }
        return this.hbaseDataWriter;
    }
}
