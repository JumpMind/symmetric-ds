package org.jumpmind.symmetric.io.data.writer;

import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.load.DefaultDataLoaderFactory;

public class MongoDataLoaderFactory extends DefaultDataLoaderFactory implements
        ISymmetricEngineAware {

    protected ISymmetricEngine engine;

    protected String typeName = "mongodb";

    protected IDBObjectMapper objectMapper;

    public MongoDataLoaderFactory() {
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

    @Override
    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
            TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {
        return new MongoDatabaseWriter(objectMapper, new SimpleMongoClientManager(parameterService, typeName),
                new DefaultTransformWriterConflictResolver(transformWriter),
                buildDatabaseWriterSettings(filters, errorHandlers, conflictSettings, resolvedData));
    }

    @Override
    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return true;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public void setObjectMapper(IDBObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

}
