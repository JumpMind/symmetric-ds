package org.jumpmind.symmetric.io;

import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.db.platform.generic.GenericJdbcDatabasePlatform;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.util.BasicDataSourceFactory;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.SecurityServiceFactory;
import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.JdbcDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.DefaultDataLoaderFactory;

public class JdbcDataLoaderFactory  extends DefaultDataLoaderFactory implements
	ISymmetricEngineAware, IBuiltInExtensionPoint {

	protected ISymmetricEngine engine;

	protected String typeName = "jdbc";

	public final static String PROPERTY_PREFIX = "jdbc.";
	
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
		
		TypedProperties properties = new TypedProperties();
		for (String prop : BasicDataSourcePropertyConstants.allProps ) {
			properties.put(prop, parameterService.getString(PROPERTY_PREFIX + prop));
		}
		
		DataSource dataSource = BasicDataSourceFactory.create(properties, SecurityServiceFactory.create(SecurityServiceType.CLIENT, properties));
		GenericJdbcDatabasePlatform platform = new GenericJdbcDatabasePlatform(dataSource, new SqlTemplateSettings());

		platform.setName(parameterService.getString(PROPERTY_PREFIX + "alias"));
		platform.getDatabaseInfo().setNotNullColumnsSupported(parameterService.is(PROPERTY_PREFIX + ParameterConstants.CREATE_TABLE_NOT_NULL_COLUMNS, true));
		
		JdbcDatabaseWriter writer = new JdbcDatabaseWriter(platform);
		writer.setTablePrefix(engine.getTablePrefix());
		return writer;
	}
	
}
