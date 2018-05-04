package org.jumpmind.symmetric.io.cassandra;

import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.DefaultDataLoaderFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraDataLoaderFactory extends DefaultDataLoaderFactory
		implements ISymmetricEngineAware, IBuiltInExtensionPoint {

	protected ISymmetricEngine engine;

	protected String typeName = "cassandra";

	@Override
	public void setSymmetricEngine(ISymmetricEngine engine) {
		this.engine = engine;
	}

	@Override
	public String getTypeName() {
		return typeName;
	}

	@Override
	public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
			TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
			List<IDatabaseWriterErrorHandler> errorHandlers, List<? extends Conflict> conflictSettings,
			List<ResolvedData> resolvedData) {

		Cluster cluster = null;
		try {
			cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			Session session = cluster.connect();
			return null; //new CassandraDatabaseWriter(session, cluster);

		} catch (Exception e) {
			log.warn(
					"Failed to create the cassandra database writer.  Check to see if all of the required jars have been added");
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}
		}

	}

	@Override
	public boolean isPlatformSupported(IDatabasePlatform platform) {
		return true;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

}
