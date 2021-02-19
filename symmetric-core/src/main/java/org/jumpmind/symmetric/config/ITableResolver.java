package org.jumpmind.symmetric.config;

import java.util.Set;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.INodeService;

public interface ITableResolver extends IExtensionPoint {

	public void resolve(String catalog, String schema, Set<Table> tables, IDatabasePlatform platform, 
			INodeService nodeService, Trigger trigger, boolean useTableCache);
}
