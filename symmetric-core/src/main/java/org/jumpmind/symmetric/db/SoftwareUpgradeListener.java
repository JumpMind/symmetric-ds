package org.jumpmind.symmetric.db;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SoftwareUpgradeListener implements ISoftwareUpgradeListener, ISymmetricEngineAware, IBuiltInExtensionPoint {

	protected static final Logger log = LoggerFactory.getLogger(SoftwareUpgradeListener.class);

	ISymmetricEngine engine;
	
	@Override
	public void setSymmetricEngine(ISymmetricEngine engine) {
		this.engine = engine;
	}
	
	@Override
	public void upgrade(String databaseVersion, String softwareVersion) {
		if (databaseVersion.equals("3.8.0")) {
			log.info("Detected an original value of 3.8.0 performing necessary upgrades.");
			String sql = "update  " + engine.getParameterService().getTablePrefix()
					+ "_" + TableConstants.SYM_CHANNEL +
					" set max_batch_size = 10000 where reload_flag = 1 and max_batch_size = 1";
			engine.getSqlTemplate().update(sql);
		}
	}

}
