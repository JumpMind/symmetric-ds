package org.jumpmind.symmetric.ddlutils.informix;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.platform.JdbcModelReader;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

public class InformixModelReader extends JdbcModelReader {

    final ILog logger = LogFactory.getLog(getClass());

    public InformixModelReader(Platform platform) {
	super(platform);
	setDefaultCatalogPattern(null);
	setDefaultSchemaPattern(null);
    }
}
