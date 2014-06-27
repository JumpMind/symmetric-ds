package org.jumpmind.symmetric.db.mariadb;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.db.mysql.MySqlSymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;

public class MariaDBSymmetricDialect extends MySqlSymmetricDialect {

	public MariaDBSymmetricDialect(IParameterService parameterService,
			IDatabasePlatform platform) {
		super(parameterService, platform);
	}

}
