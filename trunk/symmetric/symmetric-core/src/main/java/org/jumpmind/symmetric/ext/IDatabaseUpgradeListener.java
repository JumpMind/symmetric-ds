package org.jumpmind.symmetric.ext;

import java.io.IOException;

import org.jumpmind.db.model.Database;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.db.IDbDialect;

public interface IDatabaseUpgradeListener extends IExtensionPoint {

    public String beforeUpgrade(IDbDialect dbDialect, String tablePrefix, Database currentModel,
        Database desiredModel) throws IOException;

    public String afterUpgrade(IDbDialect dbDialect, String tablePrefix, Database model) throws IOException;

}
