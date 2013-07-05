package org.jumpmind.db.platform;

import java.util.List;

import org.jumpmind.db.alter.IModelChange;
import org.jumpmind.db.model.Database;
import org.jumpmind.extension.IExtensionPoint;

public interface IAlterDatabaseInterceptor extends IExtensionPoint {

    public List<IModelChange> intercept(List<IModelChange> detectedChanges, Database currentModel,
            Database desiredModel);

}
