package org.jumpmind.symmetric.android;

import org.jumpmind.symmetric.android.common.AndroidLog;
import org.jumpmind.symmetric.android.db.SQLiteDbDialect;
import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.resources.IResourceFactory;

import android.database.sqlite.SQLiteOpenHelper;

public class AndroidEnvironment implements IEnvironment {

    static {
        LogFactory.setLogClass(AndroidLog.class);
    }

    protected IResourceFactory resourceFactory;

    protected IDbDialect dbDialect;

    protected Parameters localParameters = new Parameters();

    public AndroidEnvironment(SQLiteOpenHelper sqliteopenhelper) {
        this.readParameters();
        this.resourceFactory = new AndroidResourceFactory();
        this.dbDialect = new SQLiteDbDialect(sqliteopenhelper, localParameters);
    }

    protected void readParameters() {
        // TODO figure out how we want to save retrieve preferences
    }

    public IDbDialect getDbDialect() {
        return dbDialect;
    }

    public IResourceFactory getResourceFactory() {
        return resourceFactory;
    }

    public Parameters getLocalParameters() {
        return localParameters;
    }

}
