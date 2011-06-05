package org.jumpmind.symmetric.android;

import org.jumpmind.symmetric.android.common.AndroidLog;
import org.jumpmind.symmetric.android.db.SQLiteDbDialect;
import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.resources.IResourceFactory;

import android.database.sqlite.SQLiteOpenHelper;

public class AndroidEnvironment implements IEnvironment {

    static {
        System.setProperty(Log.class.getName(), AndroidLog.class.getName());
    }

    protected IResourceFactory resourceFactory = new AndroidResourceFactory();

    protected IDbDialect dbDialect;

    protected Parameters parameters = new Parameters();

    public AndroidEnvironment(SQLiteOpenHelper sqliteopenhelper) {
        readParameters();
        this.dbDialect = new SQLiteDbDialect(sqliteopenhelper, parameters);
    }

    protected void readParameters() {

    }

    public IDbDialect getDbDialect() {
        return dbDialect;
    }

    public IResourceFactory getResourceFactory() {
        return resourceFactory;
    }

    public Parameters getParameters() {
        return parameters;
    }

}
