package org.jumpmind.symmetric.android;

import android.app.Service;
import android.content.Intent;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.IBinder;

abstract public class AbstractSymmetricService extends Service {

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return super.onStartCommand(intent, flags, startId);
    }

    /**
     * It is recommended that access to the database be relegated to one
     * connection. We will require the end user to provide access to the open
     * helper. It doesn't necessarily have to be the one and only open helper,
     * but things will probably go smoother if it is.
     */
    abstract protected SQLiteOpenHelper getSQLiteOpenHelper();

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    @Override
    public void onCreate() {
        super.onCreate();
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
    }
}
