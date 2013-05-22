package org.jumpmind.symmetric.android;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import android.app.Service;
import android.content.Intent;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.IBinder;
import android.util.Log;

/**
 * This is an Android Service that can be used to start SymmetricDS embedded in
 * an Android application.
 */
public class SymmetricService extends Service {

    public static final String INTENTKEY_SQLITEOPENHELPER_REGISTRY_KEY = "sqliteHelpRegistryKey";

    public static final String INTENTKEY_REGISTRATION_URL = "registrationUrl";

    public static final String INTENTKEY_EXTERNAL_ID = "externalId";

    public static final String INTENTKEY_NODE_GROUP_ID = "nodeGroupId";

    public static final String INTENTKEY_PROPERTIES = "properties";

    public static final String INTENTKEY_START_IN_BACKGROUND = "startInBackground";

    private static final String TAG = "SymmetricService";

    private AndroidSymmetricEngine symmetricEngine;

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (symmetricEngine == null) {
            try {
                Log.i(TAG, "creating engine");
                String key = intent.getStringExtra(INTENTKEY_SQLITEOPENHELPER_REGISTRY_KEY);
                if (key != null) {
                    SQLiteOpenHelper databaseHelper = SQLiteOpenHelperRegistry.lookup(key);
                    if (databaseHelper != null) {
                        String registrationUrl = intent.getStringExtra(INTENTKEY_REGISTRATION_URL);
                        String externalId = intent.getStringExtra(INTENTKEY_EXTERNAL_ID);
                        String nodeGroupId = intent.getStringExtra(INTENTKEY_NODE_GROUP_ID);
                        Serializable passedInProps = (Serializable) intent
                                .getSerializableExtra(INTENTKEY_PROPERTIES);
                        Properties properties = null;
                        if (passedInProps instanceof Properties) {
                            properties = (Properties) passedInProps;
                        } else if (passedInProps instanceof Map) {
                            properties = new Properties();
                            @SuppressWarnings("unchecked")
                            Map<String, String> map = (Map<String, String>) passedInProps;
                            for (String propKey : map.keySet()) {
                                properties.setProperty(propKey, map.get(propKey));
                            }
                        }
                        Log.i(TAG, properties.getClass().getName());
                        symmetricEngine = new AndroidSymmetricEngine(registrationUrl, externalId,
                                nodeGroupId, properties, databaseHelper, getApplicationContext());
                        Log.i(TAG, "engine created");
                    } else {
                        Log.e(TAG, "Could not find SQLiteOpenHelper in registry using " + key);
                    }
                } else {
                    Log.e(TAG, "Must provide valid " + INTENTKEY_SQLITEOPENHELPER_REGISTRY_KEY);
                }
            } catch (Exception ex) {
                Log.e(TAG, ex.getMessage(), ex);
            }
        }

        if (symmetricEngine != null) {
            if (!symmetricEngine.isStarted() && !symmetricEngine.isStarting()) {
                try {
                    Runnable runnable = new Runnable() {
                        public void run() {
                            try {
                                Log.i(TAG, "starting engine");
                                symmetricEngine.start();
                                Log.i(TAG, "engine started");
                            } catch (Exception ex) {
                                Log.e(TAG, ex.getMessage(), ex);
                            }
                        }
                    };

                    boolean startInBackground = intent.getBooleanExtra(
                            INTENTKEY_START_IN_BACKGROUND, false);
                    if (startInBackground) {
                        new Thread(runnable).start();
                    } else {
                        runnable.run();
                    }

                } catch (Exception ex) {
                    Log.e(TAG, ex.getMessage(), ex);
                }

            }
        }

        return START_REDELIVER_INTENT;
    }

    @Override
    public void onDestroy() {
        Log.i(TAG, "destroying symmetric engine");
        super.onDestroy();
        if (symmetricEngine != null) {
            try {
                symmetricEngine.destroy();
            } catch (Exception ex) {
                Log.e(TAG, ex.getMessage(), ex);
            }
        }
    }

}
