package org.jumpmind.symmetric.android.example;

import org.jumpmind.symmetric.android.AndroidEnvironment;
import org.jumpmind.symmetric.android.R;
import org.jumpmind.symmetric.core.SymmetricClient;

import android.app.Activity;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;

public class TestActivity extends Activity {

    protected final String TAG = "SymmetricDS Test";
    protected Button initializeButton;

    protected SymmetricClient symmetricClient;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        DatabaseOpener opener = new DatabaseOpener();
        this.symmetricClient = new SymmetricClient(new AndroidEnvironment(opener));
        setContentView(R.layout.main);
        this.initializeButton = (Button) findViewById(R.id.initializeButton);
        this.initializeButton.setOnClickListener(new View.OnClickListener() {
            public void onClick(View view) {
                Log.i(TAG, "initializing symmetric client");
                symmetricClient.initialize();
            }
        });
    }

    protected class DatabaseOpener extends SQLiteOpenHelper {

        public DatabaseOpener() {
            super(TestActivity.this, "symmetricds", null, 1);
        }

        @Override
        public void onCreate(SQLiteDatabase sqlitedatabase) {
        }

        @Override
        public void onUpgrade(SQLiteDatabase sqlitedatabase, int i, int j) {
        }
    }

}
