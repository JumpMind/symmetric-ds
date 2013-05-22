package org.jumpmind.symmetric.android;

import java.math.BigDecimal;

import org.jumpmind.db.platform.AbstractDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.sqlite.SqliteDdlBuilder;
import org.jumpmind.db.platform.sqlite.SqliteDdlReader;
import org.jumpmind.db.sql.ISqlTemplate;

import android.content.Context;
import android.database.sqlite.SQLiteOpenHelper;

public class AndroidDatabasePlatform extends AbstractDatabasePlatform {

    protected SQLiteOpenHelper database;

    protected AndroidSqlTemplate sqlTemplate;
    
    protected Context androidContext;

    public AndroidDatabasePlatform(SQLiteOpenHelper database, Context androidContext) {

        this.database = database;
        this.androidContext = androidContext;
        sqlTemplate = new AndroidSqlTemplate(database, androidContext);
        ddlReader = new SqliteDdlReader(this);
        ddlBuilder = new SqliteDdlBuilder();

    }

    public String getName() {
        return DatabaseNamesConstants.SQLITE;
    }

    public String getDefaultSchema() {
        return null;
    }

    public String getDefaultCatalog() {
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T> T getDataSource() {
        return (T) this.database;
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }
    
    @Override
    protected Number parseIntegerObjectValue(String value) {
        return new BigDecimal(value);
    }

}
