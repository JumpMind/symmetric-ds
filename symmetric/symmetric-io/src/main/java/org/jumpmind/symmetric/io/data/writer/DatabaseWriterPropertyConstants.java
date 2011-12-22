package org.jumpmind.symmetric.io.data.writer;

final public class DatabaseWriterPropertyConstants {
    
    public final static String DATA_LOADER_NO_KEYS_IN_UPDATE = "dont.include.keys.in.update.statement";
    public final static String DATA_LOADER_ENABLE_FALLBACK_UPDATE = "dataloader.enable.fallback.update";
    public final static String DATA_LOADER_ENABLE_FALLBACK_SAVEPOINT = "dataloader.enable.fallback.savepoint";
    public final static String DATA_LOADER_ENABLE_FALLBACK_INSERT = "dataloader.enable.fallback.insert";
    public final static String DATA_LOADER_ALLOW_MISSING_DELETE = "dataloader.allow.missing.delete";
    public final static String DATA_LOADER_MAX_ROWS_BEFORE_COMMIT = "dataloader.max.rows.before.commit";
    public final static String DB_TREAT_DATE_TIME_AS_VARCHAR = "db.treat.date.time.as.varchar.enabled";
    public final static String DATA_LOADER_USE_PKS_FROM_SOURCE = "dataloader.use.pks.from.source";

    private DatabaseWriterPropertyConstants() {
    }
    
    

}
