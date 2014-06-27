package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;

public class TransformDatabaseWriter extends TransformWriter {

    public TransformDatabaseWriter(IDatabasePlatform platform,
            DatabaseWriterSettings defaultSettings, TransformTable[] transforms) {
        super(platform, TransformPoint.LOAD,
                new DatabaseWriter(platform, defaultSettings), transforms);
        getDatabaseWriter().setConflictResolver(new DefaultTransformWriterConflictResolver(this));
    }

    public DatabaseWriter getDatabaseWriter() {
        return (DatabaseWriter) this.targetWriter;
    }

}
