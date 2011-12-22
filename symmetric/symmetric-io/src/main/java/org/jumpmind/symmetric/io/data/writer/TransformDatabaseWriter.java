package org.jumpmind.symmetric.io.data.writer;

import java.util.Map;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;

public class TransformDatabaseWriter extends TransformWriter {

    public TransformDatabaseWriter(IDatabasePlatform platform,
            DatabaseWriterSettings defaultSettings,
            Map<String, DatabaseWriterSettings> channelSpecificSettings,
            TransformTable[] transforms, IDatabaseWriterFilter[] filters) {
        super(platform, TransformPoint.LOAD, new DatabaseWriter(platform, defaultSettings,
                channelSpecificSettings, filters), transforms);
        getDatabaseWriter().setConflictResolver(new DefaultTransformWriterConflictResolver(this));
    }

    public DatabaseWriter getDatabaseWriter() {
        return (DatabaseWriter) this.targetWriter;
    }

}
