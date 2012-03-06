package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.CsvData;

public interface IDatabaseWriterConflictResolver extends IExtensionPoint {

    public void needsResolved(DatabaseWriter writer, DatabaseWriterSettings writerSettings, CsvData data);
    
    public void reportConflict(DatabaseWriter writer, DatabaseWriterSettings writerSettings, CsvData data, ConflictEvent conflictEvent);

}
