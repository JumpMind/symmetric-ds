package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter.LoadStatus;

public interface IDatabaseWriterConflictResolver extends IExtensionPoint {

    public void needsResolved(DatabaseWriter writer, CsvData data, LoadStatus loadStatus);

}
