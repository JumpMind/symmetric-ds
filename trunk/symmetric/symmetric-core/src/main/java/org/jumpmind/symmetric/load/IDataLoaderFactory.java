package org.jumpmind.symmetric.load;

import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.ConflictSetting;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;

public interface IDataLoaderFactory extends IExtensionPoint {

    public String getTypeName();

    public IDataWriter getDataWriter(String sourceNodeId, IDatabasePlatform platform,
            TransformWriter transformWriter, List<IDatabaseWriterFilter> filters, 
            List<? extends ConflictSetting> conflictSettings);

    public boolean isPlatformSupported(IDatabasePlatform platform);

}
