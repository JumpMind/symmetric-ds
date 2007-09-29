package org.jumpmind.symmetric.load;

import java.util.Date;

public interface IDataLoaderStatistics {

    public long getFallbackInsertCount();

    public long getFallbackUpdateCount();

    public long getLineCount();

    public Date getStartTime();

    public long getStatementCount();

    public long getMissingDeleteCount();

}