package org.jumpmind.symmetric.extract;

import org.jumpmind.symmetric.model.Data;

public interface IExtractorFilter {

    /**
     * @return true if the row should be extracted
     */
    public boolean filterData(Data data, DataExtractorContext ctx);
    
}
