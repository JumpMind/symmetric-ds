package org.jumpmind.symmetric.io.data.reader;

import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCsvDataReader {
    
    protected Logger log = LoggerFactory.getLogger(getClass());

    protected long logDebugAndCountBytes(String[] tokens) {
        long bytesRead =  0;
        if (tokens != null) {
            StringBuilder debugBuffer = log.isDebugEnabled() ? new StringBuilder() : null;
            for (String token : tokens) {
                bytesRead += token != null ? token.length() : 0;
                if (debugBuffer != null) {
                    if (token != null) {
                        String tokenTrimmed = FormatUtils.abbreviateForLogging(token);
                        debugBuffer.append(tokenTrimmed);
                    } else {
                        debugBuffer.append("<null>");
                    }
                    debugBuffer.append(",");                            
                }
            }                
            if (debugBuffer != null && debugBuffer.length() > 1) {
                log.debug("CSV parsed: {}", debugBuffer.substring(0, debugBuffer.length()-1));
            }
        }        
        return bytesRead;
    }
}
