package org.jumpmind.symmetric.integrate;

import org.jumpmind.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFilePublisher implements IPublisher {

    static final Logger log = LoggerFactory.getLogger(LogFilePublisher.class);
    
    @Override
    public void publish(Context context, String text) {
        log.info("\n{}", text);
    }

}
