package org.jumpmind.symmetric.ext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.springframework.jms.core.JmsTemplate;

public class SimpleJmsPublisher implements IPublisher {

    static final Log logger = LogFactory.getLog(SimpleJmsPublisher.class);

    JmsTemplate jmsTemplate;

    public void publish(IDataLoaderContext ctx, String text) {
        if (logger.isDebugEnabled()) {
            logger.debug(text);
        }
        jmsTemplate.convertAndSend(text);
    }

    public void setJmsTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

}
