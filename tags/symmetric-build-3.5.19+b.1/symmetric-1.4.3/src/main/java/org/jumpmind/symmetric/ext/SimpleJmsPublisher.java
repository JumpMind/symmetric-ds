package org.jumpmind.symmetric.ext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.springframework.jms.core.JmsTemplate;

public class SimpleJmsPublisher implements IPublisher {

    static final Log logger = LogFactory.getLog(SimpleJmsPublisher.class);

    JmsTemplate jmsTemplate;

    public boolean enabled = true;

    public void publish(IDataLoaderContext ctx, String text) {
        if (logger.isDebugEnabled()) {
            logger.debug(text);
        }

        if (enabled) {
            jmsTemplate.convertAndSend(text);
        } else {
            logger.warn("Message was not published because the publisher is not enabled.");
        }
    }

    public void setJmsTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enable) {
        this.enabled = enable;
    }

}
