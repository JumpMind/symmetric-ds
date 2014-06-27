package org.jumpmind.symmetric.ext;

import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.springframework.jms.core.JmsTemplate;

public class SimpleJmsPublisher implements IPublisher {

    JmsTemplate jmsTemplate;
    
    public void publish(IDataLoaderContext ctx, String text) {
       jmsTemplate.convertAndSend(text);
    }

    public void setJmsTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

}
