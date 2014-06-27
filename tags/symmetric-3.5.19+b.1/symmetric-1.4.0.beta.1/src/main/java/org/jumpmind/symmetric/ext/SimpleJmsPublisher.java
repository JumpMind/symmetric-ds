package org.jumpmind.symmetric.ext;

import org.springframework.jms.core.JmsTemplate;

public class SimpleJmsPublisher implements IPublisher {

    JmsTemplate jmsTemplate;
    
    public void publish(String text) {
       jmsTemplate.convertAndSend(text);
    }

    public void setJmsTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

}
