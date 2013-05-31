package org.jumpmind.symmetric.integrate;

import org.jumpmind.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.jms.core.JmsTemplate;

public class SimpleJmsPublisher implements IPublisher, BeanFactoryAware {

    static final Logger log = LoggerFactory.getLogger(SimpleJmsPublisher.class);

    private BeanFactory beanFactory;

    private String jmsTemplateBeanName;

    public boolean enabled = true;

    public void publish(Context context, String text) {
        log.debug("Publishing {}", text);
        JmsTemplate jmsTemplate = (JmsTemplate) beanFactory.getBean(jmsTemplateBeanName);
        if (enabled) {
            jmsTemplate.convertAndSend(text);
        } else {
            log.warn("Message was not published because the publisher is not enabled.");
        }
    }

    public void setEnabled(boolean enable) {
        this.enabled = enable;
    }

    public void setJmsTemplateBeanName(String jmsTemplateBeanName) {
        this.jmsTemplateBeanName = jmsTemplateBeanName;
    }

    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
}
