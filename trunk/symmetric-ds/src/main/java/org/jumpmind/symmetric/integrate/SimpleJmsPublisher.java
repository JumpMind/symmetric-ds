/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.integrate;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.jms.core.JmsTemplate;

public class SimpleJmsPublisher implements IPublisher, BeanFactoryAware {

    static final ILog log = LogFactory.getLog(SimpleJmsPublisher.class);

    private BeanFactory beanFactory;
    
    private String jmsTemplateBeanName;

    public boolean enabled = true;

    public void publish(ICacheContext ctx, String text) {
        log.debug("TextPublishing", text);
        JmsTemplate jmsTemplate = (JmsTemplate)beanFactory.getBean(jmsTemplateBeanName);
        if (enabled) {
            jmsTemplate.convertAndSend(text);
        } else {
            log.warn("TextPublishingFailed");
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
