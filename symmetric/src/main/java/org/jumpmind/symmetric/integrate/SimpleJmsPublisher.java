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
