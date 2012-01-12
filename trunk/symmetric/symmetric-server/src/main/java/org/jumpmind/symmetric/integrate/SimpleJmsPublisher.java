/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

package org.jumpmind.symmetric.integrate;

import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;
import org.jumpmind.util.Context;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.jms.core.JmsTemplate;

public class SimpleJmsPublisher implements IPublisher, BeanFactoryAware {

    static final Log log = LogFactory.getLog(SimpleJmsPublisher.class);

    private BeanFactory beanFactory;

    private String jmsTemplateBeanName;

    public boolean enabled = true;

    public void publish(Context context, String text) {
        log.debug("TextPublishing", text);
        JmsTemplate jmsTemplate = (JmsTemplate) beanFactory.getBean(jmsTemplateBeanName);
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