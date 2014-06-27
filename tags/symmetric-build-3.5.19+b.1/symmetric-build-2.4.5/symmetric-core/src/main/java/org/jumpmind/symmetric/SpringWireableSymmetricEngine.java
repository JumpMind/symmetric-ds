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
 * under the License.  */


package org.jumpmind.symmetric;

import java.util.Properties;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * This is the preferred way to wire a SymmetricDS instance into an existing
 * Spring context. It will create its own {@link ApplicationContext} as a child
 * of the Spring {@link ApplicationContext} it is being wired into.
 */
public class SpringWireableSymmetricEngine extends AbstractSymmetricEngine implements ApplicationContextAware {

    private Properties properties;
    
    public SpringWireableSymmetricEngine() {
    }
    
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.init(applicationContext, true, properties, null, null);
    }
    
    @Override
    protected ApplicationContext createContext(ApplicationContext parentContext) {
        return new ClassPathXmlApplicationContext(new String[] { "classpath:/symmetric-server.xml" }, parentContext);
    }

}