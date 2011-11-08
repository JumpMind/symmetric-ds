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

package org.jumpmind.symmetric.common.logging;

import org.apache.commons.logging.LogConfigurationException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * Used to create a SymmetricDS {@link ILog} logger.
 */
public class LogFactory {

    public static ILog getLog(Class<?> clazz) throws LogConfigurationException {
        return new CommonsResourceLog(org.apache.commons.logging.LogFactory.getLog(clazz));
    }

    public static ILog getLog(String name) throws LogConfigurationException {
        return new CommonsResourceLog(org.apache.commons.logging.LogFactory.getLog(name));
    }

    public static ILog getLog(IParameterService parameterService) throws LogConfigurationException {
        return new CommonsResourceLog(org.apache.commons.logging.LogFactory.getLog("org.jumpmind."
                + parameterService.getString(ParameterConstants.ENGINE_NAME)));
    }
}