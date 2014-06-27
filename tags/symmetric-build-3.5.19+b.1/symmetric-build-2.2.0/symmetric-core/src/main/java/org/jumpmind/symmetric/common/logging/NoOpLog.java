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

package org.jumpmind.symmetric.common.logging;

/**
 * 
 */
public class NoOpLog implements ILog {

    public void debug(String messageKey) {

    }

    public void debug(String messageKey, Throwable t) {

    }

    public void debug(String messageKey, Object... args) {

    }

    public void debug(String messageKey, Throwable t, Object... args) {

    }

    public void error(String messageKey) {

    }

    public void error(String messageKey, Throwable t) {

    }

    public void error(String messageKey, Object... args) {

    }

    public void error(String messageKey, Throwable t, Object... args) {

    }

    public void error(Throwable t) {

    }

    public void fatal(String messageKey) {

    }

    public void fatal(String messageKey, Throwable t) {

    }

    public void fatal(String messageKey, Object... args) {

    }

    public void fatal(String messageKey, Throwable t, Object... args) {

    }

    public void fatal(Throwable t) {

    }

    public void info(String messageKey) {

    }

    public void info(String messageKey, Throwable t) {

    }

    public void info(String messageKey, Object... args) {

    }

    public void info(String messageKey, Throwable t, Object... args) {

    }

    public boolean isDebugEnabled() {

        return false;
    }

    public boolean isErrorEnabled() {

        return false;
    }

    public boolean isInfoEnabled() {

        return false;
    }

    public boolean isWarnEnabled() {

        return false;
    }

    public void warn(String messageKey) {

    }

    public void warn(String messageKey, Throwable t) {

    }

    public void warn(String messageKey, Object... args) {

    }

    public void warn(String messageKey, Throwable t, Object... args) {

    }

    public void warn(Throwable t) {

    }

}