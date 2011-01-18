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
package org.jumpmind.symmetric.test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jumpmind.symmetric.load.csv.CsvLoader;
import org.jumpmind.symmetric.service.impl.DataExtractorService;
import org.jumpmind.symmetric.service.impl.DataLoaderService;
import org.jumpmind.symmetric.service.impl.RouterService;
import org.jumpmind.symmetric.util.CsvUtils;
import org.springframework.jdbc.core.JdbcTemplate;

abstract public class AbstractTest {

    protected Level setLoggingLevelForTest(Level level) {
        Level old = Logger.getLogger(getClass()).getLevel();
        Logger.getLogger(DataLoaderService.class).setLevel(level);
        Logger.getLogger(DataExtractorService.class).setLevel(level);
        Logger.getLogger(RouterService.class).setLevel(level);
        Logger.getLogger(CsvLoader.class).setLevel(level);
        Logger.getLogger(CsvUtils.class).setLevel(level);
        Logger.getLogger(JdbcTemplate.class).setLevel(level);
        return old;
    }
    
    protected void logTestRunning() {
        Logger.getLogger(getClass()).info("Running " + new Exception().getStackTrace()[1].getMethodName() + ". "
                + printDatabases());
    }
    
    protected void logTestComplete() {
        Logger.getLogger(getClass()).info("Completed running " + new Exception().getStackTrace()[1].getMethodName() + ". "
                + printDatabases());
    }
    
    abstract protected String printDatabases();
    
}
