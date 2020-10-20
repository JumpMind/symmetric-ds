/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.driver;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.properties.TypedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomErrorInterceptor extends StatementInterceptor {
    private final static Logger log = LoggerFactory.getLogger(RandomErrorInterceptor.class);
    
    private static AtomicInteger okCounter = new AtomicInteger(0);
    private static AtomicInteger errorCounter = new AtomicInteger(0);
    
    private static final int MIN = 1;
    private static final int MAX = 1000;
    private static final int ERROR_THRESHOLD = (int)(MAX * 0.01);
    
    private Random random = new Random();

    public RandomErrorInterceptor(Object wrapped, TypedProperties properties) {
        super(wrapped, properties);
    }
    
    @Override
    protected InterceptResult preparedStatementPreExecute(PreparedStatementWrapper ps, String methodName, Object[] parameters) {
        if (shouldThrowError()) {
            int errorCount = errorCounter.incrementAndGet();
            int okCount = okCounter.get();
            throw new SqlException("MOCK/RANDOM ERROR from RandomErrorInterceptor (" + okCount + " successful statements, " + errorCount + " error statements.)");
        } else {
            okCounter.incrementAndGet();
            return new InterceptResult();
        }
    }
    
    @Override
    public void preparedStatementExecute(String methodName, long elapsed, String sql) {
        // no op.
    }
    
    protected boolean shouldThrowError() {
        int count = okCounter.get();
        if (count < 1000) { // the database needs to be available for the engine to start up in the first place.
            return false;
        }
        // for now, error 1% of the time.
        int randomValue = random.nextInt(MAX - MIN + 1) + MIN;
        return (randomValue <= ERROR_THRESHOLD);
    }
}
