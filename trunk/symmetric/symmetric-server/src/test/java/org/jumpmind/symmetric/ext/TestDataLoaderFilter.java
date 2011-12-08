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

package org.jumpmind.symmetric.ext;

import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;

/**
 * 
 */
public class TestDataLoaderFilter implements IDataLoaderFilter, ITestDataLoaderFilter {

    private boolean autoRegister = true;

    private int numberOfTimesCalled = 0;

    private static int numberOfTimesCreated;

    public TestDataLoaderFilter() {
        numberOfTimesCreated++;
    }
    
    public static int getNumberOfTimesCreated() {
        return numberOfTimesCreated;
    }

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        numberOfTimesCalled++;
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        numberOfTimesCalled++;
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        numberOfTimesCalled++;
        return true;
    }

    public boolean isAutoRegister() {
        return this.autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public int getNumberOfTimesCalled() {
        return numberOfTimesCalled;
    }
    


}