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
package org.jumpmind.symmetric.load;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Extension point that can be used to change or take action on data as it is about to be 
 * written to a target {@link Node}.
 * <p/>
 * If work needs to occur on the target database itself within the same database transaction
 * that the load is taking place in, then the {@link IDataLoaderContext} should be 
 * used to access an {@link JdbcTemplate}.  The JdbcTemplate contains the connection that is being 
 * used for the data load.
 */
public interface IDataLoaderFilter extends IExtensionPoint {

    /**
     * @return True if the row should be loaded. False if the filter has handled
     *         the row and it should be ignored.
     */
    public boolean filterInsert(IDataLoaderContext context, String[] columnValues);

    /**
     * @param columnValues Contains 'new' values for both key and non-key columns
     * @param keyValues Contains the 'old' values for the key columns
     * @return True if the row should be loaded. False if the filter has handled
     *         the row and it should be ignored.
     */
    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues);

    /**
     * @return True if the row should be loaded. False if the filter has handled
     *         the row and it should be ignored.
     */
    public boolean filterDelete(IDataLoaderContext context, String[] keyValues);

}