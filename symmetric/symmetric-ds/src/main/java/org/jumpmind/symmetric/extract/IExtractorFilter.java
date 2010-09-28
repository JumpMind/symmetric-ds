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

package org.jumpmind.symmetric.extract;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.Data;

/**
 * This extension point is called after data has been extracted, but before it
 * has been streamed. It has the ability to inspect each row of data to take
 * some action and indicate, if necessary, that the row should not be streamed.
 *
 * 
 */
public interface IExtractorFilter extends IExtensionPoint {

    /**
     * @return true if the row should be extracted
     */
    public boolean filterData(Data data, String routerId, DataExtractorContext ctx);

}