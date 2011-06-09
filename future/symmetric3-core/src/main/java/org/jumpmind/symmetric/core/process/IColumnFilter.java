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

package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.ext.IExtensionPoint;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;

/**
 * This is an extension point that can be implemented to filter out columns from
 * use by the dataloader. One column filter may be added per target table. </p>
 * Please implement {@link ITableColumnFilter} instead of this class directly if
 * you want the extension to be auto discovered.
 * 
 * 
 */
public interface IColumnFilter extends IExtensionPoint {

    /**
     * This method is always called first. Typically, you must cache the column
     * index you are interested in order to be able to filter the column value
     * as well.
     * <P>
     * 
     * @param columnNames
     *            If column names are going to change, then you should change
     *            the name in this reference and return it as the return value.
     * @return The columnName that the data loader will use to build its dml.
     */
    public Column[] filterColumnsNames(DataContext ctx, Table table, Column[] columns);

    /**
     * This method is always called after
     * {@link IColumnFilter#filterColumnsNames(DmlType, String[])}. It should
     * perform the same filtering under the same conditions for the values as
     * was done for the column names.
     * 
     * @return the column values
     */
    public String[] filterColumnsValues(DataContext ctx, Table table, String[] values);
}