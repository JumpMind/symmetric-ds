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
package org.jumpmind.vaadin.ui.common;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.vaadin.data.provider.Query;
import com.vaadin.ui.Grid;
import com.vaadin.ui.Grid.Column;

public class GridDataProvider implements IDataProvider {
    private Grid<?> grid;

    public GridDataProvider(Grid<?> grid) {
        this.grid = grid;
    }

    @Override
    public Collection<?> getRowItems() {
        return (Collection<?>) grid.getDataProvider().fetch(new Query<>()).collect(Collectors.toList());
    }

    @Override
    public List<?> getColumns() {
        return grid.getColumns();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object getCellValue(Object item, Object column) {
        if (column instanceof Column) {
            grid.getColumns().stream().map(Column::getId).collect(Collectors.toList());
            return ((Column<Object, ?>) column).getValueProvider().apply(item);
        }
        return null;
    }

    @Override
    public String getHeaderValue(Object column) {
        return grid.getDefaultHeaderRow().getCell((Column<?, ?>) column).getText();
    }

    @Override
    public boolean isHeaderVisible() {
        return grid.isHeaderVisible();
    }
}