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

import java.util.Set;

import com.vaadin.v7.event.ItemClickEvent;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.shared.MouseEventDetails.MouseButton;
import com.vaadin.v7.ui.Table;

public class MultiSelectTable extends Table {

    private static final long serialVersionUID = 1L;

    private Set<Object> lastSelected;
    
    public MultiSelectTable() {
        setMultiSelect(true);
        setSelectable(true);
        
        addValueChangeListener(new ValueChangeListener() {
            private static final long serialVersionUID = 1L;
            @SuppressWarnings("unchecked")
            @Override
            public void valueChange(com.vaadin.v7.data.Property.ValueChangeEvent event) {
                lastSelected = (Set<Object>) getValue();
            }
        });
        
        addItemClickListener(new ItemClickListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void itemClick(ItemClickEvent event) {
                if (event.getButton() == MouseButton.LEFT) {
                    if (lastSelected != null && lastSelected.contains(event.getItemId())) {
                        unselect(event.getItemId());
                    }
                }
            }
        });
    }
    
    @SuppressWarnings("unchecked")
    public <T> Set<T> getSelected() {
        return (Set<T>)getValue();
    }
}
