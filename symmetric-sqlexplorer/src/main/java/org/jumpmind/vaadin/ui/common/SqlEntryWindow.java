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

import org.vaadin.aceeditor.AceEditor;
import org.vaadin.aceeditor.AceMode;

public class SqlEntryWindow extends ResizableWindow {

    private static final long serialVersionUID = 1L;

    protected AceEditor editor;

    public SqlEntryWindow(String sql) {
        setCaption("Edit SQL");
        editor = CommonUiUtils.createAceEditor();
        editor.setMode(AceMode.sql);
        editor.setValue(sql);
        editor.setSizeFull();
        content.addComponents(editor, buildButtonFooter(buildCloseButton()));
        content.setExpandRatio(editor, 1);
    }
    
    public String getSQL() {
        return editor.getValue();
    }

    @Override
    protected boolean onClose() {
        return super.onClose();
    }

}
