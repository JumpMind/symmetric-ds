/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.admin;

import java.awt.Dimension;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JSplitPane;

public class AppFrame extends JFrame implements IAppController {

    static final org.apache.commons.logging.Log logger = org.apache.commons.logging.LogFactory.getLog(AppFrame.class);

    private static final long serialVersionUID = 8642706738637297303L;

    public AppFrame() throws Exception {
        this.setSize(800, 600);
        this.setTitle("SymmetricDS Administration Console");
        this.setIconImage(new ImageIcon(getClass().getResource("/images/application_view_tile.png")).getImage());
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        AdminTreeControl leftPane = new AdminTreeControl(this);
        leftPane.setMinimumSize(new Dimension(200, 50));
        JSplitPane splitPanel = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftPane, new JPanel());
        splitPanel.setOneTouchExpandable(true);
        splitPanel.setDividerLocation(200);
        this.getContentPane().add(splitPanel);
    }

    public void showError(String message, Exception ex) {
        logger.error(message, ex);
    }

    public JFrame getFrame() {
        return this;
    }

}
