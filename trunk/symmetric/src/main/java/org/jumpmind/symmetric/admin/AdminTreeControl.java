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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.ImageIcon;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

public class AdminTreeControl extends JScrollPane {

    private static final long serialVersionUID = -4986503985750730049L;

    private JTree tree;

    private JPopupMenu rootPopup;

    private JPopupMenu connectionPopup;

    private List<SymmetricConnection> connections = new ArrayList<SymmetricConnection>();

    private IAppController appController;

    private ConnectionDialog connectionDialog;

    AbstractAction connectAction;

    protected AdminTreeControl(IAppController controller) throws Exception {
        this.connectionDialog = new ConnectionDialog();
        this.appController = controller;
        final DefaultMutableTreeNode top = new DefaultMutableTreeNode("Connections") {

            private static final long serialVersionUID = -1L;

            @Override
            public boolean isLeaf() {
                return false;
            }
        };

        rootPopup = new JPopupMenu();
        rootPopup.add(new JMenuItem(new AbstractAction("Add Connection") {
            private static final long serialVersionUID = -1L;

            public void actionPerformed(ActionEvent e) {
                SymmetricConnection c = connectionDialog.activateConnectionDialog(appController.getFrame());
                if (c != null) {
                    addSymmetricConnection(c, top);
                    saveConnections(connections);
                }
            }
        }));

        connectAction = new AbstractAction("Connect") {
            private static final long serialVersionUID = -1L;

            public void actionPerformed(ActionEvent e) {
            }

        };

        connectionPopup = new JPopupMenu();
        connectionPopup.add(new JMenuItem(connectAction));
        connectionPopup.add(new JMenuItem(new AbstractAction("Remove Connection") {
            private static final long serialVersionUID = -1L;

            public void actionPerformed(ActionEvent e) {
                removeSymmetricConnection((DefaultMutableTreeNode) tree.getSelectionPath().getLastPathComponent());
                saveConnections(connections);
            }
        }));

        top.setAllowsChildren(true);
        tree = new JTree(top);
        tree.setCellRenderer(new TreeCellRenderer());
        tree.addMouseListener(new PopupListener());

        List<SymmetricConnection> connections = loadConnections();
        for (SymmetricConnection c : connections) {
            addSymmetricConnection(c, top);
        }

        this.getViewport().add(tree);
    }

    private void addSymmetricConnection(SymmetricConnection c, DefaultMutableTreeNode top) {
        this.connections.add(c);
        DefaultTreeModel model = (DefaultTreeModel) tree.getModel();
        DefaultMutableTreeNode child = new DefaultMutableTreeNode(c);
        model.insertNodeInto(child, top, top.getChildCount());
        tree.scrollPathToVisible(new TreePath(child));
    }

    private void removeSymmetricConnection(DefaultMutableTreeNode node) {
        this.connections.remove(node.getUserObject());
        DefaultTreeModel model = (DefaultTreeModel) tree.getModel();
        model.removeNodeFromParent(node);
    }

    private File getAppDir() {
        File appDir = new File(System.getProperty("user.home") + "/.symmetricds/");
        if (!appDir.exists()) {
            appDir.mkdirs();
        }
        return appDir;
    }

    private File getConnectionsFile() {
        return new File(getAppDir(), "connections.xml");
    }

    @SuppressWarnings("unchecked")
    private List<SymmetricConnection> loadConnections() throws Exception {
        List<SymmetricConnection> connections = null;
        File connectionsFile = getConnectionsFile();
        if (connectionsFile.exists()) {
            XMLDecoder d = new XMLDecoder(new BufferedInputStream(new FileInputStream(connectionsFile)));
            connections = (List<SymmetricConnection>) d.readObject();
            d.close();
        }
        return connections == null ? Collections.EMPTY_LIST : connections;
    }

    private void saveConnections(List<SymmetricConnection> connections) {
        try {
            XMLEncoder e = new XMLEncoder(new BufferedOutputStream(new FileOutputStream(getConnectionsFile())));
            e.writeObject(connections);
            e.close();
        } catch (Exception ex) {
            appController.showError("Failed to save connections.xml.", ex);
        }

    }

    class PopupListener extends MouseAdapter {
        public void mousePressed(MouseEvent e) {
            maybeShowPopup(e);
        }

        public void mouseReleased(MouseEvent e) {
            maybeShowPopup(e);
        }

        private void maybeShowPopup(MouseEvent e) {
            if (e.isPopupTrigger()) {
                TreePath path = tree.getSelectionPath();
                if (path != null
                        && ((DefaultMutableTreeNode) path.getLastPathComponent()).getUserObject() instanceof SymmetricConnection) {
                    SymmetricConnection c = (SymmetricConnection) ((DefaultMutableTreeNode) path.getLastPathComponent())
                            .getUserObject();
                    if (c.isConnected()) {
                        connectAction.putValue(Action.NAME, "Disconnect");
                    } else {
                        connectAction.putValue(Action.NAME, "Connect");
                    }
                    connectionPopup.show(e.getComponent(), e.getX(), e.getY());
                } else {
                    rootPopup.show(e.getComponent(), e.getX(), e.getY());
                }
            }
        }
    }

    class TreeCellRenderer extends DefaultTreeCellRenderer {

        private static final long serialVersionUID = 254830074017647254L;

        ImageIcon databaseIcon;

        TreeCellRenderer() {
            databaseIcon = new ImageIcon(getClass().getResource("/images/database.png"));
        }

        @Override
        public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded,
                boolean leaf, int row, boolean hasFocus) {
            super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
            if (((DefaultMutableTreeNode) value).getUserObject() instanceof SymmetricConnection) {
                setIcon(databaseIcon);
            }
            return this;
        }
    }

}
