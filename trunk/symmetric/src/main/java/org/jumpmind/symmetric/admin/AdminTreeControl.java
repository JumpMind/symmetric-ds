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
import java.awt.Cursor;
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

import foxtrot.Task;
import foxtrot.Worker;

public class AdminTreeControl extends JScrollPane {

    private static final String EDIT = "Edit Settings";

    private static final String SYMMETRICDS_ADMIN_DIR = "/.symmetricds-admin/";

    private static final String CONNECTIONS_XML = "databases.xml";

    private static final String ADD_CONNECTION = "Add Database";

    private static final String REMOVE_CONNECTION = "Remove Database";

    private static final String CONNECT = "Connect";

    private static final long serialVersionUID = -4986503985750730049L;

    private JTree tree;

    private JPopupMenu rootPopup;

    private JPopupMenu connectionPopup;

    private List<SymmetricDatabase> connections = new ArrayList<SymmetricDatabase>();

    private IAppController appController;

    private ConnectionDialog connectionDialog;

    AbstractAction connectAction;

    protected AdminTreeControl(IAppController controller) throws Exception {
        this.connectionDialog = new ConnectionDialog();
        this.appController = controller;
        final DefaultMutableTreeNode top = new DefaultMutableTreeNode("Databases") {

            private static final long serialVersionUID = -1L;

            @Override
            public boolean isLeaf() {
                return false;
            }
        };

        rootPopup = new JPopupMenu();
        rootPopup.add(new JMenuItem(new AbstractAction(ADD_CONNECTION) {
            private static final long serialVersionUID = -1L;

            public void actionPerformed(ActionEvent e) {
                SymmetricDatabase c = connectionDialog.activateConnectionDialog(appController.getFrame(), null);
                if (c != null) {
                    addSymmetricConnection(c, top);
                    saveConnections(connections);
                }
            }
        }));

        connectAction = new AbstractAction(CONNECT) {
            private static final long serialVersionUID = -1L;

            public void actionPerformed(ActionEvent e) {
                final AbstractNode c = getSelectedConnection();
                if (c != null && this.getValue(Action.NAME).equals(CONNECT)) {
                    try {
                        tree.setCursor(new Cursor(Cursor.WAIT_CURSOR));
                        Worker.post(new Task() {
                            public Object run() throws Exception {
                                c.getSymmetricDatabase().connect();
                                return null;
                            }
                        });

                        DatabaseNode db = (DatabaseNode)c;
                        db.add(new ChannelNode(c.getSymmetricDatabase()));
                        db.add(new GroupNode(c.getSymmetricDatabase()));
                        db.add(new GroupLinkNode(c.getSymmetricDatabase()));
                        tree.scrollPathToVisible(new TreePath(db));
                    } catch (Exception ex) {
                        appController.showError("Trouble connecting to the symmetric database.", ex);
                    }
                    tree.setCursor(new Cursor(Cursor.DEFAULT_CURSOR));
                    tree.repaint();
                } else {
                    appController.showError("No connection was selected.", null);
                }
            }

        };

        connectionPopup = new JPopupMenu();
        connectionPopup.add(new JMenuItem(connectAction));
        connectionPopup.add(new AbstractAction(EDIT) {
            private static final long serialVersionUID = -1L;

            public void actionPerformed(ActionEvent e) {
                SymmetricDatabase c = connectionDialog.activateConnectionDialog(appController.getFrame(),
                        getSelectedConnection().getSymmetricDatabase());
                if (c != null) {
                    tree.revalidate();
                    tree.repaint();
                    appController.show(ScreenName.INFO, c);
                    saveConnections(connections);
                }
            }

        });
        connectionPopup.add(new JMenuItem(new AbstractAction(REMOVE_CONNECTION) {
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

        List<SymmetricDatabase> connections = loadConnections();
        for (SymmetricDatabase c : connections) {
            addSymmetricConnection(c, top);
        }

        this.getViewport().add(tree);
    }

    private void addSymmetricConnection(SymmetricDatabase c, DefaultMutableTreeNode top) {
        this.connections.add(c);
        DefaultTreeModel model = (DefaultTreeModel) tree.getModel();
        DatabaseNode db = new DatabaseNode(c);
        model.insertNodeInto(db, top, top.getChildCount());
        tree.scrollPathToVisible(new TreePath(db));
    }

    private void removeSymmetricConnection(DefaultMutableTreeNode node) {
        this.connections.remove(node.getUserObject());
        DefaultTreeModel model = (DefaultTreeModel) tree.getModel();
        model.removeNodeFromParent(node);
    }

    private File getAppDir() {
        File appDir = new File(System.getProperty("user.home") + SYMMETRICDS_ADMIN_DIR);
        if (!appDir.exists()) {
            appDir.mkdirs();
        }
        return appDir;
    }

    private File getConnectionsFile() {
        return new File(getAppDir(), CONNECTIONS_XML);
    }

    @SuppressWarnings("unchecked")
    private List<SymmetricDatabase> loadConnections() throws Exception {
        List<SymmetricDatabase> connections = null;
        File connectionsFile = getConnectionsFile();
        if (connectionsFile.exists()) {
            XMLDecoder d = new XMLDecoder(new BufferedInputStream(new FileInputStream(connectionsFile)));
            connections = (List<SymmetricDatabase>) d.readObject();
            d.close();
        }
        return connections == null ? Collections.EMPTY_LIST : connections;
    }

    private void saveConnections(List<SymmetricDatabase> connections) {
        try {
            XMLEncoder e = new XMLEncoder(new BufferedOutputStream(new FileOutputStream(getConnectionsFile())));
            e.writeObject(connections);
            e.close();
        } catch (Exception ex) {
            appController.showError("Failed to save connections.xml.", ex);
        }

    }

    private AbstractNode getSelectedConnection() {
        AbstractNode c = null;

        TreePath path = tree.getSelectionPath();
        if (path != null && ((DefaultMutableTreeNode) path.getLastPathComponent()) instanceof AbstractNode) {
            c = (AbstractNode) path.getLastPathComponent();
        }
        return c;
    }

    class PopupListener extends MouseAdapter {
        private static final String DISCONNECT = "Disconnect";

        public void mousePressed(MouseEvent e) {
            AbstractNode d = getSelectedConnection();
            if (d instanceof DatabaseNode) {
                appController.show(ScreenName.INFO, d.getSymmetricDatabase());
            } else {
                appController.show(ScreenName.BLANK, null);
            }
            maybeShowPopup(e);
        }

        public void mouseReleased(MouseEvent e) {
            maybeShowPopup(e);
        }

        private void maybeShowPopup(MouseEvent e) {
            if (e.isPopupTrigger()) {
                AbstractNode c = getSelectedConnection();
                if (c != null) {
                    if (c.getSymmetricDatabase().isConnected()) {
                        connectAction.putValue(Action.NAME, DISCONNECT);
                    } else {
                        connectAction.putValue(Action.NAME, CONNECT);
                    }
                    connectionPopup.show(e.getComponent(), e.getX(), e.getY());
                } else {
                    rootPopup.show(e.getComponent(), e.getX(), e.getY());
                }
            }
        }
    }

    class TreeCellRenderer extends DefaultTreeCellRenderer {

        private static final String IMAGES_DATABASE_ERROR_PNG = "/images/database_error.png";

        private static final String IMAGES_DATABASE_PNG = "/images/database.png";

        private static final String IMAGES_CHANNELS = "/images/television.png";

        private static final String IMAGES_GROUPS = "/images/group.png";

        private static final String IMAGES_GROUP_LINKS = "/images/group_link.png";

        private static final long serialVersionUID = 1L;

        ImageIcon databaseIcon;

        ImageIcon disconnectedIcon;

        ImageIcon channelIcon;

        ImageIcon groupIcon;

        ImageIcon groupLinkIcon;

        TreeCellRenderer() {
            databaseIcon = new ImageIcon(getClass().getResource(IMAGES_DATABASE_PNG));
            disconnectedIcon = new ImageIcon(getClass().getResource(IMAGES_DATABASE_ERROR_PNG));
            channelIcon = new ImageIcon(getClass().getResource(IMAGES_CHANNELS));
            groupIcon = new ImageIcon(getClass().getResource(IMAGES_GROUPS));
            groupLinkIcon = new ImageIcon(getClass().getResource(IMAGES_GROUP_LINKS));
        }

        @Override
        public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded,
                boolean leaf, int row, boolean hasFocus) {
            super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
            if (value instanceof DatabaseNode) {
                SymmetricDatabase c = (SymmetricDatabase) ((DefaultMutableTreeNode) value).getUserObject();
                if (c.isConnected()) {
                    setIcon(databaseIcon);
                } else {
                    setIcon(disconnectedIcon);
                }
            } else if (value instanceof ChannelNode) {
                setIcon(channelIcon);
            } else if (value instanceof GroupNode) {
                setIcon(groupIcon);
            } else if (value instanceof GroupLinkNode) {
                setIcon(groupLinkIcon);
            }
            return this;
        }
    }

    abstract class AbstractNode extends DefaultMutableTreeNode {
        protected AbstractNode(SymmetricDatabase db) {
            super(db);
        }

        public SymmetricDatabase getSymmetricDatabase() {
            return (SymmetricDatabase) getUserObject();
        }
    }

    class DatabaseNode extends AbstractNode {

        private static final long serialVersionUID = 1L;

        public DatabaseNode(SymmetricDatabase db) {
            super(db);
        }
    }

    class ChannelNode extends AbstractNode {

        private static final long serialVersionUID = 1L;

        public ChannelNode(SymmetricDatabase db) {
            super(db);
        }

        public String toString() {
            return "Channels";
        }

    }

    class GroupNode extends AbstractNode {

        private static final long serialVersionUID = 1L;

        public GroupNode(SymmetricDatabase db) {
            super(db);
        }

        public String toString() {
            return "Groups";
        }

    }

    class GroupLinkNode extends AbstractNode {

        private static final long serialVersionUID = 1L;

        public GroupLinkNode(SymmetricDatabase db) {
            super(db);
        }

        public String toString() {
            return "Group Links";
        }

    }

}
