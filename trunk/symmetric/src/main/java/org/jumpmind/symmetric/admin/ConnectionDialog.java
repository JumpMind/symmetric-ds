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
import java.awt.GridLayout;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

public class ConnectionDialog extends JPanel {

    private static final long serialVersionUID = -4325767568474807143L;

    JLabel nameLabel;

    JTextField nameField;

    JLabel userNameLabel;

    JTextField userNameField;

    JLabel passwordLabel;

    JTextField passwordField;

    JLabel serverLabel;

    JTextField serverField;

    JLabel driverLabel;

    JTextField driverField;

    public ConnectionDialog() {
        super(false);

        nameLabel = new JLabel("Connection name: ", JLabel.RIGHT);
        nameField = new JTextField("My Symmetric");

        userNameLabel = new JLabel("User name: ", JLabel.RIGHT);
        userNameField = new JTextField("");

        passwordLabel = new JLabel("Password: ", JLabel.RIGHT);
        passwordField = new JPasswordField("");

        serverLabel = new JLabel("Database URL: ", JLabel.RIGHT);
        serverField = new JTextField("jdbc:derby:target/derby/root;create=true");

        driverLabel = new JLabel("Driver: ", JLabel.RIGHT);
        driverField = new JTextField("org.apache.derby.jdbc.EmbeddedDriver");

        setLayout(new BoxLayout(this, BoxLayout.X_AXIS));

        JPanel namePanel = new JPanel(false);
        namePanel.setLayout(new GridLayout(0, 1));
        namePanel.add(nameLabel);
        namePanel.add(userNameLabel);
        namePanel.add(passwordLabel);
        namePanel.add(serverLabel);
        namePanel.add(driverLabel);

        JPanel fieldPanel = new JPanel(false);
        fieldPanel.setLayout(new GridLayout(0, 1));
        fieldPanel.add(nameField);
        fieldPanel.add(userNameField);
        fieldPanel.add(passwordField);
        fieldPanel.add(serverField);
        fieldPanel.add(driverField);

        add(namePanel);
        add(fieldPanel);
    }

    public SymmetricConnection activateConnectionDialog(Component parent) {
        if (JOptionPane.showOptionDialog(parent, this, "Database Connection Info", JOptionPane.DEFAULT_OPTION,
                JOptionPane.INFORMATION_MESSAGE, null, new String[] { "Apply", "Cancel" }, "Apply") == 0) {
            SymmetricConnection c = new SymmetricConnection(nameField.getText());
            c.setDriverName(driverField.getText());
            c.setJdbcUrl(serverLabel.getText());
            c.setUsername(userNameField.getText());
            c.setPassword(passwordField.getText());
            return c;
        } else {
            return null;
        }
    }

}
