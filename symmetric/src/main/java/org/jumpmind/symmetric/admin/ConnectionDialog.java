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
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.swing.BoxLayout;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.MutableComboBoxModel;

public class ConnectionDialog extends JPanel {

    private static final String DEFAULT_TABLE_PREFIX = "sym";

    private static final String MY_SYMMETRIC = "My Symmetric";

    private static final long serialVersionUID = -4325767568474807143L;

    JLabel nameLabel;

    JTextField nameField;

    JLabel userNameLabel;

    JTextField userNameField;

    JLabel passwordLabel;

    JTextField passwordField;
    
    JLabel tablePrefixLabel;
    
    JTextField tablePrefixField;

    JLabel urlLabel;

    JTextField urlField;

    JLabel driverLabel;

    JComboBox driverField;

    MutableComboBoxModel driverList;

    final static Map<String, String> DRIVERS = new HashMap<String, String>();

    static {
        DRIVERS.put("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:path/to/db;create=true");
        DRIVERS.put("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/db");
        DRIVERS.put("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:db");
        DRIVERS.put("org.hsqldb.jdbcDriver", "jdbc:hsqldb:file:path/to/db;shutdown=true");
        DRIVERS.put("net.sourceforge.jtds.jdbc.Driver", "jdbc:jtds:sqlserver://localhost/db");
    }

    public ConnectionDialog() {
        super(false);

        nameLabel = new JLabel("Connection name: ", JLabel.RIGHT);
        nameField = new JTextField(MY_SYMMETRIC);
        
        tablePrefixLabel = new JLabel("SymmmetricDS table prefix:", JLabel.RIGHT);
        tablePrefixField = new JTextField(DEFAULT_TABLE_PREFIX);

        userNameLabel = new JLabel("User name: ", JLabel.RIGHT);
        userNameField = new JTextField("");

        passwordLabel = new JLabel("Password: ", JLabel.RIGHT);
        passwordField = new JPasswordField("");

        urlLabel = new JLabel("Database URL: ", JLabel.RIGHT);
        urlField = new JTextField("jdbc:derby:target/derby/root;create=true");

        driverLabel = new JLabel("Driver: ", JLabel.RIGHT);
        driverList = new DefaultComboBoxModel();
        driverField = new JComboBox(driverList);
        driverField.setEditable(true);
        driverField.setFont(urlField.getFont());
        driverField.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                urlField.setText(DRIVERS.get(driverField.getSelectedItem()));
            }
        });

        setLayout(new BoxLayout(this, BoxLayout.X_AXIS));

        JPanel namePanel = new JPanel(false);
        namePanel.setLayout(new GridLayout(0, 1));
        namePanel.add(nameLabel);
        namePanel.add(tablePrefixLabel);
        namePanel.add(driverLabel);
        namePanel.add(urlLabel);
        namePanel.add(userNameLabel);
        namePanel.add(passwordLabel);

        JPanel fieldPanel = new JPanel(false);
        fieldPanel.setLayout(new GridLayout(0, 1));
        fieldPanel.add(nameField);
        fieldPanel.add(tablePrefixField);
        fieldPanel.add(driverField);
        fieldPanel.add(urlField);
        fieldPanel.add(userNameField);
        fieldPanel.add(passwordField);

        add(namePanel);
        add(fieldPanel);
    }

    private void resetDialog() {
        while (driverList.getSize() > 0) {
            driverList.removeElementAt(0);
        }
        Set<String> drivers = DRIVERS.keySet();
        for (String string : drivers) {
            driverList.addElement(string);
        }
        driverField.setSelectedIndex(0);
        nameField.setText(MY_SYMMETRIC);
        tablePrefixField.setText(DEFAULT_TABLE_PREFIX);
    }

    public SymmetricDatabase activateConnectionDialog(Component parent, SymmetricDatabase db) {
        resetDialog();
        if (db != null) {
            nameField.setText(db.getName());
            driverField.setSelectedItem(db.getDriverName());
            urlField.setText(db.getJdbcUrl());
            userNameField.setText(db.getUserName());
            passwordField.setText(db.getPassword());
        }
        if (JOptionPane.showOptionDialog(parent, this, "Database Connection Info", JOptionPane.DEFAULT_OPTION,
                JOptionPane.INFORMATION_MESSAGE, null, new String[] { "Apply", "Cancel" }, "Apply") == 0) {
            if (db == null) {
                db = new SymmetricDatabase();
            }
            db.setName(nameField.getText());
            db.setDriverName(driverField.getSelectedItem().toString());
            db.setJdbcUrl(urlField.getText());
            db.setUsername(userNameField.getText());
            db.setPassword(passwordField.getText());
            return db;
        } else {
            return null;
        }
    }

}
