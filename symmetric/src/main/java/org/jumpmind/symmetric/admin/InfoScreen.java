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

import info.clearthought.layout.TableLayout;

import java.awt.Dimension;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

public class InfoScreen extends AbstractScreen {

    private static final long serialVersionUID = -6026696921425245160L;

    private JLabel jdbcUrl;

    private JLabel databaseName;
    
    private JLabel tablePrefix;
    
    private JLabel userName;
    
    private JLabel driverName;

    JPanel databaseInfoPanel;

    protected InfoScreen() {
        BoxLayout l = new BoxLayout(this, BoxLayout.Y_AXIS);
        this.setLayout(l);

        databaseInfoPanel = new JPanel();
        databaseInfoPanel.setMinimumSize(new Dimension(500, 100));
        databaseInfoPanel.setBorder(new TitledBorder("Server Info"));

        double size[][] = { { 5, 100, TableLayout.FILL }, { 5, 20, 20, 20, 20, 20, 5 } };

        TableLayout layout = new TableLayout(size);
        databaseInfoPanel.setLayout(layout);
        this.add(databaseInfoPanel);

        JLabel label = new JLabel("Name: ");
        databaseInfoPanel.add(label, "1,1");

        databaseName = new JLabel();
        databaseName.setEnabled(false);
        databaseInfoPanel.add(databaseName, "2,1");
        
        databaseInfoPanel.add(new JLabel("Table Prefix: "), "1,2");
        tablePrefix = new JLabel();
        tablePrefix.setEnabled(false);
        databaseInfoPanel.add(tablePrefix, "2,2");

        databaseInfoPanel.add(new JLabel("Jdbc Url: "), "1,3");
        jdbcUrl = new JLabel();
        jdbcUrl.setEnabled(false);
        databaseInfoPanel.add(jdbcUrl, "2,3");
        
        databaseInfoPanel.add(new JLabel("Driver Name: "), "1,4");
        driverName = new JLabel();
        driverName.setEnabled(false);
        databaseInfoPanel.add(driverName, "2,4");
        
        databaseInfoPanel.add(new JLabel("User Name: "), "1,5");
        userName = new JLabel();
        userName.setEnabled(false);
        databaseInfoPanel.add(userName, "2,5");

        JPanel bottomStrut = new JPanel();
        bottomStrut.setPreferredSize(new Dimension(100, 500));
        this.add(bottomStrut);

    }

    public void setup(SymmetricDatabase c) {
        jdbcUrl.setText(c.getJdbcUrl());
        driverName.setText(c.getDriverName());
        databaseName.setText(c.getName());
        userName.setText(c.getUserName());
        tablePrefix.setText(c.getTablePrefix());
        this.repaint();
    }

    public ScreenName getScreenName() {
        return ScreenName.INFO;
    }

}
