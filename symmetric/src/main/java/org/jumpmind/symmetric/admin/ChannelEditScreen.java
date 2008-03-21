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

import java.awt.BorderLayout;
import java.awt.Component;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.ListSelectionModel;
import javax.swing.border.TitledBorder;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableColumn;

import org.jumpmind.symmetric.admin.table.ChannelTableModel;
import org.jumpmind.symmetric.admin.table.ModelObjectTableModel;
import org.jumpmind.symmetric.admin.table.ValidationException;
import org.jumpmind.symmetric.model.Channel;

public class ChannelEditScreen extends AbstractScreen {

    private static final long serialVersionUID = -843273502686831584L;

    protected JTable table;

    protected JScrollPane scroller;

    protected ModelObjectTableModel<Channel> tableModel;

    int selectedRow = 0;

    public ChannelEditScreen() {

        tableModel = new ChannelTableModel();
        tableModel.addTableModelListener(new ChannelEditScreen.InteractiveTableModelListener());
        table = new JTable();
        table.setModel(tableModel);
        table.setSurrendersFocusOnKeystroke(true);
        if (!tableModel.hasEmptyRow()) {
            tableModel.addEmptyRow();
        }

        scroller = new javax.swing.JScrollPane(table);
        table.setPreferredScrollableViewportSize(new java.awt.Dimension(500, 300));
        TableColumn hidden = table.getColumnModel().getColumn(tableModel.getColumnCount() - 1);
        hidden.setMinWidth(2);
        hidden.setPreferredWidth(2);
        hidden.setMaxWidth(2);
        hidden.setCellRenderer(new InteractiveRenderer(tableModel.getColumnCount() - 1));

        table.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

        table.getSelectionModel().addListSelectionListener(tableModel);

        table.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent e) {
                if (!e.getValueIsAdjusting()) {
                    int oldRow = selectedRow;
                    selectedRow = table.getSelectedRow();
                    try {
                        tableModel.save();
                    } catch (ValidationException e1) {
                        table.getSelectionModel().setSelectionInterval(oldRow, oldRow);
                    }
                    if (!ChannelEditScreen.this.tableModel.hasEmptyRow()) {
                        ChannelEditScreen.this.tableModel.addEmptyRow();
                    }
                }
            }
        });

        int columnCount = tableModel.getColumnCount();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            TableColumn column = table.getColumnModel().getColumn(columnIndex);
            TableCellEditor editor = tableModel.getCellEditorForColumn(columnIndex);
            if (editor != null) {
                column.setCellEditor(editor);
            }
        }

        setLayout(new BorderLayout(5, 5));

        JPanel instructionsPanel = new JPanel();
        // TODO from resource bundle
        instructionsPanel.setBorder(new TitledBorder("Channels"));
        instructionsPanel.setLayout(new BorderLayout());
        JTextArea area = new JTextArea();
        area.setEditable(false);
        area.setOpaque(false);
        // TODO resource bundle
        area.setText("Channels are ...");
        instructionsPanel.add(area, BorderLayout.CENTER);

        add(instructionsPanel, BorderLayout.NORTH);
        add(scroller, BorderLayout.CENTER);
    }

    @Override
    public ScreenName getScreenName() {
        return ScreenName.CHANNELS;
    }

    @Override
    public void setup(SymmetricDatabase c) {
        tableModel.setup(c);
        this.table.revalidate();
        this.table.repaint();
    }

    public void highlightLastRow(int row) {
        int lastrow = tableModel.getRowCount();
        if (row == lastrow - 1) {
            table.setRowSelectionInterval(lastrow - 1, lastrow - 1);
        } else {
            table.setRowSelectionInterval(row + 1, row + 1);
        }

        table.setColumnSelectionInterval(0, 0);
    }

    @SuppressWarnings("serial")
    class InteractiveRenderer extends DefaultTableCellRenderer {
        protected int interactiveColumn;

        public InteractiveRenderer(int interactiveColumn) {
            this.interactiveColumn = interactiveColumn;
        }

        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
                boolean hasFocus, int row, int column) {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (column == interactiveColumn && hasFocus) {
                if ((ChannelEditScreen.this.tableModel.getRowCount() - 1) == row
                        && !ChannelEditScreen.this.tableModel.hasEmptyRow()) {
                    ChannelEditScreen.this.tableModel.addEmptyRow();
                }

                highlightLastRow(row);
            }

            return c;
        }
    }

    public class InteractiveTableModelListener implements TableModelListener {
        public void tableChanged(TableModelEvent evt) {
            if (evt.getType() == TableModelEvent.UPDATE) {
                int column = evt.getColumn();
                int row = evt.getFirstRow();
                table.setColumnSelectionInterval(column + 1, column + 1);
                table.setRowSelectionInterval(row, row);
            }
        }
    }

}
