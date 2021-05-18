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

import static org.apache.commons.lang3.StringUtils.abbreviate;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.time.FastDateFormat;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vaadin.aceeditor.AceEditor;

import com.vaadin.flow.component.ClickEvent;
import com.vaadin.flow.component.ComponentEventListener;
import com.vaadin.flow.component.UI;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.grid.Grid.Column;
import com.vaadin.flow.component.grid.Grid.SelectionMode;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.Notification.Position;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.page.Page;

public final class CommonUiUtils {

    final static Logger log = LoggerFactory.getLogger(CommonUiUtils.class);

    static final FastDateFormat DATETIMEFORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    static final FastDateFormat TIMEFORMAT = FastDateFormat.getInstance("HH:mm:ss.SSS");

    public static final String NULL_TEXT = "<null>";

    private CommonUiUtils() {
    }

    /*public static void styleTabSheet(TabSheet tabSheet) {
        tabSheet.setSizeFull();
        tabSheet.addClassName(ValoTheme.TABSHEET_FRAMED);
        tabSheet.addClassName(ValoTheme.TABSHEET_COMPACT_TABBAR);
        tabSheet.addClassName(ValoTheme.TABSHEET_PADDED_TABBAR);
    }*/

    /*public static TabSheet createTabSheet() {
        TabSheet tabSheet = new TabSheet();
        styleTabSheet(tabSheet);
        return tabSheet;
    }*/

    public static Button createPrimaryButton(String name) {
        return createPrimaryButton(name, null);
    }

    public static Button createPrimaryButton(String name, ComponentEventListener<ClickEvent<Button>> listener) {
        Button button = new Button(name);
        if (listener != null) {
            button.addClickListener(listener);
        }
        button.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        return button;
    }

    /*public static AceEditor createAceEditor() {
        AceEditor editor = new AceEditor();
        editor.setSizeFull();
        ServletContext context = VaadinServlet.getCurrent().getServletContext();
        if (context.getRealPath("/VAADIN/ace") != null) {
            String acePath = context.getContextPath() + "/VAADIN/ace";
            editor.setThemePath(acePath);
            editor.setModePath(acePath);
            editor.setWorkerPath(acePath);
        } else {
            log.warn("Could not find a local version of the ace editor.  " + "You might want to consider installing the ace web artifacts at "
                    + context.getRealPath(""));
        }
        editor.setHighlightActiveLine(true);
        editor.setShowPrintMargin(false);
        return editor;
    }*/

    public static void notify(String message) {
        notify("", message, NotificationVariant.LUMO_SUCCESS);
    }

    public static void notify(String caption, String message) {
        notify(caption, message, NotificationVariant.LUMO_SUCCESS);
    }

    public static void notify(String message, NotificationVariant type) {
        notify("", message, type);
    }

    public static void notify(String caption, String message, NotificationVariant type) {
        notify(caption, message, null, type);
    }

    public static void notify(String caption, String message, Throwable ex, NotificationVariant type) {
        Page page = UI.getCurrent().getPage();
        if (page != null) {
            //Notification notification = new Notification(caption, contactWithLineFeed(FormatUtils.wordWrap(message, 150)));
            Notification notification = new Notification();
            notification.setText(caption + "\n" + contactWithLineFeed(FormatUtils.wordWrap(message, 150)));
            notification.setPosition(Position.MIDDLE);
            notification.setDuration(-1);
            //notification.setClassName(notification.getStyleName() + " " + ValoTheme.NOTIFICATION_CLOSABLE);
            notification.addThemeVariants(type);
            notification.open();
        }
    }

    private static String contactWithLineFeed(String[] lines) {
        StringBuilder line = new StringBuilder();
        for (String l : lines) {
            line.append(l).append("\n");
        }
        return line.toString();
    }

    public static void notify(String message, Throwable ex) {
        notify("An error occurred", message, ex, NotificationVariant.LUMO_ERROR);
    }

    public static void notify(Throwable ex) {
        notify("An unexpected error occurred", "See the log file for additional details", ex, NotificationVariant.LUMO_ERROR);
    }

    public static Object getObject(ResultSet rs, int i) throws SQLException {
        Object obj = JdbcSqlTemplate.getResultSetValue(rs, rs.getMetaData(), i, false, false);
        if (obj instanceof byte[]) {
            obj = new String(Hex.encodeHex((byte[]) obj));
        }

        if (obj instanceof String) {
            obj = abbreviate((String) obj, 1024 * 4);
        }
        return obj;
    }

    public static String[] getHeaderCaptions(Grid<?> grid) {
        List<String> headers = new ArrayList<String>();
        for (Column<?> column : grid.getColumns()) {
            headers.add(column.getKey());
        }
        return headers.toArray(new String[headers.size()]);
    }
    
    public static Grid<List<Object>> putResultsInGrid(final ResultSet rs, int maxResultSize, final boolean showRowNumbers,
            String... excludeValues) throws SQLException {
        final Grid<List<Object>> grid = new Grid<List<Object>>();
        grid.setSelectionMode(SelectionMode.MULTI);
        grid.setColumnReorderingAllowed(true);
        grid.addItemClickListener(event -> {
            if (event.getColumn() != null) {
                grid.deselectAll();
                grid.select(event.getItem());
            }
        });
        
        List<List<Object>> outerList = new ArrayList<List<Object>>();
        if (rs != null) {
            grid.addColumn(row -> {
                return outerList.indexOf(row) + 1;
            }).setHeader("#").setKey("#").setClassNameGenerator(row -> {
                if (!grid.getSelectedItems().contains(row)) {
                    return "rowheader";
                }
                return null;
            }).setFrozen(true).setVisible(showRowNumbers);
            
            final ResultSetMetaData meta = rs.getMetaData();
            int totalColumns = meta.getColumnCount();
            Set<Integer> skipColumnIndexes = new HashSet<Integer>();
            Set<String> columnNames = new HashSet<String>();
            int[] types = new int[totalColumns];
            final int[] columnCounter = {1};
            while (columnCounter[0] <= totalColumns) {
                String realColumnName = meta.getColumnName(columnCounter[0]);
                String columnName = realColumnName;
                if (!Arrays.asList(excludeValues).contains(columnName)) {
                    int index = 1;
                    while (columnNames.contains(columnName)) {
                        columnName = realColumnName + "_" + index++;
                    }
                    columnNames.add(columnName);
                    
                    Integer colNum = new Integer(columnCounter[0] - 1 - skipColumnIndexes.size());
                    grid.addColumn(row -> row.get(colNum)).setKey(columnName).setHeader(columnName).setClassNameGenerator(row -> {
                        if (row.get(colNum) == null) {
                            return "italics";
                        }
                        return null;
                    }).setVisible(false);
                    
                    types[columnCounter[0] - 1] = meta.getColumnType(columnCounter[0]);
                } else {
                    skipColumnIndexes.add(columnCounter[0] - 1);
                }
                columnCounter[0]++;
            }
            
            for (int rowNumber = 1; rs.next() && rowNumber <= maxResultSize; rowNumber++) {
                List<Object> innerList = new ArrayList<Object>();
                
                for (int i = 0; i < totalColumns; i++) {
                    if (!skipColumnIndexes.contains(i)) {
                        Object o = getObject(rs, i + 1);
                        int type = types[i];
                        switch (type) {
                            case Types.FLOAT:
                            case Types.DOUBLE:
                            case Types.REAL:
                            case Types.NUMERIC:
                            case Types.DECIMAL:
                                if (o != null && !(o instanceof BigDecimal)) {
                                    o = new BigDecimal(castToNumber(o.toString()));
                                }
                                break;
                            case Types.TINYINT:
                            case Types.SMALLINT:
                            case Types.BIGINT:
                            case Types.INTEGER:
                                if (o != null && !(o instanceof Long)) {
                                    o = new Long(castToNumber(o.toString()));
                                }
                                break;
                            default:
                                break;
                        }
                        
                        innerList.add(o == null ? NULL_TEXT : o);
                    }
                }
                
                outerList.add(innerList);
                
                if (rowNumber < 100) {
                    grid.getColumnByKey("#").setWidth("75px");
                } else if (rowNumber < 1000) {
                    grid.getColumnByKey("#").setWidth("95px");
                } else {
                    grid.getColumnByKey("#").setWidth("115px");
                }
            }
        } else {
            grid.addColumn(row -> row.get(0)).setHeader("Status").setKey("Status");
            List<Object> innerList = new ArrayList<Object>();
            innerList.add("Metadata unavailable");
            outerList.add(innerList);
        }
        grid.setItems(outerList);
        return grid;
    }

    public static String castToNumber(String value) {
        if ("NO".equalsIgnoreCase(value) || "FALSE".equalsIgnoreCase(value)) {
            return "0";
        } else if ("YES".equalsIgnoreCase(value) || "TRUE".equalsIgnoreCase(value)) {
            return "1";
        } else {
            return value.replace(",", ".");
        }
    }

    public static String formatDuration(long timeInMs) {
        if (timeInMs > 60000) {
            long minutes = timeInMs / 60000;
            long seconds = (timeInMs - (minutes * 60000)) / 1000;
            return minutes + " m " + seconds + " s";
        } else if (timeInMs > 1000) {
            long seconds = timeInMs / 1000;
            return seconds + " s";
        } else {
            return timeInMs + " ms";
        }
    }

    public static String formatDateTime(Date dateTime) {
        if (dateTime != null) {
            Calendar cal = Calendar.getInstance();
            Calendar ref = Calendar.getInstance();
            ref.setTime(dateTime);
            if (ref.get(Calendar.DAY_OF_YEAR) == cal.get(Calendar.DAY_OF_YEAR) && ref.get(Calendar.YEAR) == cal.get(Calendar.YEAR)) {
                return TIMEFORMAT.format(dateTime);
            } else {
                return DATETIMEFORMAT.format(dateTime);
            }
        } else {
            return null;
        }
    }

    public static String getJdbcTypeValue(String type) {
        String value = null;
        if (type.equalsIgnoreCase("CHAR")) {
            value = "";
        } else if (type.equalsIgnoreCase("VARCHAR")) {
            value = "";
        } else if (type.equalsIgnoreCase("LONGVARCHAR")) {
            value = "";
        } else if (type.equalsIgnoreCase("NUMERIC")) {
            value = "0";
        } else if (type.equalsIgnoreCase("DECIMAL")) {
            value = "0.00";
        } else if (type.equalsIgnoreCase("BIT")) {
            value = "0";
        } else if (type.equalsIgnoreCase("BOOLEAN")) {
            value = "0";
        } else if (type.equalsIgnoreCase("TINYINT")) {
            value = "0";
        } else if (type.equalsIgnoreCase("SMALLINT")) {
            value = "0";
        } else if (type.equalsIgnoreCase("INTEGER")) {
            value = "0";
        } else if (type.equalsIgnoreCase("BIGINT")) {
            value = "0";
        } else if (type.equalsIgnoreCase("REAL")) {
            value = "0";
        } else if (type.equalsIgnoreCase("DOUBLE")) {
            value = "0.0";
        } else if (type.equalsIgnoreCase("BINARY")) {
            value = null;
        } else if (type.equalsIgnoreCase("VARBINARY")) {
            value = null;
        } else if (type.equalsIgnoreCase("LONGBINARY")) {
            value = null;
        } else if (type.equalsIgnoreCase("DATE")) {
            value = "'2014-07-08'";
        } else if (type.equalsIgnoreCase("TIME")) {
            value = "'12:00:00'";
        } else if (type.equalsIgnoreCase("TIMESTAMP")) {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = new Date();
            value = dateFormat.format(date);
        } else if (type.equalsIgnoreCase("CLOB")) {
            value = "''";
        } else if (type.equalsIgnoreCase("BLOB")) {
            value = "''";
        } else if (type.equalsIgnoreCase("ARRAY")) {
            value = "[]";
        } else if (type.equalsIgnoreCase("DISTINCT")) {
            value = null;
        } else if (type.equalsIgnoreCase("STRUCT")) {
            value = null;
        } else if (type.equalsIgnoreCase("REF")) {
            value = null;
        } else if (type.equalsIgnoreCase("DATALINK")) {
            value = null;
        } else if (type.equalsIgnoreCase("JAVA_OBJECT")) {
            value = null;
        } else {
            value = null;
        }
        return value;
    }

    public static Span createSeparator() {
        Span separator = new Span(" ");
        separator.setClassName("vrule");
        separator.setHeightFull();
        separator.setWidth(null);
        return separator;
    }
}
