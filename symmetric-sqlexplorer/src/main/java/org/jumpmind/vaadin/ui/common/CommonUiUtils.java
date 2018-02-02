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

import static org.apache.commons.lang.StringUtils.abbreviate;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.servlet.ServletContext;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vaadin.aceeditor.AceEditor;

import com.vaadin.v7.data.Container;
import com.vaadin.v7.data.Property;
import com.vaadin.v7.data.util.converter.Converter;
import com.vaadin.v7.data.util.converter.StringToBigDecimalConverter;
import com.vaadin.v7.data.util.converter.StringToLongConverter;
import com.vaadin.server.Page;
import com.vaadin.server.Sizeable.Unit;
import com.vaadin.server.VaadinServlet;
import com.vaadin.shared.Position;
import com.vaadin.v7.ui.AbstractSelect;
import com.vaadin.v7.ui.AbstractTextField.TextChangeEventMode;
import com.vaadin.ui.Button;
import com.vaadin.v7.ui.Grid;
import com.vaadin.v7.ui.Grid.Column;
import com.vaadin.v7.ui.Grid.SelectionMode;
import com.vaadin.ui.Label;
import com.vaadin.v7.ui.NativeSelect;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Notification.Type;
import com.vaadin.ui.TabSheet;
import com.vaadin.v7.ui.Table;
import com.vaadin.v7.ui.Table.CellStyleGenerator;
import com.vaadin.ui.themes.ValoTheme;

public final class CommonUiUtils {

    final static Logger log = LoggerFactory.getLogger(CommonUiUtils.class);

    static final FastDateFormat DATETIMEFORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    static final FastDateFormat TIMEFORMAT = FastDateFormat.getInstance("HH:mm:ss.SSS");

    static final String NULL_TEXT = "<null>";

    private CommonUiUtils() {
    }

    public static void styleTabSheet(TabSheet tabSheet) {
        tabSheet.setSizeFull();
        tabSheet.addStyleName(ValoTheme.TABSHEET_FRAMED);
        tabSheet.addStyleName(ValoTheme.TABSHEET_COMPACT_TABBAR);
        tabSheet.addStyleName(ValoTheme.TABSHEET_PADDED_TABBAR);
    }

    public static TabSheet createTabSheet() {
        TabSheet tabSheet = new TabSheet();
        styleTabSheet(tabSheet);
        return tabSheet;
    }

    public static Button createPrimaryButton(String name) {
        return createPrimaryButton(name, null);
    }

    public static Button createPrimaryButton(String name, Button.ClickListener listener) {
        Button button = new Button(name);
        if (listener != null) {
            button.addClickListener(listener);
        }
        button.addStyleName(ValoTheme.BUTTON_PRIMARY);
        return button;
    }

    public static Table createTable() {
        Table table = new Table() {

            private static final long serialVersionUID = 1L;

            @Override
            protected String formatPropertyValue(Object rowId, Object colId, Property<?> property) {
                if (property.getValue() != null) {
                    if (property.getType() == Date.class) {
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss aaa");
                        return df.format((Date) property.getValue());
                    } else if (Number.class.isAssignableFrom(property.getType())) {
                        return property.getValue().toString();
                    }
                }
                return super.formatPropertyValue(rowId, colId, property);
            }

        };
        
        table.setCellStyleGenerator(new CellStyleGenerator() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public String getStyle(Table source, Object itemId, Object propertyId) {
				if (propertyId != null && propertyId.equals("#")) {
					return "rowheader";
				}
				return null;
			}
		});
        
        return table;
    }

    public static AceEditor createAceEditor() {
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
    }

    public static void notify(String message) {
        notify("", message, Type.HUMANIZED_MESSAGE);
    }

    public static void notify(String caption, String message) {
        notify(caption, message, Type.HUMANIZED_MESSAGE);
    }

    public static void notify(String message, Type type) {
        notify("", message, type);
    }

    public static void notify(String caption, String message, Type type) {
        notify(caption, message, null, type);
    }

    public static void notify(String caption, String message, Throwable ex, Type type) {
        Page page = Page.getCurrent();
        if (page != null) {
            Notification notification = new Notification(caption, contactWithLineFeed(FormatUtils.wordWrap(message, 150)),
                    Type.HUMANIZED_MESSAGE);
            notification.setPosition(Position.MIDDLE_CENTER);
            notification.setDelayMsec(-1);

            String style = ValoTheme.NOTIFICATION_SUCCESS;
            if (type == Type.ERROR_MESSAGE) {
                style = ValoTheme.NOTIFICATION_FAILURE;
            } else if (type == Type.WARNING_MESSAGE) {
                style = ValoTheme.NOTIFICATION_WARNING;
            }
            notification.setStyleName(notification.getStyleName() + " " + ValoTheme.NOTIFICATION_CLOSABLE + " " + style);
            notification.show(Page.getCurrent());
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
        notify("An error occurred", message, ex, Type.ERROR_MESSAGE);
    }

    public static void notify(Throwable ex) {
        notify("An unexpected error occurred", "See the log file for additional details", ex, Type.ERROR_MESSAGE);
    }

    public static Object getObject(ResultSet rs, int i) throws SQLException {
        Object obj = JdbcSqlTemplate.getResultSetValue(rs, rs.getMetaData(), i, false);
        if (obj instanceof byte[]) {
            obj = new String(Hex.encodeHex((byte[]) obj));
        }

        if (obj instanceof String) {
            obj = abbreviate((String) obj, 1024 * 4);
        }
        return obj;
    }

    public static Table putResultsInTable(final ResultSet rs, int maxResultSize, final boolean showRowNumbers, String... excludeValues)
            throws SQLException {
        try {
            final Table table = createTable();
            table.setImmediate(true);
            table.setSortEnabled(true);
            table.setSelectable(true);
            table.setMultiSelect(true);
            table.setColumnReorderingAllowed(true);
            table.setColumnReorderingAllowed(true);
            table.setColumnCollapsingAllowed(true);

            final ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            table.addContainerProperty("#", Integer.class, null);
            Set<String> columnNames = new HashSet<String>();
            Set<Integer> skipColumnIndexes = new HashSet<Integer>();
            int[] types = new int[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                String realColumnName = meta.getColumnName(i);
                String columnName = realColumnName;
                if (!Arrays.asList(excludeValues).contains(columnName)) {

                    int index = 1;
                    while (columnNames.contains(columnName)) {
                        columnName = realColumnName + "_" + index++;
                    }
                    columnNames.add(columnName);

                    Class<?> typeClass = Object.class;
                    int type = meta.getColumnType(i);
                    types[i - 1] = type;
                    switch (type) {
                        case Types.FLOAT:
                        case Types.DOUBLE:
                        case Types.NUMERIC:
                        case Types.REAL:
                        case Types.DECIMAL:
                            typeClass = BigDecimal.class;
                            break;
                        case Types.TINYINT:
                        case Types.SMALLINT:
                        case Types.BIGINT:
                        case Types.INTEGER:
                            typeClass = Long.class;
                            break;
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.NVARCHAR:
                        case Types.NCHAR:
                        case Types.CLOB:
                            typeClass = String.class;
                        default:
                            break;
                    }
                    table.addContainerProperty(i, typeClass, null);
                    table.setColumnHeader(i, columnName);
                } else {
                    skipColumnIndexes.add(i - 1);
                }

            }
            int rowNumber = 1;
            while (rs.next() && rowNumber <= maxResultSize) {
                Object[] row = new Object[columnNames.size() + 1];
                row[0] = new Integer(rowNumber);
                int rowIndex = 1;
                for (int i = 0; i < columnCount; i++) {
                    if (!skipColumnIndexes.contains(i)) {
                        Object o = getObject(rs, i + 1);
                        int type = types[i];
                        switch (type) {
                            case Types.FLOAT:
                            case Types.DOUBLE:
                            case Types.REAL:
                            case Types.NUMERIC:
                            case Types.DECIMAL:
                                if (o == null) {
                                    o = new BigDecimal(-1);
                                }
                                if (!(o instanceof BigDecimal)) {
                                    o = new BigDecimal(castToNumber(o.toString()));
                                }
                                break;
                            case Types.TINYINT:
                            case Types.SMALLINT:
                            case Types.BIGINT:
                            case Types.INTEGER:
                                if (o == null) {
                                    o = new Long(-1);
                                }

                                if (!(o instanceof Long)) {
                                    o = new Long(castToNumber(o.toString()));
                                }
                                break;
                            default:
                                break;
                        }
                        row[rowIndex] = o == null ? NULL_TEXT : o;
                        rowIndex++;
                    }
                }
                table.addItem(row, rowNumber);
                rowNumber++;
            }

            if (rowNumber < 100) {
                table.setColumnWidth("#", 18);
            } else if (rowNumber < 1000) {
                table.setColumnWidth("#", 25);
            } else {
                table.setColumnWidth("#", 30);
            }

            if (!showRowNumbers) {
                table.setColumnCollapsed("#", true);
            }

            return table;
        } finally {
            JdbcSqlTemplate.close(rs);
        }
    }

    public static String[] getHeaderCaptions(Grid grid) {
        List<String> headers = new ArrayList<String>();
        List<Column> columns = grid.getColumns();
        for (Column column : columns) {
            headers.add(column.getHeaderCaption());
        }
        return headers.toArray(new String[headers.size()]);
    }

	public static Grid putResultsInGrid(final ResultSet rs, org.jumpmind.db.model.Table resultTable, int maxResultSize, final boolean showRowNumbers, String... excludeValues)
            throws SQLException {

        final Grid grid = new Grid();
        grid.setSelectionMode(SelectionMode.MULTI);
        grid.setColumnReorderingAllowed(true);
        grid.setData(new HashMap<Object, List<Object>>());

        final ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        grid.addColumn("#", Integer.class).setHeaderCaption("#").setHidable(true);
        Set<String> columnNames = new HashSet<String>();
        Set<Integer> skipColumnIndexes = new HashSet<Integer>();
        int[] types = new int[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            String realColumnName = meta.getColumnName(i);
            String columnName = realColumnName;
            if (!Arrays.asList(excludeValues).contains(columnName)) {

                int index = 1;
                while (columnNames.contains(columnName)) {
                    columnName = realColumnName + "_" + index++;
                }
                columnNames.add(columnName);

                Class<?> typeClass = Object.class;
                int type = meta.getColumnType(i);
                types[i - 1] = type;
                switch (type) {
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.REAL:
                    case Types.DECIMAL:
                        typeClass = BigDecimal.class;
                        break;
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                    case Types.INTEGER:
                        typeClass = Long.class;
                        break;
                    case Types.VARCHAR:
                    case Types.CHAR:
                    case Types.NVARCHAR:
                    case Types.NCHAR:
                    case Types.CLOB:
                        typeClass = String.class;
                    default:
                        break;
                }
                Column column = grid.addColumn(columnName, typeClass).setHeaderCaption(columnName).setHidable(true);
                if (typeClass.equals(Long.class)) {
                    column.setConverter(new StringToLongConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public String convertToPresentation(Long value, Class<? extends String> targetType, Locale locale)
                                throws com.vaadin.v7.data.util.converter.Converter.ConversionException {
                            if (value == null) {
                                return NULL_TEXT;
                            } else {
                                return value.toString();
                            }
                        }
                    });
                } else if (typeClass.equals(BigDecimal.class)) {
                    column.setConverter(new StringToBigDecimalConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public String convertToPresentation(BigDecimal value, Class<? extends String> targetType, Locale locale)
                                throws com.vaadin.v7.data.util.converter.Converter.ConversionException {
                            if (value == null) {
                                return NULL_TEXT;
                            } else {
                                return value.toString();
                            }
                        }
                    });
                } else {
                	column.setConverter(new Converter<String, Object>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Object convertToModel(String value, Class<? extends Object> targetType, Locale locale)
								throws com.vaadin.v7.data.util.converter.Converter.ConversionException {
							return null;
						}

						@Override
						public String convertToPresentation(Object value, Class<? extends String> targetType, Locale locale)
								throws com.vaadin.v7.data.util.converter.Converter.ConversionException {
							if (value == null) {
								return NULL_TEXT;
							} else {
								return value.toString();
							}
						}

						@Override
						public Class<Object> getModelType() {
							return Object.class;
						}

						@Override
						public Class<String> getPresentationType() {
							return String.class;
						}
                		
                	});
                }
            } else {
                skipColumnIndexes.add(i - 1);
            }

        }
        int rowNumber = 1;
        while (rs.next() && rowNumber <= maxResultSize) {
            Object[] row = new Object[columnNames.size() + 1];
            row[0] = new Integer(rowNumber);
            int rowIndex = 1;
            for (int i = 0; i < columnCount; i++) {
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
                    row[rowIndex] = o;
                    rowIndex++;
                }
            }
            grid.addRow(row);
            rowNumber++;
        }

        if (rowNumber < 100) {
            grid.getColumn("#").setWidth(75);
        } else if (rowNumber < 1000) {
            grid.getColumn("#").setWidth(95);
        } else {
            grid.getColumn("#").setWidth(115);
        }

        if (!showRowNumbers) {
            grid.getColumn("#").setHidden(true);
        } else {
            grid.setFrozenColumnCount(1);
        }

        
        
        return grid;
    }

    protected static String castToNumber(String value) {
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

    public static void addItems(List<?> items, Container container) {
        for (Object item : items) {
            container.addItem(item);
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

    public static Label createSeparator() {
        Label separator = new Label(" ");
        separator.setStyleName("vrule");
        separator.setHeight(100, Unit.PERCENTAGE);
        separator.setWidthUndefined();
        return separator;
    }

    public static AbstractSelect createComboBox() {
        return createComboBox(null);
    }

    public static AbstractSelect createComboBox(String name) {
        NativeSelect cb = name == null ? new NativeSelect() : new NativeSelect(name);
        cb.setImmediate(true);
        cb.setWidth(16, Unit.EM);
        cb.setHeight(2.15f, Unit.EM);
        cb.setNullSelectionAllowed(false);
        return cb;
    }
}
