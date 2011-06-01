package org.jumpmind.symmetric.core.db;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.core.common.Base64;
import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.common.DateUtils;
import org.jumpmind.symmetric.core.common.Hex;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;

abstract public class AbstractDbDialect implements IDbDialect {

    protected final Log log = LogFactory.getLog(getClass());

    public static final String REQUIRED_FIELD_NULL_SUBSTITUTE = " ";

    public static final String[] TIMESTAMP_PATTERNS = { "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd" };

    public static final String[] TIME_PATTERNS = { "HH:mm:ss.S", "HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss" };

    protected DbDialectInfo platformInfo = new DbDialectInfo();

    protected Database cachedModel = new Database();

    protected String defaultSchema;

    protected String defaultCatalog;

    protected AbstractTableBuilder tableBuilder;

    protected IDataCaptureBuilder dataCaptureBuilder;

    protected Parameters parameters;

    public AbstractDbDialect(Parameters parameters) {
        this.parameters = parameters == null ? new Parameters() : parameters;

    }

    public Parameters getParameters() {
        return parameters;
    }

    public IDataCaptureBuilder getDataCaptureBuilder() {
        return dataCaptureBuilder;
    }

    public AbstractTableBuilder getTableBuilder() {
        return tableBuilder;
    }

    public boolean isLob(int type) {
        return type == Types.CLOB || type == Types.BLOB || type == Types.BINARY
                || type == Types.VARBINARY || type == Types.LONGVARBINARY ||
                // SQL-Server ntext binary type
                type == -10;
    }

    public void alter(boolean failOnError, Table... tables) {
        String alterSql = getAlterScriptFor(tables);
        SqlScript script = new SqlScript(alterSql, this, failOnError, ";", null);
        script.execute();
    }

    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData) {
        List<Object> list = new ArrayList<Object>(values.length);
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            Object objectValue = value;
            Column column = orderedMetaData[i];
            try {
                if (column != null) {
                    int type = column.getTypeCode();
                    if ((value == null || (platformInfo.isEmptyStringNulled() && value.equals("")))
                            && column.isRequired() && column.isOfTextType()) {
                        objectValue = REQUIRED_FIELD_NULL_SUBSTITUTE;
                    }
                    if (value != null) {
                        if (type == Types.DATE && !platformInfo.isDateOverridesToTimestamp()) {
                            objectValue = getDate(value, TIMESTAMP_PATTERNS);
                        } else if (type == Types.TIMESTAMP
                                || (type == Types.DATE && platformInfo.isDateOverridesToTimestamp())) {
                            objectValue = new Timestamp(getTime(value, TIMESTAMP_PATTERNS));
                        } else if (type == Types.CHAR) {
                            String charValue = value.toString();
                            if ((StringUtils.isBlank(charValue) && platformInfo
                                    .isBlankCharColumnSpacePadded())
                                    || (!StringUtils.isBlank(charValue) && platformInfo
                                            .isNonBlankCharColumnSpacePadded())) {
                                objectValue = StringUtils.rightPad(value.toString(),
                                        column.getSizeAsInt(), ' ');
                            }
                        } else if (type == Types.INTEGER || type == Types.SMALLINT
                                || type == Types.BIT) {
                            objectValue = Integer.valueOf(value);
                        } else if (type == Types.NUMERIC || type == Types.DECIMAL
                                || type == Types.FLOAT || type == Types.DOUBLE) {
                            // The number will have either one period or one
                            // comma
                            // for the decimal point, but we need a period
                            objectValue = new BigDecimal(value.replace(',', '.'));
                        } else if (type == Types.BOOLEAN) {
                            objectValue = value.equals("1") ? Boolean.TRUE : Boolean.FALSE;
                        } else if (type == Types.BLOB || type == Types.LONGVARBINARY
                                || type == Types.BINARY || type == Types.VARBINARY) {
                            if (encoding == BinaryEncoding.NONE) {
                                objectValue = value.getBytes();
                            } else if (encoding == BinaryEncoding.BASE64) {
                                objectValue = Base64.decodeBase64(value.getBytes());
                            } else if (encoding == BinaryEncoding.HEX) {
                                objectValue = Hex.decodeHex(value.toCharArray());
                            }
                        } else if (type == Types.TIME) {
                            objectValue = new Time(getTime(value, TIME_PATTERNS));
                        } else if (type == Types.ARRAY) {
                            objectValue = createArray(column, value);
                        }
                    }
                    list.add(objectValue);
                }
            } catch (Exception ex) {
                log.log(LogLevel.ERROR, "Could not convert a value of %s for column %s of type %s",
                        value, column.getName(), column.getType());
                throw new RuntimeException(ex);
            }
        }

        return list.toArray();
    }

    protected Array createArray(Column column, final String value) {
        return null;
    }

    final private java.util.Date getDate(String value, String[] pattern) {
        try {
            return DateUtils.parseDate(value, pattern);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    final private long getTime(String value, String[] pattern) {
        return getDate(value, pattern).getTime();
    }

    public DbDialectInfo getPlatformInfo() {
        return platformInfo;
    }

    /**
     * Returns the constraint name. This method takes care of length limitations
     * imposed by some databases.
     * 
     * @param prefix
     *            The constraint prefix, can be <code>null</code>
     * @param table
     *            The table that the constraint belongs to
     * @param secondPart
     *            The second name part, e.g. the name of the constraint column
     * @param suffix
     *            The constraint suffix, e.g. a counter (can be
     *            <code>null</code>)
     * @return The constraint name
     */
    public String getConstraintName(String prefix, Table table, String secondPart, String suffix) {
        StringBuffer result = new StringBuffer();

        if (prefix != null) {
            result.append(prefix);
            result.append("_");
        }
        result.append(table.getTableName());
        result.append("_");
        result.append(secondPart);
        if (suffix != null) {
            result.append("_");
            result.append(suffix);
        }
        return shortenName(result.toString(), getPlatformInfo().getMaxConstraintNameLength());
    }

    /**
     * Generates a version of the name that has at most the specified length.
     * 
     * @param name
     *            The original name
     * @param desiredLength
     *            The desired maximum length
     * @return The shortened version
     */
    public String shortenName(String name, int desiredLength) {
        // TODO: Find an algorithm that generates unique names
        int originalLength = name.length();

        if ((desiredLength <= 0) || (originalLength <= desiredLength)) {
            return name;
        }

        int delta = originalLength - desiredLength;
        int startCut = desiredLength / 2;

        StringBuffer result = new StringBuffer();

        result.append(name.substring(0, startCut));
        if (((startCut == 0) || (name.charAt(startCut - 1) != '_'))
                && ((startCut + delta + 1 == originalLength) || (name.charAt(startCut + delta + 1) != '_'))) {
            // just to make sure that there isn't already a '_' right before or
            // right after the cutting place (which would look odd with an
            // additional one)
            result.append("_");
        }
        result.append(name.substring(startCut + delta + 1, originalLength));
        return result.toString();
    }

    public void resetCachedTableModel() {
        synchronized (this.getClass()) {
            this.cachedModel.resetTableIndexCache();
            Table[] tables = this.cachedModel.getTables();
            if (tables != null) {
                for (Table table : tables) {
                    this.cachedModel.removeTable(table);
                }
            }
        }
    }

    public String getDefaultCatalog() {
        return defaultCatalog;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultCatalog(String defaultCatalog) {
        this.defaultCatalog = defaultCatalog;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }        

}
