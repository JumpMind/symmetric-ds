package org.jumpmind.symmetric.load;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * An instance of TableTemplate knows how to insert, update, and delete from a
 * single table. It uses metadata from the IDbDialect to get the columns for the
 * table in the target database in case the table is missing or has missing
 * columns. It uses a cache of StatementBuilder objects that contain the SQL and
 * PreparedStatement.
 * 
 * @author elong
 * 
 */
public class TableTemplate {
    public static final String REQUIRED_FIELD_NULL_SUBSTITUTE = " ";
    
    public static final String[] DATE_PATTERNS = { "yyyy-MM-dd" };
    
    public static final String[] TIMESTAMP_PATTERNS = { "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss" };

    private JdbcTemplate jdbcTemplate;

    private IDbDialect dbDialect;

    private Table table;

    private String[] keyNames;

    private String[] columnNames;

    private String[] existKeyNames;

    private String[] existColumnNames;

    private Map<String, Column> allMetaData;

    private Column[] keyMetaData;

    private Column[] columnMetaData;

    private HashMap<DmlType, StatementBuilder> statementMap;
    
    public TableTemplate(JdbcTemplate jdbcTemplate, IDbDialect dbDialect, String tableName) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbDialect = dbDialect;
        // TODO should we be passing the schema in the csv?
        table = dbDialect.getMetaDataFor(null, tableName, true);
        allMetaData = new HashMap<String, Column>();
        statementMap = new HashMap<DmlType, StatementBuilder>();

        if (table != null) {
            for (Column column : table.getColumns()) {
                allMetaData.put(column.getName().trim().toUpperCase(), column);
            }
        }
    }

    public String getTableName() {
        return table.getName();
    }

    public boolean isIgnoreThisTable() {
        return table == null;
    }

    public int insert(String[] columnValues) {
        StatementBuilder st = getStatementBuilder(DmlType.INSERT);
        Object[] values = filterValues(columnMetaData, columnValues);
        return jdbcTemplate.update(st.getSql(), values);
    }

    public int update(String[] columnValues, String[] keyValues) {
        StatementBuilder st = getStatementBuilder(DmlType.UPDATE);
        Object[] values = ArrayUtils.addAll(filterValues(columnMetaData, columnValues),
                filterValues(keyMetaData, keyValues));
        return jdbcTemplate.update(st.getSql(), values);
    }

    public int delete(String[] keyValues) {
        StatementBuilder st = getStatementBuilder(DmlType.DELETE);
        Object[] values = filterValues(keyMetaData, keyValues);
        return jdbcTemplate.update(st.getSql(), values);
    }

    private StatementBuilder getStatementBuilder(DmlType type) {
        StatementBuilder st = statementMap.get(type);
        if (st == null) {
            st = new StatementBuilder(type, table.getName(), existKeyNames, existColumnNames);
            statementMap.put(type, st);
        }
        return st;
    }

    private Object[] filterValues(Column[] metaData, String[] values) {
        List<Object> list = new ArrayList<Object>();
        
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            Object objectValue = value;
            Column column = metaData[i];

            if (column != null) {
                int type = column.getTypeCode();
                // TODO: should there be defaults for date and numeric types?
                if ((value == null || (dbDialect.isEmptyStringNulled() && value.equals("")))
                        && column.isRequired() && column.isOfTextType()) {
                    objectValue = REQUIRED_FIELD_NULL_SUBSTITUTE;
                } else if (value != null && type == Types.DATE) {
                    objectValue = new Date(getTime(value, DATE_PATTERNS));
                } else if (value != null && type == Types.TIMESTAMP) {
                    objectValue = new Timestamp(getTime(value, TIMESTAMP_PATTERNS));
                }
                list.add(objectValue);
            }
        }
        return list.toArray();
    }
    
    private long getTime(String value, String[] pattern) {
        try {
            return DateUtils.parseDate(value, pattern).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public void setKeyNames(String[] keyNames) {
        this.keyNames = keyNames;
        keyMetaData = getColumnMetaData(keyNames);
        existKeyNames = getExistColumnNames(keyMetaData);
        statementMap.clear();
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
        columnMetaData = getColumnMetaData(columnNames);
        existColumnNames = getExistColumnNames(columnMetaData);
        statementMap.clear();
    }
    
    private Column[] getColumnMetaData(String[] names) {
        Column[] columns = new Column[names.length];
        for (int i = 0; i < names.length; i++) {
            columns[i] = allMetaData.get(names[i].trim().toUpperCase());
        }
        return columns;
    }

    private String[] getExistColumnNames(Column[] columns) {
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                list.add(columns[i].getName());
            }
        }
        return list.toArray(new String[list.size()]);
    }

    public String[] getKeyNames() {
        return keyNames;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

}
