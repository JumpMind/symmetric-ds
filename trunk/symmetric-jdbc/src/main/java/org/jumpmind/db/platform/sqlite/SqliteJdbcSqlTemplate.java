package org.jumpmind.db.platform.sqlite;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.lob.LobHandler;

public class SqliteJdbcSqlTemplate extends JdbcSqlTemplate {

    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
    private DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public SqliteJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings, SymmetricLobHandler lobHandler,
            DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
    }

    @Override
    public boolean isUniqueKeyViolation(Throwable ex) {
        SQLException sqlEx = findSQLException(ex);
        return (sqlEx != null && sqlEx.getMessage() != null && sqlEx.getMessage().contains("[SQLITE_CONSTRAINT]"));
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "SELECT last_insert_rowid();";
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObjectFromResultSet(ResultSet rs, Class<T> clazz) throws SQLException {        
        if (Date.class.isAssignableFrom(clazz) || Timestamp.class.isAssignableFrom(clazz)) {

            String s = rs.getString(1);
            Date d = null;
            try {
                d = dateTimeFormat.parse(s);
            } catch (ParseException e) {
                try {
                    d =  timeFormat.parse(s);
                } catch (ParseException f) {
                    try {
                        d = dateFormat.parse(s);
                    } catch (ParseException g) {
                    }
                }
            }
            if (Timestamp.class.isAssignableFrom(clazz)) {
                return (T) new Timestamp(d.getTime());
            } else {
                return (T) d;
            }
        } else {
            return super.getObjectFromResultSet(rs, clazz);
        }

    }
    
    @Override
    public void setValues(PreparedStatement ps, Object[] args, int[] argTypes,
            LobHandler lobHandler) throws SQLException {
        for (int i = 1; i <= args.length; i++) {
            Object arg = args[i - 1];
            int argType = argTypes != null && argTypes.length >= i ? argTypes[i - 1] : SqlTypeValue.TYPE_UNKNOWN;
            if (argType == Types.BLOB && lobHandler != null && arg instanceof byte[]) {
                lobHandler.getLobCreator().setBlobAsBytes(ps, i, (byte[]) arg);
            } else if (argType == Types.BLOB && lobHandler != null && arg instanceof String) {
                lobHandler.getLobCreator().setBlobAsBytes(ps, i, arg.toString().getBytes());
            } else if (argType == Types.CLOB && lobHandler != null) {
                lobHandler.getLobCreator().setClobAsString(ps, i, (String) arg);
            } else if (argType == Types.DATE && arg!=null ) {
                StatementCreatorUtils.setParameterValue(ps, i, verifyArgType(Types.VARCHAR), dateFormat.format(arg));
            } else if (argType == Types.TIME && arg!=null ) {
                StatementCreatorUtils.setParameterValue(ps, i, verifyArgType(Types.VARCHAR), timeFormat.format(arg));
            } else if (argType == Types.TIMESTAMP && arg!=null ) {
                  StatementCreatorUtils.setParameterValue(ps, i, verifyArgType(Types.VARCHAR), dateTimeFormat.format(arg));
            } else {
                StatementCreatorUtils.setParameterValue(ps, i, verifyArgType(argType), arg);
            }
        }
    }

}
