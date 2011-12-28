package org.jumpmind.db.platform.oracle;

import java.io.OutputStream;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.jdbc.DefaultNativeConnectionExtractor;
import org.jumpmind.db.sql.jdbc.ILobHandler;
import org.jumpmind.db.sql.jdbc.INativeConnectionExtractor;

public class OracleLobHandler implements ILobHandler {

    private Class<?> blobClass;

    private Class<?> clobClass;

    private static final String BLOB_CLASS_NAME = "oracle.sql.BLOB";

    private static final String CLOB_CLASS_NAME = "oracle.sql.CLOB";

    private static final String DURATION_SESSION_FIELD_NAME = "DURATION_SESSION";

    private static final String MODE_READWRITE_FIELD_NAME = "MODE_READWRITE";

    private static final String MODE_READONLY_FIELD_NAME = "MODE_READONLY";

    private Boolean cache = Boolean.TRUE;

    private final Map<Class<?>, Integer> durationSessionConstants = new HashMap<Class<?>, Integer>(
            2);

    private final Map<Class<?>, Integer> modeReadWriteConstants = new HashMap<Class<?>, Integer>(2);

    private final Map<Class<?>, Integer> modeReadOnlyConstants = new HashMap<Class<?>, Integer>(2);

    private INativeConnectionExtractor nativeConnectionExtractor = new DefaultNativeConnectionExtractor();

    public void setBlobAsBytes(PreparedStatement ps, int paramIndex, final byte[] content)
            throws SQLException {

        if (content != null) {
            Blob blob = (Blob) createLob(ps, false, new LobCallback() {
                public void populateLob(Object lob) throws Exception {
                    Method methodToInvoke = lob.getClass().getMethod("getBinaryOutputStream");
                    OutputStream out = (OutputStream) methodToInvoke.invoke(lob);
                    IOUtils.write(content, out);
                }
            });
            ps.setBlob(paramIndex, blob);

        } else {
            ps.setBlob(paramIndex, (Blob) null);
        }
    }

    public void setClobAsString(PreparedStatement ps, int paramIndex, final String content)
            throws SQLException {

        if (content != null) {
            Clob clob = (Clob) createLob(ps, true, new LobCallback() {
                public void populateLob(Object lob) throws Exception {
                    Method methodToInvoke = lob.getClass().getMethod("getCharacterOutputStream",
                            (Class[]) null);
                    Writer writer = (Writer) methodToInvoke.invoke(lob, (Object[]) null);
                    IOUtils.write(content, writer);
                }
            });
            ps.setClob(paramIndex, clob);
        } else {
            ps.setClob(paramIndex, (Clob) null);
        }
    }

    /**
     * Create a LOB instance for the given PreparedStatement, populating it via
     * the given callback.
     */
    protected Object createLob(PreparedStatement ps, boolean clob, LobCallback callback)
            throws SQLException {

        Connection con = null;
        try {
            con = getOracleConnection(ps);
            initOracleDriverClasses(con);
            Object lob = prepareLob(con, clob ? clobClass : blobClass);
            callback.populateLob(lob);
            lob.getClass().getMethod("close", (Class[]) null).invoke(lob, (Object[]) null);
            return lob;
        } catch (SQLException ex) {
            throw ex;
        } catch (InvocationTargetException ex) {
            if (ex.getTargetException() instanceof SQLException) {
                throw (SQLException) ex.getTargetException();
            } else if (con != null && ex.getTargetException() instanceof ClassCastException) {
                throw new SqlException(
                        "OracleLobCreator needs to work on [oracle.jdbc.OracleConnection], not on ["
                                + con.getClass().getName()
                                + "]: specify a corresponding NativeJdbcExtractor",
                        ex.getTargetException());
            } else {
                throw new SqlException("Could not create Oracle LOB", ex.getTargetException());
            }
        } catch (Exception ex) {
            throw new SqlException("Could not create Oracle LOB", ex);
        }
    }

    /**
     * Create and open an oracle.sql.BLOB/CLOB instance via reflection.
     */
    protected Object prepareLob(Connection con, Class<?> lobClass) throws Exception {
        /*
         * BLOB blob = BLOB.createTemporary(con, false, BLOB.DURATION_SESSION);
         * blob.open(BLOB.MODE_READWRITE); return blob;
         */
        Method createTemporary = lobClass.getMethod("createTemporary", Connection.class,
                boolean.class, int.class);
        Object lob = createTemporary.invoke(null, con, cache,
                durationSessionConstants.get(lobClass));
        Method open = lobClass.getMethod("open", int.class);
        open.invoke(lob, modeReadWriteConstants.get(lobClass));
        return lob;
    }

    protected synchronized void initOracleDriverClasses(Connection con) {
        if (this.blobClass == null) {
            try {
                // Initialize oracle.sql.BLOB class
                this.blobClass = con.getClass().getClassLoader().loadClass(BLOB_CLASS_NAME);
                this.durationSessionConstants.put(this.blobClass,
                        this.blobClass.getField(DURATION_SESSION_FIELD_NAME).getInt(null));
                this.modeReadWriteConstants.put(this.blobClass,
                        this.blobClass.getField(MODE_READWRITE_FIELD_NAME).getInt(null));
                this.modeReadOnlyConstants.put(this.blobClass,
                        this.blobClass.getField(MODE_READONLY_FIELD_NAME).getInt(null));

                // Initialize oracle.sql.CLOB class
                this.clobClass = con.getClass().getClassLoader().loadClass(CLOB_CLASS_NAME);
                this.durationSessionConstants.put(this.clobClass,
                        this.clobClass.getField(DURATION_SESSION_FIELD_NAME).getInt(null));
                this.modeReadWriteConstants.put(this.clobClass,
                        this.clobClass.getField(MODE_READWRITE_FIELD_NAME).getInt(null));
                this.modeReadOnlyConstants.put(this.clobClass,
                        this.clobClass.getField(MODE_READONLY_FIELD_NAME).getInt(null));
            } catch (Exception ex) {
                throw new SqlException(
                        "Couldn't initialize OracleLobHandler because Oracle driver classes are not available. "
                                + "Note that OracleLobHandler requires Oracle JDBC driver 9i or higher!",
                        ex);
            }
        }
    }

    /**
     * Retrieve the underlying OracleConnection, using a NativeJdbcExtractor if
     * set.
     */
    protected Connection getOracleConnection(PreparedStatement ps) throws SQLException,
            ClassNotFoundException {
        return (nativeConnectionExtractor != null) ? nativeConnectionExtractor.extract(ps
                .getConnection()) : ps.getConnection();
    }

    protected interface LobCallback {
        public void populateLob(Object lob) throws Exception;
    }
}
