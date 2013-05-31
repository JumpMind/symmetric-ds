package org.jumpmind.db.util;

/**
 * Constants that represent parameters that can be retrieved or saved via the
 * {@link IParameterService}
 */
final public class BasicDataSourcePropertyConstants {

    public static final String ALL = "ALL";

    private BasicDataSourcePropertyConstants() {
    }

    public final static String DB_POOL_URL = "db.url";
    public final static String DB_POOL_DRIVER = "db.driver";
    public final static String DB_POOL_USER = "db.user";
    public final static String DB_POOL_PASSWORD = "db.password";
    public final static String DB_POOL_INITIAL_SIZE = "db.pool.initial.size";
    public final static String DB_POOL_MAX_ACTIVE = "db.pool.max.active";
    public final static String DB_POOL_MAX_WAIT = "db.pool.max.wait.millis";
    public final static String DB_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS = "db.pool.min.evictable.idle.millis";
    public final static String DB_POOL_VALIDATION_QUERY = "db.validation.query";
    public final static String DB_POOL_TEST_ON_BORROW = "db.test.on.borrow";
    public final static String DB_POOL_TEST_ON_RETURN = "db.test.on.return";
    public final static String DB_POOL_TEST_WHILE_IDLE = "db.test.while.idle";
    public final static String DB_POOL_INIT_SQL = "db.init.sql";
    public final static String DB_POOL_CONNECTION_PROPERTIES = "db.connection.properties";

}