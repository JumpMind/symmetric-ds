package org.jumpmind.symmetric.wrapper;

public class Constants {

    public enum Status {
        START_PENDING, RUNNING, STOP_PENDING, STOPPED;
    }

    public static final int RC_BAD_USAGE = 1;
    public static final int RC_INVALID_ARGUMENT = 2;
    public static final int RC_MISSING_CONFIG_FILE = 3;
    public static final int RC_FAIL_READ_CONFIG_FILE = 4;
    public static final int RC_SERVER_ALREADY_RUNNING = 5;
    public static final int RC_SERVER_NOT_RUNNING = 6;
    public static final int RC_FAIL_WRITE_LOG_FILE = 7;
    public static final int RC_FAIL_EXECUTION = 8;
    public static final int RC_FAIL_STOP_SERVER = 9;
    public static final int RC_NO_INSTALL_WHEN_RUNNING = 10;
    public static final int RC_NOT_INSTALLED = 11;
    public static final int RC_ALREADY_INSTALLED = 12;
    public static final int RC_FAIL_REGISTER_SERVICE = 13;
    public static final int RC_MUST_BE_ROOT = 14;
    public static final int RC_MISSING_INIT_FOLDER = 15;
    public static final int RC_SERVER_EXITED = 16;
    public static final int RC_FAIL_INSTALL = 17;
    public static final int RC_FAIL_UNINSTALL = 18;
    public static final int RC_NATIVE_ERROR = 19;
    public static final int RC_MISSING_LIB_FOLDER = 20;
    
}
