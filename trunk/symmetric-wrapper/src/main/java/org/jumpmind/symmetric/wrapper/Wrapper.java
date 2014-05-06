package org.jumpmind.symmetric.wrapper;

import java.io.FileNotFoundException;
import java.io.IOException;

public class Wrapper {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            printUsage();
            System.exit(Constants.RC_BAD_USAGE);
        }

        WrapperService service = WrapperService.getInstance();
        try {
            service.loadConfig(args[1]);
        } catch (FileNotFoundException e) {
            System.out.println("Cannot find config file " + args[1]);
            System.out.println(e.getMessage());
            System.exit(Constants.RC_MISSING_CONFIG_FILE);
        } catch (IOException e) {
            System.out.println("Cannot read config file " + args[1]);
            System.out.println(e.getMessage());
            System.exit(Constants.RC_FAIL_READ_CONFIG_FILE);
        }

        try {
            if (args[0].equalsIgnoreCase("start")) {
                service.start();
            } else if (args[0].equalsIgnoreCase("exec")) {
                service.execJava(false);
            } else if (args[0].equalsIgnoreCase("init")) {
                service.init();
            } else if (args[0].equalsIgnoreCase("stop")) {
                service.stop();
            } else if (args[0].equalsIgnoreCase("restart")) {    
                service.restart();
            } else if (args[0].equalsIgnoreCase("install")) {
                service.install();
            } else if (args[0].equalsIgnoreCase("uninstall")) {
                service.uninstall();
            } else if (args[0].equalsIgnoreCase("status")) {
                service.status();
            } else if (args[0].equalsIgnoreCase("console")) {
                service.console();
            } else {
                System.out.println("ERROR: Invalid argument");
                printUsage();
                System.exit(Constants.RC_INVALID_ARGUMENT);
            }
        } catch (WrapperException e) {
            System.out.println("Error " + e.getErrorCode() + ": " + e.getMessage());
            if (e.getCause() != null) {
                System.out.println("Exception " + e.getCause().getClass().getSimpleName() + ": "
                        + e.getCause().getMessage());
            }
            if (e.getNativeErrorCode() > 0) {
                System.out.println("Native error " + e.getErrorCode());    
            }            
        }
    }

    private static void printUsage() {
        System.out.println("Usage: [start|stop|restart|install|remove|console] <config-file>");
        System.out.println("   start      - Start service");
        System.out.println("   stop       - Stop service");
        System.out.println("   restart    - Restart service");
        System.out.println("   install    - Install service");
        System.out.println("   uninstall  - Uninstall service");
        System.out.println("   status     - Status of service");
        System.out.println("   console    - Run from console");
    }
}
