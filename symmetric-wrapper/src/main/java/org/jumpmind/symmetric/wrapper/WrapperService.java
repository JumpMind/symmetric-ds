package org.jumpmind.symmetric.wrapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.jumpmind.symmetric.wrapper.Constants.Status;

import com.sun.jna.Platform;

public abstract class WrapperService {

    protected WrapperConfig config;

    protected BufferedWriter logWriter;

    protected boolean keepRunning = true;

    protected Process child;

    protected BufferedReader childReader;

    private static WrapperService instance;

    public static WrapperService getInstance() throws IOException {
        if (Platform.isWindows()) {
            instance = new WindowsService();
        } else {
            instance = new UnixService();
        }
        return instance;
    }

    public void loadConfig(String configFile) throws IOException {
        config = new WrapperConfig(configFile);
        setWorkingDirectory(config.getWorkingDirectory().getAbsolutePath());
    }

    public void start() {
        if (isRunning()) {
            System.out.println("Server is already running");
            System.exit(Constants.RC_SERVER_ALREADY_RUNNING);
        }

        System.out.print("Waiting for server to start");
        boolean success = false;
        int rc = 0;
        try {
            ProcessBuilder pb = new ProcessBuilder(getWrapperCommand("exec"));
            Process process = pb.start();
            if (!(success = waitForPid(getProcessPid(process)))) {
                rc = process.exitValue();
            }
        } catch (IOException e) {
            rc = -1;
            System.out.println("");
            System.out.println(e.getMessage());
        }

        if (success) {
            System.out.println("Started");
        } else {
            System.out.println("Error occurred, rc=" + rc);
        }
    }

    public void init() {
        execJava(false);
    }

    public void console() {
        if (isRunning()) {
            System.out.println("Server is already running");
            System.exit(Constants.RC_SERVER_ALREADY_RUNNING);
        }
        execJava(true);
    }

    protected void execJava(boolean isConsole) {
        try {
            logWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(config.getLogFile())));
        } catch (IOException e) {
            System.out.println("Cannot open log file " + config.getLogFile());
            System.out.println(e.getMessage());
            System.exit(Constants.RC_FAIL_WRITE_LOG_FILE);
        }

        int pid = getCurrentPid();
        writePidToFile(pid, config.getWrapperPidFile());
        log("Started wrapper [" + pid + "]");

        ArrayList<String> cmd = config.getCommand(isConsole);
        String cmdString = commandToString(cmd);
        log("Working directory is " + System.getProperty("user.dir"));

        long startTime = 0;
        int startCount = 0;
        boolean startProcess = true;
        int serverPid = 0;

        while (keepRunning) {
            if (startProcess) {
                log("Executing " + cmdString);
                if (startCount == 0) {
                    updateStatus(Status.START_PENDING);
                }
                startTime = System.currentTimeMillis();
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.redirectErrorStream(true);

                try {
                    child = pb.start();
                } catch (IOException e) {
                    log("Failed to execute: " + e.getMessage());
                    updateStatus(Status.STOPPED);
                    System.exit(Constants.RC_FAIL_EXECUTION);
                }

                serverPid = getProcessPid(child);
                log("Started server [" + serverPid + "]");
                writePidToFile(serverPid, config.getSymPidFile());

                if (startCount == 0) {
                    Runtime.getRuntime().addShutdownHook(new ShutdownHook());
                    updateStatus(Status.RUNNING);
                }
                startProcess = false;
                startCount++;
            } else {
                try {
                    childReader = new BufferedReader(new InputStreamReader(child.getInputStream()));
                    String line = null;

                    while ((line = childReader.readLine()) != null) {
                        if (isConsole) {
                            System.out.println(line);
                        } else {
                            log(line);
                        }
                        if (line.matches(".*java.lang.OutOfMemoryError.*") || line.matches(".*java.net.BindException.*")) {
                            log("Stopping server because its output matches a failure condition");
                            child.destroy();
                            childReader.close();
                            stopProcess(serverPid, "symmetricds");
                            break;
                        }
                    }
                } catch (IOException e) {
                    log("Error while reading from process");
                }
                
                if (keepRunning) {
                    log("Unexpected exit from server: " + child.exitValue());
                    long runTime = System.currentTimeMillis() - startTime;
                    if (System.currentTimeMillis() - startTime < 5000) {
                        log("Stopping because server exited too quickly after only " + runTime + " milliseconds");
                        updateStatus(Status.STOPPED);
                        System.exit(child.exitValue());
                    } else {
                        startProcess = true;
                    }
                }
            }
        }
    }

    public void stop() {
        int symPid = readPidFromFile(config.getSymPidFile());
        int wrapperPid = readPidFromFile(config.getWrapperPidFile());
        if (!isPidRunning(symPid) && !isPidRunning(wrapperPid)) {
            System.out.println("Server is not running");
            System.exit(Constants.RC_SERVER_NOT_RUNNING);
        }
        System.out.print("Waiting for server to stop");
        stopProcess(wrapperPid, "wrapper");
        stopProcess(symPid, "symmetricds");
        System.out.println("Stopped");
    }
    
    protected boolean stopProcess(int pid, String name) {
        killProcess(pid, false);
        if (waitForPid(pid)) {
            killProcess(pid, true);
            if (waitForPid(pid)) {
                System.out.println("ERROR: '" + name + "' did not stop");
                return false;
            }
        }
        return true;
    }

    protected void shutdown() {
        if (keepRunning) {
            keepRunning = false;
            log("Stopping server");
            child.destroy();
            try {
                childReader.close();
            } catch (IOException e) {
            }
            log("Stopping wrapper");
            deletePidFile(config.getWrapperPidFile());
            deletePidFile(config.getSymPidFile());
            updateStatus(Status.STOPPED);
        }
    }

    public void restart() {
        if (isRunning()) {
            stop();
        }
        start();
    }
    
    public void status() {
        System.out.println("Installed: " + isInstalled());
        System.out.println("Running: " + isRunning());
    }

    public boolean isRunning() {
        return isPidRunning(readPidFromFile(config.getSymPidFile()));
    }

    protected String commandToString(ArrayList<String> cmd) {
        StringBuilder sb = new StringBuilder();
        for (String c : cmd) {
            sb.append(c).append(" ");
        }
        return sb.toString();
    }

    protected ArrayList<String> getWrapperCommand(String arg) {
        ArrayList<String> cmd = new ArrayList<String>();
        String quote = getWrapperCommandQuote();
        cmd.add(quote + config.getJavaCommand() + quote);
        cmd.add("-cp");
        cmd.add(quote + System.getProperty("java.class.path") + quote);
        cmd.add(Wrapper.class.getCanonicalName());
        cmd.add(arg);
        cmd.add(quote + config.getConfigFile() + quote);
        return cmd;
    }
    
    protected String getWrapperCommandQuote() {
        return "";
    }

    protected int readPidFromFile(String filename) {
        int pid = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            pid = Integer.parseInt(reader.readLine());
            reader.close();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
        return pid;
    }

    protected void writePidToFile(int pid, String filename) {
        try {
            FileWriter writer = new FileWriter(filename, false);
            writer.write(String.valueOf(pid));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void deletePidFile(String filename) {
        new File(filename).delete();
    }

    protected boolean waitForPid(int pid) {
        int seconds = 0;
        while (seconds <= 5) {
            System.out.print(".");
            if (!isPidRunning(pid)) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            seconds++;
        }
        System.out.println("");
        return isPidRunning(pid);
    }

    protected void log(String message) {
        // TODO: rotate the log file based on size or time
        // TODO: use java util logging with log level and add debug logging
        try {
            logWriter.write(message + System.getProperty("line.separator"));
            logWriter.flush();
        } catch (Exception e) {
        }
    }

    protected void updateStatus(Status status) {
    }

    class ShutdownHook extends Thread {
        public void run() {
            shutdown();
        }
    }

    public abstract void install();

    public abstract void uninstall();

    public abstract boolean isInstalled();

    protected abstract boolean setWorkingDirectory(String dir);

    protected abstract int getProcessPid(Process process);

    protected abstract int getCurrentPid();

    protected abstract boolean isPidRunning(int pid);

    protected abstract void killProcess(int pid, boolean isTerminate);

}
