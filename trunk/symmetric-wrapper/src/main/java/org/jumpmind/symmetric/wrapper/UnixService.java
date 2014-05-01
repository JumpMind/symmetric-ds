package org.jumpmind.symmetric.wrapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.jumpmind.symmetric.wrapper.jna.CLibrary;

@IgnoreJRERequirement
public class UnixService extends WrapperService {

    private static final String PROC_DIR = "/proc/";

    private static final String[] RUN_LEVELS_START = new String[] { "2", "3", "5" };

    private static final String[] RUN_LEVELS_STOP = new String[] { "0", "1", "6" };

    private static final String RUN_SEQUENCE_START = "20";

    private static final String RUN_SEQUENCE_STOP = "80";

    private static final String RC_DIR = "/etc";

    private static final String INITD_DIR = "/etc/init.d";

    @Override
    protected boolean setWorkingDirectory(String dir) {
        return CLibrary.INSTANCE.chdir(dir) == 0;
    }

    @Override
    public void install() {
        String rcDir = getRunCommandDir();
        String runFile = INITD_DIR + "/" + config.getName();

        if (CLibrary.INSTANCE.geteuid() != 0) {
            System.out.println("Must be root to install");
            System.exit(Constants.RC_MUST_BE_ROOT);
        }
        System.out.println("Installing " + config.getName() + " ...");

        try {
            FileWriter writer = new FileWriter(runFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(
                "/symmetricds.initd")));
            String line = null;
            while ((line = reader.readLine()) != null) {
                line = line.replaceAll("\\$\\{wrapper.home}", config.getWorkingDirectory().getAbsolutePath());
                line = line.replaceAll("\\$\\{wrapper.name}", config.getName());
                line = line.replaceAll("\\$\\{wrapper.displayname}", config.getDisplayName());
                line = line.replaceAll("\\$\\{wrapper.description}", config.getDescription());
                line = line.replaceAll("\\$\\{wrapper.java.command}", config.getJavaCommand());                
                line = line.replaceAll("\\$\\{wrapper.classpath}", System.getProperty("java.class.path"));
                line = line.replaceAll("\\$\\{wrapper.mainclass}", Wrapper.class.getCanonicalName());
                line = line.replaceAll("\\$\\{wrapper.propfile}", config.getConfigFile());
                writer.write(line + "\n");
            }
            reader.close();
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        new File(runFile).setExecutable(true, false);

        for (String runLevel : RUN_LEVELS_START) {
            CLibrary.INSTANCE.symlink(runFile, rcDir + "/rc" + runLevel + ".d/S" + RUN_SEQUENCE_START
                + config.getName());
        }
        for (String runLevel : RUN_LEVELS_STOP) {
            CLibrary.INSTANCE.symlink(runFile, rcDir + "/rc" + runLevel + ".d/K" + RUN_SEQUENCE_STOP
                + config.getName());
        }
        System.out.println("Done");
    }

    @Override
    public void uninstall() {
        String rcDir = getRunCommandDir();
        String runFile = INITD_DIR + "/" + config.getName();
        
        if (CLibrary.INSTANCE.geteuid() != 0) {
            System.out.println("Must be root to uninstall");
            System.exit(Constants.RC_MUST_BE_ROOT);
        }

        System.out.println("Uninstalling " + config.getName() + " ...");

        for (String runLevel : RUN_LEVELS_START) {
            new File(rcDir + "/rc" + runLevel + ".d/S" + RUN_SEQUENCE_START + config.getName()).delete();
        }
        for (String runLevel : RUN_LEVELS_STOP) {
            new File(rcDir + "/rc" + runLevel + ".d/K" + RUN_SEQUENCE_STOP + config.getName()).delete();
        }
        new File(runFile).delete();
        System.out.println("Done");
    }

    protected String getRunCommandDir() {
        String rcDir = "";
        if (new File(INITD_DIR + "/rc0.d").exists()) {
            rcDir = INITD_DIR;
        } else if (new File(RC_DIR + "/rc0.d").exists()) {
            rcDir = RC_DIR;
        } else {
            System.out.println("Unable to locate run level folders");
            System.exit(Constants.RC_MISSING_INIT_FOLDER);
        }
        return rcDir;
    }

    @Override
    public boolean isInstalled() {
        return new File(INITD_DIR + "/" + config.getName()).exists();
    }

    @Override
    protected boolean isPidRunning(int pid) {
        boolean isRunning = false;
        if (pid != 0) {
            File procFile = new File(PROC_DIR + pid);
            isRunning = procFile.exists();
        }
        return isRunning;
    }

    @Override
    protected int getCurrentPid() {
        return CLibrary.INSTANCE.getpid(); 
    }

    @Override
    protected int getProcessPid(Process process) {
        int pid = 0;
        try {
            Field field = process.getClass().getDeclaredField("pid");
            field.setAccessible(true);
            pid = field.getInt(process);
        } catch (Exception e) {
        }
        return pid;
    }

    @Override
    protected void killProcess(int pid, boolean isTerminate) {
        int signal = isTerminate ? 9 : 1;
        CLibrary.INSTANCE.kill(pid, signal);
    }
}
