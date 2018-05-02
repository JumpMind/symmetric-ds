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
package org.jumpmind.symmetric.wrapper;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.jumpmind.symmetric.wrapper.jna.CLibrary;

@IgnoreJRERequirement
public class UnixService extends WrapperService {

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

        if (!isPrivileged()) {
            throw new WrapperException(Constants.RC_MUST_BE_ROOT, 0, "Must be root to install");
        }
        System.out.println("Installing " + config.getName() + " ...");

        try {
            FileWriter writer = new FileWriter(runFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(
                "/symmetricds.initd")));
            String line = null;
            while ((line = reader.readLine()) != null) {
                line = line.replaceAll("\\$\\{wrapper.name}", config.getName());
                line = line.replaceAll("\\$\\{wrapper.displayname}", config.getDisplayName());
                line = line.replaceAll("\\$\\{wrapper.description}", config.getDescription());
                line = line.replaceAll("\\$\\{wrapper.home}", config.getWorkingDirectory().getAbsolutePath());
                line = line.replaceAll("\\$\\{wrapper.java.command}", config.getJavaCommand());                
                line = line.replaceAll("\\$\\{wrapper.jarfile}", config.getWrapperJarPath());
                writer.write(line + "\n");
            }
            reader.close();
            writer.close();
        } catch (IOException e) {
            throw new WrapperException(Constants.RC_FAIL_INSTALL, 0, "Failed while writing run file", e);
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
        
        if (!isPrivileged()) {
            throw new WrapperException(Constants.RC_MUST_BE_ROOT, 0, "Must be root to uninstall");
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
            throw new WrapperException(Constants.RC_MISSING_INIT_FOLDER, 0, "Unable to locate run level folders");
        }
        return rcDir;
    }

    @Override
    public boolean isPrivileged() {
        return CLibrary.INSTANCE.geteuid() == 0;
    }

    @Override
    public boolean isInstalled() {
        return new File(INITD_DIR + "/" + config.getName()).exists();
    }

    @Override
    protected boolean isPidRunning(int pid) {
        File procFile = new File("/proc/" + pid + "/cmdline");
        if (procFile.canRead()) {
            try {
                List<String> args = readProcFile(procFile);
                for (String arg : args) {
                    if (arg.contains(config.getJavaCommand())) {
                        return true;
                    }
                }
                return false;
            } catch (IOException e) {
            }
        }        
        return pid != 0 && CLibrary.INSTANCE.kill(pid, 0) == 0;
    }

    private List<String> readProcFile(File procFile) throws IOException {
        FileInputStream in = new FileInputStream(procFile);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        List<String> args = new ArrayList<String>();
        byte buffer[] = new byte[512];
        int len = 0;

        while ((len = in.read(buffer)) != -1) {
            for (int i = 0; i < len; i++) {
                if (buffer[i] == (byte) 0x0) {
                    bout.flush();
                    args.add(new String(bout.toString()));
                    bout.reset();
                } else {
                    bout.write(buffer[i]);
                }
            }
        }
        in.close();
        return args;
    }

    @Override
    protected int getCurrentPid() {
        return CLibrary.INSTANCE.getpid(); 
    }

    @Override
    protected int getProcessPid(Process process) {
        int pid = 0;
        try {
            // Java 9
            Method method = Process.class.getDeclaredMethod("pid", (Class[]) null);
            Object object = method.invoke(process);
            pid = ((Long) object).intValue();
        } catch (Exception e) {
            try {
                // Prior to Java 9
                Field field = process.getClass().getDeclaredField("pid");
                field.setAccessible(true);
                pid = field.getInt(process);
            } catch (Exception ex) {
            }
        }
        return pid;
    }

    @Override
    protected void killProcess(int pid, boolean isTerminate) {
        CLibrary.INSTANCE.kill(pid, isTerminate ? 9 : 1);
    }
}
