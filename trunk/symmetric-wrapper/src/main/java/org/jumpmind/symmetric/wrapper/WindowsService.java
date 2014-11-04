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

import java.lang.reflect.Field;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.jumpmind.symmetric.wrapper.Constants.Status;
import org.jumpmind.symmetric.wrapper.jna.Advapi32Ex;
import org.jumpmind.symmetric.wrapper.jna.Advapi32Ex.HANDLER_FUNCTION;
import org.jumpmind.symmetric.wrapper.jna.Advapi32Ex.SERVICE_STATUS_HANDLE;
import org.jumpmind.symmetric.wrapper.jna.Kernel32Ex;
import org.jumpmind.symmetric.wrapper.jna.Shell32Ex;
import org.jumpmind.symmetric.wrapper.jna.WinsvcEx;
import org.jumpmind.symmetric.wrapper.jna.WinsvcEx.SERVICE_MAIN_FUNCTION;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.WString;
import com.sun.jna.platform.win32.Advapi32;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.Kernel32Util;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.platform.win32.Winsvc;
import com.sun.jna.platform.win32.Winsvc.SC_HANDLE;
import com.sun.jna.platform.win32.Winsvc.SC_STATUS_TYPE;
import com.sun.jna.platform.win32.Winsvc.SERVICE_STATUS_PROCESS;
import com.sun.jna.ptr.IntByReference;

@IgnoreJRERequirement
public class WindowsService extends WrapperService {

    private final static Logger logger = Logger.getLogger(WindowsService.class.getName());

    protected ServiceControlHandler serviceControlHandler;
    
    protected SERVICE_STATUS_HANDLE serviceStatusHandle;

    protected Winsvc.SERVICE_STATUS serviceStatus;

    @Override
    protected boolean setWorkingDirectory(String dir) {
        return Kernel32Ex.INSTANCE.SetCurrentDirectory(dir);
    }

    @Override
    public void init() {
        logger.log(Level.INFO, "Requesting service dispatch");
        WinsvcEx.SERVICE_TABLE_ENTRY entry = new WinsvcEx.SERVICE_TABLE_ENTRY(config.getName(), new ServiceMain());
        WinsvcEx.SERVICE_TABLE_ENTRY[] serviceTable = (WinsvcEx.SERVICE_TABLE_ENTRY[]) entry.toArray(2);
        if (!Advapi32Ex.INSTANCE.StartServiceCtrlDispatcher(serviceTable)) {
            logger.log(Level.SEVERE, "Error " + Native.getLastError());
            System.exit(Native.getLastError());
        }
    }

    @Override
    public void start() {
        if (!isInstalled()) {
            super.start();
        } else if (isRunning()) {
            throw new WrapperException(Constants.RC_SERVER_ALREADY_RUNNING, 0, "Server is already running");
        } else {
            Advapi32Ex advapi = Advapi32Ex.INSTANCE;
            SC_HANDLE manager = openServiceManager();
            SC_HANDLE service = advapi.OpenService(manager, config.getName(), Winsvc.SERVICE_ALL_ACCESS);
            try {
                if (service != null) {
                    System.out.println("Waiting for server to start");
                    if (!advapi.StartService(service, 0, null)) {
                        throwException("StartService");
                    }
                    Winsvc.SERVICE_STATUS_PROCESS status = waitForService(manager, service);
                    if (status.dwCurrentState == Winsvc.SERVICE_STOPPED) {
                        throw new WrapperException(Constants.RC_SERVER_EXITED, status.dwWin32ExitCode, "Unexpected exit from service");
                    }
                    System.out.println("Started");                
                } else {
                    throwException("OpenService");
                }
            } finally {
                closeServiceHandle(service);
                closeServiceHandle(manager);
            }
        }
    }

    @Override
    public void stop() {
        if (!isInstalled()) {
            super.stop();
        } else if (!isRunning()) {
            throw new WrapperException(Constants.RC_SERVER_NOT_RUNNING, 0, "Server is not running");
        } else {
            Advapi32Ex advapi = Advapi32Ex.INSTANCE;
            SC_HANDLE manager = openServiceManager();
            SC_HANDLE service = advapi.OpenService(manager, config.getName(), Winsvc.SERVICE_ALL_ACCESS);
            try {
                if (service != null) {
                    System.out.println("Waiting for server to stop");
                    if (!advapi.ControlService(service, Winsvc.SERVICE_CONTROL_STOP, new Winsvc.SERVICE_STATUS())) {
                        throwException("ControlService");
                    }
                    Winsvc.SERVICE_STATUS_PROCESS status = waitForService(manager, service);
                    if (status.dwCurrentState != Winsvc.SERVICE_STOPPED) {
                        throw new WrapperException(Constants.RC_FAIL_STOP_SERVER, status.dwWin32ExitCode, "Service did not stop");
                    }
                    System.out.println("Stopped");
                } else {
                    throwException("OpenService");
                }
            } finally {
                closeServiceHandle(service);
                closeServiceHandle(manager);                
            }
        }
    }

    @Override
    public boolean isRunning() {
        Advapi32 advapi = Advapi32.INSTANCE;
        SC_HANDLE manager = advapi.OpenSCManager(null, null, Winsvc.SC_MANAGER_ENUMERATE_SERVICE);
        if (manager == null) {
            throwException("OpenSCManager");
        } else {
            SC_HANDLE service = advapi.OpenService(manager, config.getName(), Winsvc.SERVICE_QUERY_STATUS);
            if (service != null) {                
                IntByReference bytesNeeded = new IntByReference();
                advapi.QueryServiceStatusEx(service, SC_STATUS_TYPE.SC_STATUS_PROCESS_INFO, null, 0, bytesNeeded);
                SERVICE_STATUS_PROCESS status = new SERVICE_STATUS_PROCESS(bytesNeeded.getValue());
                if (!advapi.QueryServiceStatusEx(service, SC_STATUS_TYPE.SC_STATUS_PROCESS_INFO, status, status.size(), bytesNeeded)) {
                    throwException("QueryServiceStatusEx");
                }
                closeServiceHandle(service);
                closeServiceHandle(manager);
                return (status.dwCurrentState == Winsvc.SERVICE_RUNNING);
            }
            closeServiceHandle(manager);
        }
        return super.isRunning();
    }

    @Override
    protected boolean isPidRunning(int pid) {
        boolean isRunning = false;
        if (pid != 0) {
            Kernel32 kernel = Kernel32.INSTANCE;
            HANDLE process = kernel.OpenProcess(Kernel32.SYNCHRONIZE, false, pid);
            if (process != null) {
                int rc = kernel.WaitForSingleObject(process, 0);
                kernel.CloseHandle(process);
                isRunning = (rc == Kernel32.WAIT_TIMEOUT);
            }
        }
        return isRunning;
    }

    @Override
    protected int getCurrentPid() {
        return Kernel32.INSTANCE.GetCurrentProcessId();
    }

    @Override
    public boolean isPrivileged() {
        try {
            closeServiceHandle(openServiceManager());
            return true;
        } catch (WrapperException e) {
            return false;
        }
    }

    @Override
    public void relaunchAsPrivileged(String cmd, String args) {
        Shell32Ex.SHELLEXECUTEINFO execInfo = new Shell32Ex.SHELLEXECUTEINFO();
        execInfo.lpFile = new WString(cmd);
        if (args != null) {
            execInfo.lpParameters = new WString(args);
        }
        execInfo.nShow = Shell32Ex.SW_SHOWDEFAULT;
        execInfo.fMask = Shell32Ex.SEE_MASK_NOCLOSEPROCESS;
        execInfo.lpVerb = new WString("runas");
        if (!Shell32Ex.INSTANCE.ShellExecuteEx(execInfo)) {
            throwException("ShellExecuteEx");
        }
        System.exit(0);
    }

    @Override
    public boolean isInstalled() {
        Advapi32 advapi = Advapi32.INSTANCE;
        boolean isInstalled = false;

        SC_HANDLE manager = advapi.OpenSCManager(null, null, Winsvc.SC_MANAGER_ENUMERATE_SERVICE);
        if (manager == null) {
            throwException("OpenSCManager");
        } else {
            SC_HANDLE service = advapi.OpenService(manager, config.getName(), Winsvc.SERVICE_QUERY_STATUS);
            isInstalled = (service != null);
            closeServiceHandle(service);
            closeServiceHandle(manager);
        }
        return isInstalled;
    }

    @Override
    protected int getProcessPid(Process process) {
        int pid = 0;
        try {
            Field field = process.getClass().getDeclaredField("handle");
            field.setAccessible(true);
            HANDLE processHandle = new HANDLE(Pointer.createConstant(field.getLong(process)));
            pid = Kernel32.INSTANCE.GetProcessId(processHandle);
        } catch (Exception e) {
        }
        return pid;
    }

    @Override
    protected void killProcess(int pid, boolean isTerminate) {
        Kernel32 kernel = Kernel32.INSTANCE;
        HANDLE processHandle = kernel.OpenProcess(WinNT.PROCESS_TERMINATE, true, pid);
        if (processHandle == null) {
            throwException("OpenProcess");
        }
        kernel.TerminateProcess(processHandle, 99);
    }

    @Override
    public void install() {
        if (isRunning()) {
            System.out.println("Server must be stopped before installing");
            System.exit(Constants.RC_NO_INSTALL_WHEN_RUNNING);
        }

        Advapi32Ex advapi = Advapi32Ex.INSTANCE;
        SC_HANDLE manager = openServiceManager();
        SC_HANDLE service = advapi.OpenService(manager, config.getName(), Winsvc.SERVICE_ALL_ACCESS);
        try {
            if (service != null) {
                throw new WrapperException(Constants.RC_ALREADY_INSTALLED, 0, "Service " + config.getName() + " is already installed");
            } else {
                System.out.println("Installing " + config.getName() + " ...");
                
                String dependencies = null;
                if (config.getDependencies() != null && config.getDependencies().size() > 0) {
                    StringBuffer sb = new StringBuffer();
                    for (String dependency : config.getDependencies()) {
                        sb.append(dependency).append("\0");
                    }
                    dependencies = sb.append("\0").toString();                    
                }

                service = advapi.CreateService(manager, config.getName(), config.getDisplayName(), Winsvc.SERVICE_ALL_ACCESS,
                        WinsvcEx.SERVICE_WIN32_OWN_PROCESS, config.isAutoStart() || config.isDelayStart() ? WinsvcEx.SERVICE_AUTO_START
                                : WinsvcEx.SERVICE_DEMAND_START, WinsvcEx.SERVICE_ERROR_NORMAL,
                        commandToString(getWrapperCommand("init")), null, null, dependencies, null, null);
    
                if (service != null) {
                    Advapi32Ex.SERVICE_DESCRIPTION desc = new Advapi32Ex.SERVICE_DESCRIPTION(config.getDescription());
                    advapi.ChangeServiceConfig2(service, WinsvcEx.SERVICE_CONFIG_DESCRIPTION, desc);

                    if (config.isDelayStart()) {
                        WinsvcEx.SERVICE_DELAYED_AUTO_START_INFO delayedInfo = new WinsvcEx.SERVICE_DELAYED_AUTO_START_INFO(true);
                        advapi.ChangeServiceConfig2(service, WinsvcEx.SERVICE_CONFIG_DELAYED_AUTO_START_INFO, delayedInfo);
                    }
                } else {
                    throwException("CreateService");
                }
    
                System.out.println("Done");
            }
        } finally {
            closeServiceHandle(service);
            closeServiceHandle(manager);                
        }
    }

    @Override
    public void uninstall() {
        if (isRunning()) {
            throw new WrapperException(Constants.RC_NO_INSTALL_WHEN_RUNNING, 0, "Server must be stopped before uninstalling");
        }

        Advapi32Ex advapi = Advapi32Ex.INSTANCE;
        SC_HANDLE manager = openServiceManager();
        SC_HANDLE service = advapi.OpenService(manager, config.getName(), Winsvc.SERVICE_ALL_ACCESS);
        try {
            if (service != null) {
                System.out.println("Uninstalling " + config.getName() + " ...");
                if (!advapi.DeleteService(service)) {
                    throwException("DeleteService");
                }
            } else {
                throw new WrapperException(Constants.RC_NOT_INSTALLED, 0, "Service " + config.getName() + " is not installed");
            }
        } finally {
            closeServiceHandle(service);
            closeServiceHandle(manager);                
        }
        
        int seconds = 0;
        while (seconds <= 30) {
            if (!isInstalled()) {
                break;
            }
            System.out.print(".");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            seconds++;
        }
        if (seconds > 0) {
            System.out.println("");
        }
        if (isInstalled()) {
            System.out.println("Service manager did not complete");
        } else {
            System.out.println("Done");
        }
    }

    protected SC_HANDLE openServiceManager() {
        Advapi32 advapi = Advapi32.INSTANCE;
        SC_HANDLE handle = advapi.OpenSCManager(null, null, Winsvc.SC_MANAGER_ALL_ACCESS);
        if (handle == null) {
            throwException("OpenSCManager");
        }
        return handle;
    }

    @Override
    protected void updateStatus(Status status) {
        switch (status) {
            case START_PENDING:
                updateStatus(Winsvc.SERVICE_START_PENDING, 0);
                break;
            case RUNNING:
                updateStatus(Winsvc.SERVICE_RUNNING, Winsvc.SERVICE_ACCEPT_STOP);
                break;
            case STOP_PENDING:
                updateStatus(Winsvc.SERVICE_STOP_PENDING, 0);
                break;
            case STOPPED:
                updateStatus(Winsvc.SERVICE_STOPPED, 0);
                break;
        }
    }

    protected void updateStatus(int status, int controlsAccepted) {
        if (serviceStatus != null) {
            Advapi32Ex advapi = Advapi32Ex.INSTANCE;
            serviceStatus.dwCurrentState = status;
            serviceStatus.dwControlsAccepted = controlsAccepted;
            if (!advapi.SetServiceStatus(serviceStatusHandle.getPointer(), serviceStatus)) {
                throwException("SetServiceStatus");
            }
        }
    }

    protected Winsvc.SERVICE_STATUS_PROCESS waitForService(SC_HANDLE manager, SC_HANDLE service) {
        int seconds = 0;
        Advapi32Ex advapi = Advapi32Ex.INSTANCE;
        IntByReference bytesNeeded = new IntByReference();        
        advapi.QueryServiceStatusEx(service, SC_STATUS_TYPE.SC_STATUS_PROCESS_INFO, null, 0, bytesNeeded);
        Winsvc.SERVICE_STATUS_PROCESS status = new Winsvc.SERVICE_STATUS_PROCESS(bytesNeeded.getValue());

        while (seconds <= 5) {
            System.out.print(".");
            if (!advapi.QueryServiceStatusEx(service, SC_STATUS_TYPE.SC_STATUS_PROCESS_INFO,
                    status, status.size(), bytesNeeded)) {
                throwException("QueryServiceStatusEx");
            }
            if (status.dwCurrentState == Winsvc.SERVICE_STOPPED) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            seconds++;
        }
        System.out.println("");
        return status;
    }

    protected void closeServiceHandle(SC_HANDLE handle) {
        if (handle != null) {
            Advapi32Ex advapi = Advapi32Ex.INSTANCE;
            advapi.CloseServiceHandle(handle);
        }
    }
    
    protected void throwException(String name) {
        int rc = Native.getLastError();
        throw new WrapperException(Constants.RC_NATIVE_ERROR, rc, name + " returned error " + rc + ": "
                + Kernel32Util.formatMessageFromLastErrorCode(rc));
    }

    protected String getWrapperCommandQuote() {
        return "\"";
    }

    class ServiceMain implements SERVICE_MAIN_FUNCTION {
        @Override
        public void serviceMain(int argc, Pointer argv) {
            logger.log(Level.INFO, "Getting service status");
            serviceControlHandler = new ServiceControlHandler();
            serviceStatusHandle = Advapi32Ex.INSTANCE.RegisterServiceCtrlHandlerEx(config.getName(), serviceControlHandler,
                    null);
            if (serviceStatusHandle == null) {
                System.exit(Constants.RC_FAIL_REGISTER_SERVICE);
            }

            serviceStatus = new Winsvc.SERVICE_STATUS();
            serviceStatus.dwServiceType = WinsvcEx.SERVICE_WIN32_OWN_PROCESS;

            if (!isRunning()) {
                execJava(false);
            } else {
                updateStatus(Winsvc.SERVICE_STOPPED, 0);
            }
        }
    }

    class ServiceControlHandler implements HANDLER_FUNCTION {
        public void serviceControlHandler(int controlCode) {
            if (controlCode != Winsvc.SERVICE_CONTROL_INTERROGATE) {
                logger.log(Level.INFO, "Service manager requesting control code " + controlCode);
            }
            if (controlCode == Winsvc.SERVICE_CONTROL_STOP) {
                logger.log(Level.INFO, "Service manager requesting to stop service");
                shutdown();
            }
        }
    }

}
