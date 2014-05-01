package org.jumpmind.symmetric.wrapper;

import java.lang.reflect.Field;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.jumpmind.symmetric.wrapper.Constants.Status;
import org.jumpmind.symmetric.wrapper.jna.Advapi32Ex;
import org.jumpmind.symmetric.wrapper.jna.Advapi32Ex.HANDLER_FUNCTION;
import org.jumpmind.symmetric.wrapper.jna.Advapi32Ex.SERVICE_STATUS_HANDLE;
import org.jumpmind.symmetric.wrapper.jna.Kernel32Ex;
import org.jumpmind.symmetric.wrapper.jna.WinsvcEx;
import org.jumpmind.symmetric.wrapper.jna.WinsvcEx.SERVICE_MAIN_FUNCTION;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Advapi32;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.Kernel32Util;
import com.sun.jna.platform.win32.Tlhelp32;
import com.sun.jna.platform.win32.WinDef;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.platform.win32.Winsvc;
import com.sun.jna.platform.win32.Winsvc.SC_HANDLE;

// TODO: return RC instead of exiting
@IgnoreJRERequirement
public class WindowsService extends WrapperService {

    protected SERVICE_STATUS_HANDLE serviceStatusHandle;

    protected Winsvc.SERVICE_STATUS serviceStatus;

    @Override
    protected boolean setWorkingDirectory(String dir) {
        return Kernel32Ex.INSTANCE.SetCurrentDirectory(dir);
    }

    @Override
    public void init() {
        log("Requesting service dispatch");
        WinsvcEx.SERVICE_TABLE_ENTRY entry = new WinsvcEx.SERVICE_TABLE_ENTRY(config.getName(), new ServiceMain());
        WinsvcEx.SERVICE_TABLE_ENTRY[] serviceTable = (WinsvcEx.SERVICE_TABLE_ENTRY[]) entry.toArray(2);
        if (!Advapi32Ex.INSTANCE.StartServiceCtrlDispatcher(serviceTable)) {
            log("Error " + Native.getLastError());
            System.exit(Native.getLastError());
        }
    }

    @Override
    public void start() {
        if (!isInstalled()) {
            super.start();
        } else if (isRunning()) {
            System.out.println("Server is already running");
            System.exit(Constants.RC_SERVER_ALREADY_RUNNING);
        } else {
            Advapi32Ex advapi = Advapi32Ex.INSTANCE;
            SC_HANDLE manager = openServiceManager();
            SC_HANDLE service = advapi.OpenService(manager, config.getName(), Winsvc.SERVICE_ALL_ACCESS);

            if (service != null) {
                System.out.println("Waiting for server to start");
                if (!advapi.StartService(service, 0, null)) {
                    displayError("StartService");
                }
                advapi.CloseServiceHandle(service);
                System.out.println("Started");
            } else {
                displayError("OpenService");
            }
            advapi.CloseServiceHandle(manager);
        }
    }

    @Override
    public void stop() {
        if (!isInstalled()) {
            super.stop();
        } else if (!isRunning()) {
            System.out.println("Server is not running");
            System.exit(Constants.RC_SERVER_NOT_RUNNING);
        } else {
            Advapi32Ex advapi = Advapi32Ex.INSTANCE;
            SC_HANDLE manager = openServiceManager();
            SC_HANDLE service = advapi.OpenService(manager, config.getName(), Winsvc.SERVICE_ALL_ACCESS);

            if (service != null) {
                System.out.println("Waiting for server to stop");
                if (!advapi.ControlService(service, Winsvc.SERVICE_CONTROL_STOP, new Winsvc.SERVICE_STATUS())) {
                    displayError("ControlService");
                }
                advapi.CloseServiceHandle(service);
                System.out.println("Stopped");
            } else {
                displayError("OpenService");
            }
            advapi.CloseServiceHandle(manager);
        }
    }

    @Override
    protected boolean isPidRunning(int pid) {
        boolean isRunning = false;
        if (pid != 0) {
            Kernel32 kernel = Kernel32.INSTANCE;
            WinDef.DWORD processId = new WinDef.DWORD(pid);
            Tlhelp32.PROCESSENTRY32.ByReference processEntry = new Tlhelp32.PROCESSENTRY32.ByReference();
            HANDLE snapshot = kernel.CreateToolhelp32Snapshot(Tlhelp32.TH32CS_SNAPPROCESS, new WinDef.DWORD(0));
            try {
                while (kernel.Process32Next(snapshot, processEntry)) {
                    if (processEntry.th32ProcessID.equals(processId)) {
                        isRunning = true;
                        break;
                    }
                }
            } finally {
                kernel.CloseHandle(snapshot);
            }
        }
        return isRunning;
    }

    @Override
    protected int getCurrentPid() {
        return Kernel32.INSTANCE.GetCurrentProcessId();
    }

    @Override
    public boolean isInstalled() {
        Advapi32 advapi = Advapi32.INSTANCE;
        boolean isInstalled = false;

        SC_HANDLE serviceManager = openServiceManager();
        if (serviceManager != null) {
            SC_HANDLE service = advapi.OpenService(serviceManager, config.getName(), Winsvc.SERVICE_ALL_ACCESS);
            isInstalled = (service != null);
            advapi.CloseServiceHandle(serviceManager);
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
            displayError("OpenProcess");
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

        if (service != null) {
            System.out.println("Service " + config.getName() + " is already installed");
            System.exit(Constants.RC_ALREADY_INSTALLED);
        } else {
            System.out.println("Installing " + config.getName() + " ...");

            service = advapi.CreateService(manager, config.getName(), config.getDisplayName(), Winsvc.SERVICE_ALL_ACCESS,
                    WinsvcEx.SERVICE_WIN32_OWN_PROCESS, config.isAutoStart() ? WinsvcEx.SERVICE_AUTO_START
                            : WinsvcEx.SERVICE_DEMAND_START, WinsvcEx.SERVICE_ERROR_NORMAL,
                    commandToString(getWrapperCommand("init")), null, null, null, null, null);

            if (service != null) {
                Advapi32Ex.SERVICE_DESCRIPTION desc = new Advapi32Ex.SERVICE_DESCRIPTION(config.getDescription());
                advapi.ChangeServiceConfig2(service, WinsvcEx.SERVICE_CONFIG_DESCRIPTION, desc);
                advapi.CloseServiceHandle(service);
            } else {
                displayError("CreateService");
            }

            advapi.CloseServiceHandle(manager);
            System.out.println("Done");
        }
    }

    @Override
    public void uninstall() {
        if (isRunning()) {
            System.out.println("Server must be stopped before uninstalling");
            System.exit(Constants.RC_NO_INSTALL_WHEN_RUNNING);
        }

        Advapi32Ex advapi = Advapi32Ex.INSTANCE;
        SC_HANDLE manager = openServiceManager();
        SC_HANDLE service = advapi.OpenService(manager, config.getName(), Winsvc.SERVICE_ALL_ACCESS);
        if (service != null) {
            System.out.println("Uninstalling " + config.getName() + " ...");
            if (!advapi.DeleteService(service)) {
                displayError("DeleteService");
            }
            advapi.CloseServiceHandle(service);
            System.out.println("Done");
        } else {
            System.out.println("Service " + config.getName() + " is not installed");
            System.exit(Constants.RC_NOT_INSTALLED);
        }
        advapi.CloseServiceHandle(manager);
    }

    protected SC_HANDLE openServiceManager() {
        Advapi32 advapi = Advapi32.INSTANCE;
        SC_HANDLE handle = advapi.OpenSCManager(null, null, Winsvc.SC_MANAGER_ALL_ACCESS);
        if (handle == null) {
            displayError("OpenSCManager");
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
                displayError("SetServiceStatus");
            }
        }
    }

    protected void displayError(String name) {
        int rc = Native.getLastError();
        System.out.println(name + " failed with error code: " + rc);
        System.out.println(Kernel32Util.formatMessageFromLastErrorCode(rc));
        System.exit(rc);
    }

    protected String getWrapperCommandQuote() {
        return "\"";
    }

    class ServiceMain implements SERVICE_MAIN_FUNCTION {
        @Override
        public void serviceMain(int argc, Pointer argv) {
            log("Getting service status");
            serviceStatusHandle = Advapi32Ex.INSTANCE.RegisterServiceCtrlHandlerEx(config.getName(), new ServiceControlHandler(),
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
            log("Service manager requesting control code " + controlCode);
            if (controlCode == Winsvc.SERVICE_CONTROL_STOP) {
                shutdown();
            }
        }
    }

}
