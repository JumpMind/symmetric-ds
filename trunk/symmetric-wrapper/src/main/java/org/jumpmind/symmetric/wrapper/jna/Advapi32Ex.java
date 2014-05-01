package org.jumpmind.symmetric.wrapper.jna;

import java.util.Arrays;
import java.util.List;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.platform.win32.Advapi32;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.platform.win32.Winsvc.SC_HANDLE;
import com.sun.jna.platform.win32.Winsvc.SERVICE_STATUS;
import com.sun.jna.win32.W32APIOptions;

public interface Advapi32Ex extends Advapi32 {
    Advapi32Ex INSTANCE = (Advapi32Ex) Native.loadLibrary("Advapi32", Advapi32Ex.class,
            W32APIOptions.UNICODE_OPTIONS);

    SC_HANDLE CreateService(SC_HANDLE manager, String serviceName, String displayName, int access,
            int serviceType, int startType, int errorControl, String commandLine,
            String loadOrderGroup, String tagId, String dependencies, String user, String password);

    boolean ChangeServiceConfig2(SC_HANDLE service, int infoLevel, SERVICE_INFO info);

    boolean DeleteService(SC_HANDLE serviceHandle);

    boolean StartServiceCtrlDispatcher(Structure[] serviceTable);

    boolean SetServiceStatus(Pointer serviceStatusHandle, SERVICE_STATUS serviceStatus);

    SERVICE_STATUS_HANDLE RegisterServiceCtrlHandlerEx(String serviceName,
            HANDLER_FUNCTION handler, Object context);

    public static class SERVICE_INFO extends Structure {
        @Override
        protected List<String> getFieldOrder() {
            return null;
        }
    }

    public static class SERVICE_DESCRIPTION extends SERVICE_INFO {
        public String description;

        public SERVICE_DESCRIPTION(String description) {
            this.description = description;
        }

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(new String[] { "description" });
        }
    }

    public static class SERVICE_STATUS_HANDLE extends HANDLE {
        public SERVICE_STATUS_HANDLE() {
        }

        public SERVICE_STATUS_HANDLE(Pointer p) {
            super(p);
        }
    }

    public interface HANDLER_FUNCTION extends StdCallCallback {
        void serviceControlHandler(int controlCode);
    }

}
