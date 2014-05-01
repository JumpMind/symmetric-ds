package org.jumpmind.symmetric.wrapper.jna;

import java.util.Arrays;
import java.util.List;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.platform.win32.Winsvc;

@IgnoreJRERequirement
public interface WinsvcEx extends Winsvc {

    int SERVICE_WIN32_OWN_PROCESS = 0x00000010;
    int SERVICE_AUTO_START = 0x00000002;
    int SERVICE_DEMAND_START = 0x00000003;
    int SERVICE_ERROR_NORMAL = 0x00000001;
    int SERVICE_CONFIG_DESCRIPTION = 1;

    public interface SERVICE_MAIN_FUNCTION extends StdCallCallback {
        void serviceMain(int argc, Pointer argv);
    }

    public static class SERVICE_TABLE_ENTRY extends Structure {
        public String serviceName;
        public SERVICE_MAIN_FUNCTION serviceCallback;

        public SERVICE_TABLE_ENTRY() {
        }
        
        public SERVICE_TABLE_ENTRY(String serviceName, SERVICE_MAIN_FUNCTION serviceCallback) {
            this.serviceName = serviceName;
            this.serviceCallback = serviceCallback;
        }
        
        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(new String[] { "serviceName", "serviceCallback" });
        }
    }
}
