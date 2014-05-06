package org.jumpmind.symmetric.wrapper.jna;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import com.sun.jna.Native;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.win32.W32APIOptions;

@IgnoreJRERequirement
public interface Kernel32Ex extends Kernel32 {

    Kernel32Ex INSTANCE = (Kernel32Ex) Native.loadLibrary("kernel32", Kernel32Ex.class,
            W32APIOptions.UNICODE_OPTIONS);

    boolean SetCurrentDirectory(String directory);

}
