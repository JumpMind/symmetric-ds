package org.jumpmind.symmetric.wrapper.jna;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import com.sun.jna.Library;
import com.sun.jna.Native;

@IgnoreJRERequirement
public interface CLibrary extends Library {
    CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);

    int chdir(String path);

    int kill(int pid, int signal);

    int symlink(String filename, String linkname);

    int getpid();

    int geteuid();
}
