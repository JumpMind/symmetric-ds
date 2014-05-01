package org.jumpmind.symmetric.wrapper.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;

public interface CLibrary extends Library {
    CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);

    int chdir(String path);

    int kill(int pid, int signal);

    int symlink(String filename, String linkname);

    int getpid();

    int geteuid();
}
