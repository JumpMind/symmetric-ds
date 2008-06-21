package org.jumpmind.symmetric.ext;

public interface IExtensionPoint {

    /**
     * Allow the plug-in implementation to specific whether the SymmetricDS
     * runtime should auto register it or if it will register itself.
     */
    public boolean isAutoRegister();

}
