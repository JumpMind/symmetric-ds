package org.jumpmind.symmetric.ext;

import org.jumpmind.symmetric.ISymmetricEngine;

/**
 * If an extension point needs access to SymmetricDS services it should
 * implement this interface and access the apis via the {@link ISymmetricEngine}
 * interface.
 */
public interface ISymmetricEngineAware {

    public void setSymmetricEngine(ISymmetricEngine engine);

}
