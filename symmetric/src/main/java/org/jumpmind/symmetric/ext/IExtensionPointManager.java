package org.jumpmind.symmetric.ext;

import org.springframework.beans.BeansException;

public interface IExtensionPointManager {

    public void register() throws BeansException;
    
}
