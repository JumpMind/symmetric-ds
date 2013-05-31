package org.jumpmind.symmetric.integrate;

import org.jumpmind.util.Context;

public interface IPublisher {
    
    public void publish(Context context, String text);
    
}