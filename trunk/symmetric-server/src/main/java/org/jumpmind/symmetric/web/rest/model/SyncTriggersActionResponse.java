package org.jumpmind.symmetric.web.rest.model;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="actionresponse")
public class SyncTriggersActionResponse extends ActionResponse {

    List<String> createdTrigger = new ArrayList<String>();
    
    public SyncTriggersActionResponse() {
     createdTrigger.add("SYM_ON_I_TEST_THIS_OUT");
     createdTrigger.add("SYM_ON_U_TEST_THIS_OUT");
     createdTrigger.add("SYM_ON_D_TEST_THIS_OUT");
    }
    
    public String[] getCreatedTrigger() {
        return createdTrigger.toArray(new String[createdTrigger.size()]);
    }
    
    public void setCreatedTrigger(String[] createdTriggers) {
        this.createdTrigger = new ArrayList<String>();
        for (String string : createdTriggers) {
            this.createdTrigger.add(string);
        }
    }
    
}
