package org.jumpmind.symmetric.web.rest;

import java.util.Set;

import javax.servlet.ServletContext;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.web.SymmetricEngineHolder;
import org.jumpmind.symmetric.web.WebConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class RestService {

    @Autowired
    ServletContext context;

    //TODO: determine error strategy
    //TODO: add throws
    
    /**
     * Returns the identity of a given node
     * @param String engine
     * @return String the indentity of the node 
     */
    @RequestMapping(value = "/identity/engines/{engine}", method = RequestMethod.GET)
    @ResponseBody
    public final String identity(@PathVariable("engine") String engineName) {
        ISymmetricEngine engine = getSymmetricEngine(engineName);
        return engine.getNodeService().findIdentityNodeId();
    }

    /**
     * Returns the list of engines that are configured in this SymmetricDS instance  
     * @return Set<String> of engine names (from the properties file) of the engines
     */
    @RequestMapping(value = "/engines", method = RequestMethod.GET)
    @ResponseBody
    public final Set<String> engines() {
        return getSymmetricEngineHolder().getEngines().keySet();
    }

    @RequestMapping(value = "/loadprofile/engines/{engine}", method = RequestMethod.PUT)
    @ResponseBody
    //TODO: figure out how we will pass the file info...
    public final void loadProfile(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }

    @RequestMapping(value = "/droptrigger/engines/{engine}/tables/{table}", method = RequestMethod.POST)
    @ResponseBody
    public final void dropTrigger(@PathVariable("engine") String engineName,
                    @PathVariable("table") String tableName) {
            //TODO: Implementation
    }

    @RequestMapping(value = "/droptrigger/engines/{engine}", method = RequestMethod.POST)
    @ResponseBody
    public final void dropTrigger(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }
    
    @RequestMapping(value = "/synctrigger/engines/{engine}/tables/{table}", method = RequestMethod.POST)
    @ResponseBody
    public final void syncTrigger(@PathVariable("engine") String engineName,
                    @PathVariable("table") String tableName) {
            //TODO: Implementation
    }

    @RequestMapping(value = "/synctrigger/engines/{engine}", method = RequestMethod.POST)
    @ResponseBody
    public final void syncTrigger(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }    
    
    @RequestMapping(value = "/reinitialize/engines/{engine}", method = RequestMethod.POST)
    @ResponseBody
    public final void initialize(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }
    
    @RequestMapping(value = "/nodestatus/engines/{engine}", method = RequestMethod.GET)
    @ResponseBody
    public final NodeStatus nodeStatus(@PathVariable("engine") String engineName) {
            //TODO: Implementation
            return null;
    }
        
    @RequestMapping(value = "/channelstatus/engines/{engine}", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> channelStatus(@PathVariable("engine") String engineName) {
            //TODO: Implementation
            return null;
    }

    @RequestMapping(value = "/uninstall/engines/{engine}", method = RequestMethod.POST)
    @ResponseBody
    public final void unintstall(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }
        
    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @ResponseBody
    public final void start() {
            //TODO: Implementation
    }
        
    @RequestMapping(value = "/stop", method = RequestMethod.POST)
    @ResponseBody
    public final void stop() {
            //TODO: Implementation
    }

    @RequestMapping(value = "/resetcache", method = RequestMethod.POST)
    @ResponseBody
    public final void resetCache() {
            //TODO: Implementation
    }

    //TODO: reloadtable
    //TODO: reloadnode
    
    protected SymmetricEngineHolder getSymmetricEngineHolder() {
        SymmetricEngineHolder holder = (SymmetricEngineHolder) context
                .getAttribute(WebConstants.ATTR_ENGINE_HOLDER);
        if (holder == null) {
            throw new NotFoundException();
        }
        return holder;
    }

    protected ISymmetricEngine getSymmetricEngine(String engineName) {
        SymmetricEngineHolder holder = getSymmetricEngineHolder();
        ISymmetricEngine engine = holder.getEngines().get(engineName);
        if (engine == null) {
            throw new NotFoundException();
        } else {
            return engine;
        }
    }

}
