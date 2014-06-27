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
     * Returns the identity for the specified engine on the node.
     * @param String engine
     * @return String the identity of the engine
     */
    @RequestMapping(value = "/identity/engines/{engine}", method = RequestMethod.GET)
    @ResponseBody
    public final String identity(@PathVariable("engine") String engineName) {
        ISymmetricEngine engine = getSymmetricEngine(engineName);
        return engine.getNodeService().findIdentityNodeId();
    }

    /**
     * Returns the list of engine names that are configured on the node.   
     * @return Set<String> of engine names configured on the node
     */
    @RequestMapping(value = "/engines", method = RequestMethod.GET)
    @ResponseBody
    public final Set<String> engines() {
        return getSymmetricEngineHolder().getEngines().keySet();
    }

    /**
     * Loads a profile for the specified engine on the node. 
     * @param engineName 
     */
    @RequestMapping(value = "/loadprofile/engines/{engine}", method = RequestMethod.PUT)
    @ResponseBody
    //TODO: figure out how we will pass the file info...
    public final void loadProfile(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }

    /**
     * Drops SymmetricDS triggers for the specified engine and table on the node.
     * @param engineName
     * @param tableName
     */
    @RequestMapping(value = "/droptrigger/engines/{engine}/tables/{table}", method = RequestMethod.POST)
    @ResponseBody
    public final void dropTrigger(@PathVariable("engine") String engineName,
                    @PathVariable("table") String tableName) {
            //TODO: Implementation
    }

    /**
     * Drops all SymmetricDS triggers for the specified engine on the node.
     * @param engineName
     */
    @RequestMapping(value = "/droptrigger/engines/{engine}", method = RequestMethod.POST)
    @ResponseBody
    public final void dropTrigger(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }
    
    /**
     * Creates SymmetricDS triggers for the specified engine and table on the node.
     * @param engineName
     * @param tableName
     */
    @RequestMapping(value = "/synctrigger/engines/{engine}/tables/{table}", method = RequestMethod.POST)
    @ResponseBody
    public final void syncTrigger(@PathVariable("engine") String engineName,
                    @PathVariable("table") String tableName) {
            //TODO: Implementation
    }

    /**
     * Creates SymmetricDS triggers for all configured tables on the specified engine on the node.
     * @param engineName
     */
    @RequestMapping(value = "/synctrigger/engines/{engine}", method = RequestMethod.POST)
    @ResponseBody
    public final void syncTrigger(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }    
    
    /**
     * Reinitializes the specified engine on the node.  This includes:
     * <ul>
     * <li>Uninstalling all SymmetricDS objects from the database</li>
     * <li>Reregistering the node</li>
     * <li>Initial load (if configured)</li>
     * <li>Reverse initial load (if configured)</li>
     * </ul>
     * @param engineName
     */
    @RequestMapping(value = "/reinitialize/engines/{engine}", method = RequestMethod.POST)
    @ResponseBody
    public final void initialize(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }
    
    /**
     * Returns an overall status for the specified engine of the node.
     * @param engineName
     * @return {@link NodeStatus}
     */
    @RequestMapping(value = "/nodestatus/engines/{engine}", method = RequestMethod.GET)
    @ResponseBody
    public final NodeStatus nodeStatus(@PathVariable("engine") String engineName) {
            //TODO: Implementation
            return null;
    }
        
    /**
     * Returns status of each channel for the specified engine of the node.
     * @param engineName
     * @return Set<{@link ChannelStatus}>
     */    
    @RequestMapping(value = "/channelstatus/engines/{engine}", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> channelStatus(@PathVariable("engine") String engineName) {
            //TODO: Implementation
            return null;
    }

    /**
     * Uninstalls all SymmetricDS objects from the database for the specified engine of the node.
     * @param engineName
     */
    @RequestMapping(value = "/uninstall/engines/{engine}", method = RequestMethod.POST)
    @ResponseBody
    public final void unintstall(@PathVariable("engine") String engineName) {
            //TODO: Implementation
    }
        
    /**
     * Starts SymmetricDS for the node.
     */
    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @ResponseBody
    public final void start() {
            //TODO: Implementation
    }
        
    /**
     * Stops SymmetricDS for the node.
     */
    @RequestMapping(value = "/stop", method = RequestMethod.POST)
    @ResponseBody
    public final void stop() {
            //TODO: Implementation
    }

    /**
     * Refreshes the cache for the node.
     */
    @RequestMapping(value = "/refreshcache", method = RequestMethod.POST)
    @ResponseBody
    public final void refreshCache() {
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
