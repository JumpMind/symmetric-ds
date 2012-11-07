/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.symmetric.web.rest;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.web.SymmetricEngineHolder;
import org.jumpmind.symmetric.web.WebConstants;
import org.jumpmind.symmetric.web.rest.model.ChannelStatus;
import org.jumpmind.symmetric.web.rest.model.Engine;
import org.jumpmind.symmetric.web.rest.model.EngineList;
import org.jumpmind.symmetric.web.rest.model.Node;
import org.jumpmind.symmetric.web.rest.model.NodeList;
import org.jumpmind.symmetric.web.rest.model.NodeStatus;
import org.jumpmind.symmetric.web.rest.model.RestError;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MultipartFile;

/**
 * REST API into the SymmetricDS Engine.
 * 
 * <p>
 * <b>General HTTP Responses to the methods:</b>
 * <ul>
 * <li>
 * ALL Methods may return the following HTTP responses.<br>
 * <br>
 * In general:<br>
 * <ul>
 * <li>HTTP 2xx = Success</li>
 * <li>HTTP 4xx = Problem on the caller (client) side</li>
 * <li>HTTP 5xx - Problem on the REST service side</li>
 * </ul>
 * ALL Methods
 * <ul>
 * <li>HTTP 401 - Unauthorized.  You have not successfully authenticated.  
 * Authentication details are in the response body.</li>
 * <li>HTTP 404 - Not Found.  You attempted to perform an operation
 * on a resource that doesn't exist.  I.E. you tried to start or stop an 
 * engine that doesn't exist. </li>
 * <li>HTTP 405 - Method Not Allowed.  I.E. you attempted a service
 * call that uses the default engine (/engine/identity vs engine/{engine}/identity)
 * and there was more than one engine found on the server.</li>  
 * <li>HTTP 500 - Internal Server Error.  Something went wrong on the
 * server / service, and we couldn't fulfill the request.  Details are in the response
 * body.</li>
 * </ul>
 * </li>
 * <li>
 * GET Methods
 * <ul>
 * <li>HTTP 200 - Success with result contained in the response body.</li>
 * <li>HTTP 204 - Success with no results.  Your GET request completed
 * successfully, but found no matching entities.</li>
 * </ul>
 * </ul>
 * </p>
 */
@Controller
public class RestService {

    @Autowired
    ServletContext context;
    /**
     * Provides a list of {@link Engine} that are configured on the node.
     * 
     * @return {@link EngineList} - Engines configured on the node
     */
    @RequestMapping(value = "/enginelist", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final EngineList engine() {
        EngineList list = new EngineList();
        Collection<String> engineNames = getSymmetricEngineHolder().getEngines().keySet();
        for (String engine : engineNames) {
            list.addEngine(new Engine(engine));
        }
        return list;
    }    
    
    /**
     * Provides a list of children {@link Node} that are registered with this engine.
     */
    @RequestMapping(value = "engine/{engine}/children", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final NodeList children(
    		@PathVariable("engine") String engineName) {
        return childrenImpl(getSymmetricEngine(engineName));
    }
    
    /**
     * Provides a list of children {@link Node} that are registered with this engine.
     */
    @RequestMapping(value = "engine/children", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )    
    @ResponseBody
    public final NodeList children() {
        return childrenImpl(getSymmetricEngine());
    }
    
    /**
     * Loads a profile for the specified engine on the node.
     * 
     * @param engine The engine name for which the action is intended.     
     * @param file A file stream that contains the profile itself.
     * TODO:  put more details here on the specifics of how the file needs to be passed
     */
    @RequestMapping(value = "engine/{engine}/profile", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void loadProfile(
    		@PathVariable("engine") String engineName,
            @RequestParam (value = "file") MultipartFile file) {
        
    	loadProfileImpl(getSymmetricEngine(engineName), file);
    }

    /**
     * Loads a profile for the single engine on the node.
     *      
     * @param file A file stream that contains the profile itself.
     * TODO:  put more details here on the specifics of how the file needs to be passed
     */
    @RequestMapping(value = "engine/profile", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void loadProfile(@RequestParam MultipartFile file) {
    	
    	loadProfileImpl(getSymmetricEngine(), file);
    }
    
    /**
     * Starts the single engine on the node
     */
    @RequestMapping(value = "engine/start", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void start() {
    	startImpl(getSymmetricEngine());
    }
    
    /**
     * Starts the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/start", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void start(@PathVariable("engine") String engineName) {
    	startImpl(getSymmetricEngine(engineName));
    }
    
    /**
     * Stops the single engine on the node
     */
    @RequestMapping(value = "engine/stop", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void stop() {
    	stopImpl(getSymmetricEngine());
    }
    
    /**
     * Stops the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/stop", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void stop(@PathVariable("engine") String engineName) {
    	stopImpl(getSymmetricEngine(engineName));
    }
    
    /**
     * Creates instances of triggers for each entry configured table/trigger for the single engine on the node
     */
    @RequestMapping(value = "engine/synctriggers", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void syncTriggers(@RequestParam(required = false, value = "force") boolean force) {
    	syncTriggersImpl(getSymmetricEngine(), force);
    }
    
    /**
     * Creates instances of triggers for each entry configured table/trigger for the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/synctriggers", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void syncTriggers(
    		@PathVariable("engine") String engineName,
    		@RequestParam(required = false, value = "force") boolean force) {
    	syncTriggersImpl(getSymmetricEngine(engineName), force);
    }
    
    /**
     * Removes instances of triggers for each entry configured table/trigger for the single engine on the node
     */
    @RequestMapping(value = "engine/droptriggers", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void dropTriggers() {
    	dropTriggersImpl(getSymmetricEngine());
    }
    
    /**
     * Removes instances of triggers for each entry configured table/trigger for the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/droptriggers", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void dropTriggers(@PathVariable("engine") String engineName) {
    	dropTriggersImpl(getSymmetricEngine(engineName));
    }
    
    /**
     * Uninstalls all SymmetricDS objects from the given node (database) for the single engine on the node
     */
    @RequestMapping(value = "engine/uninstall", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void uninstall() {
    	uninstallImpl(getSymmetricEngine());
    }
    
    /**
     * Uninstalls all SymmetricDS objects from the given node (database) for the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/uninstall", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void uninstall(@PathVariable("engine") String engineName) {
    	uninstallImpl(getSymmetricEngine(engineName));
    }

    /**
     * Generates an uninstall script for database objects for 
     * the single engine on the node. The script is contained in the response body.
     */
    @RequestMapping(value = "engine/generateuninstallscript", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final String gnerateUninstallScript() {
    	return generateUninstallScriptImpl(getSymmetricEngine());
    }
    
    /**
     * Generates an uninstall script for database objects for 
     * the specified engine on the node. The script is contained in the response body.
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/generateuninstallscript", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final String generateUninstallScript(@PathVariable("engine") String engineName) {
    	return generateUninstallScriptImpl(getSymmetricEngine(engineName));
    }
        
    /**
     * Generates an install script for database objects for 
     * the single engine on the node. The script is contained in the response body.
     */
    @RequestMapping(value = "engine/generateinstallscript", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final String gnerateInstallScript() {
    	return generateInstallScriptImpl(getSymmetricEngine());
    }
    
    /**
     * Generates an install script for database objects for 
     * the specified engine on the node. The script is contained in the response body.
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/generateInstallscript", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final String generateInstallScript(@PathVariable("engine") String engineName) {
    	return generateUninstallScriptImpl(getSymmetricEngine(engineName));
    }

	/**
	 * Reinitializes the given node (database) for the single engine on the node
	 */
	@RequestMapping(value = "engine/reinitialize", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.NO_CONTENT)
	@ResponseBody
	public final void reinitialize() {
		reinitializeImpl(getSymmetricEngine());
	}

	/**
	 * Reinitializes the given node (database) for the specified engine on the
	 * node
	 * 
	 * @param engineName
	 */
	@RequestMapping(value = "engine/{engine}/reinitialize", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.NO_CONTENT)
	@ResponseBody
	public final void reinitialize(@PathVariable("engine") String engineName) {
		reinitializeImpl(getSymmetricEngine(engineName));
	}

	/**
	 * Refreshes cache for the single engine on the node
	 */
	@RequestMapping(value = "engine/refreshcache", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.NO_CONTENT)
	@ResponseBody
	public final void refreshcache() {
		refreshCacheImpl(getSymmetricEngine());
	}

	/**
	 * Refreshes cache for the specified engine on the node node
	 * 
	 * @param engineName
	 */
	@RequestMapping(value = "engine/{engine}/refreshcache", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.NO_CONTENT)
	@ResponseBody
	public final void refreshcache(@PathVariable("engine") String engineName) {
		refreshCacheImpl(getSymmetricEngine(engineName));
	}	

    /**
     * Returns an overall status for the specified engine of the node.
     * 
     * @param engineName
     * @return {@link NodeStatus}
     */
    @RequestMapping(value = "/engines/{engine}/status", method = RequestMethod.GET)
    @ResponseBody
    public final NodeStatus nodeStatus(@PathVariable("engine") String engineName) {
        // TODO: Implementation
        return new NodeStatus();
    }

    /**
     * Returns status of each channel for the specified engine of the node.
     * 
     * @param engineName
     * @return Set<{@link ChannelStatus}>
     */
    @RequestMapping(value = "/engines/{engine}/channelstatus", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> channelStatus(@PathVariable("engine") String engineName) {
        throw new RuntimeException("Test");
    }
    
    //***********************************************************************************************
    //TODO: stuff that should probably get moved out to some type of delegate or implementation class
    //***********************************************************************************************    
    @ExceptionHandler(Exception.class)
    @ResponseBody
    public RestError handleError(Exception ex, HttpServletRequest req) {
        int httpErrorCode = 500;
        Annotation annotation = ex.getClass().getAnnotation(ResponseStatus.class);
        if (annotation != null) {
            httpErrorCode = ((ResponseStatus) annotation).value().value();
        }
        return new RestError(ex, httpErrorCode);
    }

    private void startImpl(ISymmetricEngine engine) {
    	if (!engine.start()) {
    		throw new InternalServerErrorException();
    	}
    }
    
    private void stopImpl(ISymmetricEngine engine) {
    	engine.stop();
    }
    
    private void syncTriggersImpl(ISymmetricEngine engine, boolean force) {

		ITriggerRouterService triggerRouterService = engine
				.getTriggerRouterService();
		StringBuilder buffer = new StringBuilder();
		triggerRouterService.syncTriggers(buffer, force);
		// FIGURE OUT WHAT WE WANT TO DO HERE. RESPONSE CODE IN HTTP RESPONSE
		// AND MSG IN BODY?
		// SyncTriggersActionResponse response = new
		// SyncTriggersActionResponse();
		// response.setSuccess(true);
		// response.setMessage(buffer.toString());
    }

    private void dropTriggersImpl(ISymmetricEngine engine) {
    	//TODO: implement
    }
    
    private void uninstallImpl(ISymmetricEngine engine) {
    	engine.uninstall();
    }
    
    private String generateUninstallScriptImpl(ISymmetricEngine engine) {
    	//TODO: implement
    	return null;
    }
    
    private String generateInstallScriptImpl(ISymmetricEngine engine) {
    	//TODO: implement
    	return null;
    }
    
    private void reinitializeImpl(ISymmetricEngine engine) {
    	//TODO: implement
    }

    private void refreshCacheImpl(ISymmetricEngine engine) {
    	//TODO: implement
    }
    
    private void loadProfileImpl(ISymmetricEngine engine, MultipartFile file) {
    	//TODO:implement    	
    }

    private NodeList childrenImpl(ISymmetricEngine engine) {
    	//TODO:implement
    	return null;
    }    
    
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
        
        ISymmetricEngine engine = null;        
        if (StringUtils.isNotBlank(engineName)) {
            engine = holder.getEngines().get(engineName);
        }
        if (engine == null) {
            throw new NotFoundException();
        } else {
            return engine;
        }
    }
    
    protected ISymmetricEngine getSymmetricEngine() {
        SymmetricEngineHolder holder = getSymmetricEngineHolder();

        if (holder.getEngines().size() == 1) {
            return holder.getEngines().values().iterator().next();
        } else {
        	throw new NotAllowedException();
        }        
    }

}
