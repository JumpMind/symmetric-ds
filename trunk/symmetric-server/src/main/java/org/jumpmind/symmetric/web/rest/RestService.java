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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.NodeHost;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.web.ServerSymmetricEngine;
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
     * @return {@link EngineList} - Engines configured on the node <br>
     * <pre>
     * Example xml reponse is as follows:<br><br>
     *   {@code
     *   <enginelist>
     *     <engines>
     *       <name>RootSugarDB-root</name>
     *     </engines>
     *   </enginelist>
     *   }
	 * <br>
	 * Example json response is as follows:<br><br>
	 *   {"engines":[{"name":"RootSugarDB-root"}]}
	 * </pre>
     */
    @RequestMapping(value = "/enginelist", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final EngineList getEngineList() {
        EngineList list = new EngineList();
        Collection<ServerSymmetricEngine> engines = getSymmetricEngineHolder().getEngines()
                .values();
        for (ISymmetricEngine engine : engines) {
            if (engine.getParameterService().is(ParameterConstants.REST_API_ENABLED)) {
                list.addEngine(new Engine(engine.getEngineName()));
            }
        }
        return list;
    }    

    /**
     * Provides Node information for the single engine
     * 
     * return {@link Node}<br>
     * 
     * <pre>
     * Example xml reponse is as follows:<br><br>
     *   {@code
     * <node>
	 * 	 <name>root</name>
	 *   <rootNode>true</rootNode>
	 * </node>
     *   }
	 * <br>
	 * Example json response is as follows:<br><br>
	 *   
	 * </pre>
     */
    @RequestMapping(value = "engine/node", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )    
    @ResponseBody
    public final Node getNode() {
        return nodeImpl(getSymmetricEngine());
    }
    
    /** 
     * Provides Node information for the specified engine
     */
    @RequestMapping(value = "engine/{engine}/node", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final Node getNode(
    		@PathVariable("engine") String engineName) {
        return nodeImpl(getSymmetricEngine(engineName));
    }
    
    /**
     * Provides a list of children that are registered with this engine.
     * 
     * return {@link Node}<br>
     * 
     * <pre>
     * Example xml reponse is as follows:<br><br>
     *   {@code
 	 * <nodelist>
	 *	<nodes>
	 *		<name>client01</name>
	 *		<rootNode>false</rootNode>
	 *	</nodes>
	 * </nodelist>
     *   }
	 * <br>
	 * Example json response is as follows:<br><br>
	 *   
	 * </pre>
     */
    @RequestMapping(value = "engine/children", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )    
    @ResponseBody
    public final NodeList getChildren() {
        return childrenImpl(getSymmetricEngine());
    }
        
    /**
     * Provides a list of children {@link Node} that are registered with this engine.
     */
    @RequestMapping(value = "engine/{engine}/children", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final NodeList getChildrenByEngine(
    		@PathVariable("engine") String engineName) {
        return childrenImpl(getSymmetricEngine(engineName));
    }
    
    /**
     * Takes a snapshot and streams it to the client.
     *      
     * @param file A file stream that contains the profile itself.
     */
    @RequestMapping(value = "engine/snapshot", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final void snapshot(HttpServletResponse resp) {
        snapshot(getSymmetricEngine().getEngineName(), resp);   
    }
    
    /**
     * Takes a snapshot for the specified engine and streams it to the client.
     *      
     * @param file A file stream that contains the profile itself.
     */
    @RequestMapping(value = "engine/{engine}/snapshot", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final void snapshot(@PathVariable("engine") String engineName, HttpServletResponse resp) {
        BufferedInputStream bis = null;
        try {
            ISymmetricEngine engine = getSymmetricEngine(engineName);            
            File file = engine.snapshot();
            resp.setHeader("Content-Disposition", String.format("attachment; filename=%s", file.getName()));
            bis = new BufferedInputStream(new FileInputStream(file));            
            IOUtils.copy(bis, resp.getOutputStream());
        } catch (IOException e) {
            throw new IoException(e);
        } finally {
            IOUtils.closeQuietly(bis);
        }  
    }    
    
    
    /**
     * Loads a configuration profile for the single engine on the node.
     *      
     * @param file A file stream that contains the profile itself.
     */
    @RequestMapping(value = "engine/profile", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postProfile(@RequestParam MultipartFile file) {
    	loadProfileImpl(getSymmetricEngine(), file);
    }
    
    /**
     * Loads a configuration profile for the specified engine on the node.
     * 
     * @param engine The engine name for which the action is intended.     
     * @param file A file stream that contains the profile itself.
     */
    @RequestMapping(value = "engine/{engine}/profile", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postProfileByEngine(
    		@PathVariable("engine") String engineName,
            @RequestParam (value = "file") MultipartFile file) {
        
    	loadProfileImpl(getSymmetricEngine(engineName), file);
    }

    /**
     * Starts the single engine on the node
     */
    @RequestMapping(value = "engine/start", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postStart() {
    	startImpl(getSymmetricEngine());
    }
    
    /**
     * Starts the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/start", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postStartByEngine(@PathVariable("engine") String engineName) {
    	startImpl(getSymmetricEngine(engineName));
    }
    
    /**
     * Stops the single engine on the node
     */
    @RequestMapping(value = "engine/stop", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postStop() {
    	stopImpl(getSymmetricEngine());
    }
    
    /**
     * Stops the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/stop", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postStopByEngine(@PathVariable("engine") String engineName) {
    	stopImpl(getSymmetricEngine(engineName));
    }
    
    /**
     * Creates instances of triggers for each entry configured table/trigger for the single engine on the node
     */
    @RequestMapping(value = "engine/synctriggers", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postSyncTriggers(@RequestParam(required = false, value = "force") boolean force) {
    	syncTriggersImpl(getSymmetricEngine(), force);
    }
    
    /**
     * Creates instances of triggers for each entry configured table/trigger for the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/synctriggers", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postSyncTriggersByEngine (
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
    public final void postDropTriggers() {
    	dropTriggersImpl(getSymmetricEngine());
    }
    
    /**
     * Removes instances of triggers for each entry configured table/trigger for the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/droptriggers", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postDropTriggersByEngine(@PathVariable("engine") String engineName) {
    	dropTriggersImpl(getSymmetricEngine(engineName));
    }

    /**
     * Removes instances of triggers for the specified table for the single engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/table/{table}/droptriggers", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postDropTriggersByTable(
    		@PathVariable("table") String tableName) {
    	dropTriggersImpl(getSymmetricEngine(), tableName);
    }
    
    /**
     * Removes instances of triggers for the specified table for the single engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/table/{table}/droptriggers", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postDropTriggersByEngineByTable(
    		@PathVariable("engine") String engineName,
    		@PathVariable("table") String tableName) {
    	dropTriggersImpl(getSymmetricEngine(engineName), tableName);
    }
    
    /**
     * Uninstalls all SymmetricDS objects from the given node (database) for the single engine on the node
     */
    @RequestMapping(value = "engine/uninstall", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postUninstall() {
    	uninstallImpl(getSymmetricEngine());
    }
    
    /**
     * Uninstalls all SymmetricDS objects from the given node (database) for the specified engine on the node
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/uninstall", method = RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    @ResponseBody
    public final void postUninstallByEngine(@PathVariable("engine") String engineName) {
    	uninstallImpl(getSymmetricEngine(engineName));
    }

    /**
     * Generates an uninstall script for database objects for 
     * the single engine on the node. The script is contained in the response body.
     */
    @RequestMapping(value = "engine/uninstallscript", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final String getUninstallScript() {
    	return generateUninstallScriptImpl(getSymmetricEngine());
    }
    
    /**
     * Generates an uninstall script for database objects for 
     * the specified engine on the node. The script is contained in the response body.
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/uninstallscript", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final String getUninstallScriptByEngine(@PathVariable("engine") String engineName) {
    	return generateUninstallScriptImpl(getSymmetricEngine(engineName));
    }
        
    /**
     * Generates an install script for database objects for 
     * the single engine on the node. The script is contained in the response body.
     */
    @RequestMapping(value = "engine/installscript", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final String getInstallScript() {
    	return generateInstallScriptImpl(getSymmetricEngine());
    }
    
    /**
     * Generates an install script for database objects for 
     * the specified engine on the node. The script is contained in the response body.
     * @param engineName
     */
    @RequestMapping(value = "engine/{engine}/installscript", method = RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    @ResponseBody
    public final String getInstallScriptByEngine(@PathVariable("engine") String engineName) {
    	return generateUninstallScriptImpl(getSymmetricEngine(engineName));
    }

	/**
	 * Reinitializes the given node (database) for the single engine on the node
	 */
	@RequestMapping(value = "engine/reinitialize", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.NO_CONTENT)
	@ResponseBody
	public final void postReinitialize() {
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
	public final void postReinitializeByEngine(@PathVariable("engine") String engineName) {
		reinitializeImpl(getSymmetricEngine(engineName));
	}

	/**
	 * Refreshes cache for the single engine on the node
	 */
	@RequestMapping(value = "engine/refreshcache", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.NO_CONTENT)
	@ResponseBody
	public final void postRefreshcache() {
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
	public final void postRefreshcacheByEngine(@PathVariable("engine") String engineName) {
		refreshCacheImpl(getSymmetricEngine(engineName));
	}	

    /**
	 * Returns an overall status for the single engine of the node.
	 * 
	 * @return {@link NodeStatus}
	 * 
	 *         <pre>
	 * Example xml reponse is as follows:<br><br>
	 *   {@code
	 * <nodestatus>
	 * <batchInErrorCount>0</batchInErrorCount>
	 * <batchToSendCount>0</batchToSendCount>
	 * <databaseType>Microsoft SQL Server</databaseType>
	 * <databaseVersion>9.0</databaseVersion>
	 * <deploymentType>professional</deploymentType>
	 * 	<externalId>root</externalId>
	 * 	<initialLoaded>true</initialLoaded>
	 * 	<lastHeartbeat>2012-11-17 14:52:19.267</lastHeartbeat>
	 * <nodeGroupId>RootSugarDB</nodeGroupId>
	 * <nodeId>root</nodeId>
	 * <registered>true</registered>
	 * <registrationServer>false</registrationServer>
	 * <started>true</started>
	 * <symmetricVersion>3.1.10</symmetricVersion>
	 * <syncEnabled>true</syncEnabled>
	 * <syncUrl>http://my-machine-name:31415/sync/RootSugarDB-root</syncUrl>
	 * </nodestatus>        
	 *   }
	 * <br>
	 * Example json response is as follows:<br><br>
	 * {"started":true,"registered":true,"registrationServer":false,"initialLoaded":true,
	 * "nodeId":"root","nodeGroupId":"RootSugarDB","externalId":"root",
	 * "syncUrl":"http://my-machine-name:31415/sync/RootSugarDB-root","databaseType":"Microsoft SQL Server",
	 * "databaseVersion":"9.0","syncEnabled":true,"createdAtNodeId":null,"batchToSendCount":0,
	 * "batchInErrorCount":0,"deploymentType":"professional","symmetricVersion":"3.1.10",
	 * "lastHeartbeat":"2012-11-17 15:15:00.033","hearbeatInterval":null}
	 * </pre>
	 */
    @RequestMapping(value = "/engine/status", method = RequestMethod.GET)
    @ResponseBody
    public final NodeStatus getStatus() {
        return nodeStatusImpl(getSymmetricEngine());
    }

    /**
     * Returns an overall status for the specified engine of the node.
     * 
     * @param engineName
     * @return {@link NodeStatus}
     */
    @RequestMapping(value = "/engine/{engine}/status", method = RequestMethod.GET)
    @ResponseBody
    public final NodeStatus getStatusByEngine(@PathVariable("engine") String engineName) {
        return nodeStatusImpl(getSymmetricEngine(engineName));
    }

    /**
     * Returns status of each channel for the single engine of the node.
     * 
     * @param engineName
     * @return Set<{@link ChannelStatus}>
     */
    @RequestMapping(value = "/engine/channelstatus", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> getChannelStatus(@PathVariable("engine") String engineName) {
        return channelStatusImpl(getSymmetricEngine());
    }
    
    /**
     * Returns status of each channel for the specified engine of the node.
     * 
     * @param engineName
     * @return Set<{@link ChannelStatus}>
     */
    @RequestMapping(value = "/engine/{engine}/channelstatus", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> getChannelStatusByEngine(@PathVariable("engine") String engineName) {
        return channelStatusImpl(getSymmetricEngine(engineName));
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
    }

    private void dropTriggersImpl(ISymmetricEngine engine) {
		ITriggerRouterService triggerRouterService = engine
				.getTriggerRouterService();
		triggerRouterService.dropTriggers();
    }
    
    private void dropTriggersImpl(ISymmetricEngine engine, String tableName) {
		ITriggerRouterService triggerRouterService = engine
				.getTriggerRouterService();
		HashSet<String> tables = new HashSet<String>();
		tables.add(tableName);
		triggerRouterService.dropTriggers(tables);    	
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
    	INodeService nodeService = engine.getNodeService();
    	org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity();
    	    	
    	if (!this.isRootNode(engine, modelNode)) {
    		engine.uninstall();
    	}
    }

    private void refreshCacheImpl(ISymmetricEngine engine) {
    	//TODO: implement
    }
    
    private void loadProfileImpl(ISymmetricEngine engine, MultipartFile file) {
       
        IDataLoaderService dataLoaderService = engine.getDataLoaderService();
        boolean inError = false;
        try {
            String content = new String(file.getBytes());
            List<IncomingBatch> batches = dataLoaderService.loadDataBatch(content);
            for (IncomingBatch batch : batches) {
                if (batch.getStatus() == Status.ER) {
                    inError = true;
                }
            }
        } catch (Exception e) {
            inError = true;
        }
        if (inError) {
            throw new InternalServerErrorException();
        }
    }

    private NodeList childrenImpl(ISymmetricEngine engine) {
    	NodeList children = new NodeList();
    	Node xmlChildNode = null;
    	
    	INodeService nodeService = engine.getNodeService();
    	org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity();

    	if (isRootNode(engine, modelNode)) {
    		NetworkedNode networkedNode = nodeService.getRootNetworkedNode();
    		Set<NetworkedNode> childNetwork = networkedNode.getChildren();
    		for (NetworkedNode child : childNetwork) {
    			xmlChildNode = new Node();
    			xmlChildNode.setName(child.getNode().getNodeId());
    			xmlChildNode.setRootNode(false);
    			xmlChildNode.setSyncUrl(child.getNode().getSyncUrl());
    			children.addNode(xmlChildNode);
    		}
    	}
    	return children;
    }    

    private Node nodeImpl(ISymmetricEngine engine) {      	
    	    	
    	INodeService nodeService = engine.getNodeService();
    	Node xmlNode = new Node(); 
    	org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity();    	
    	xmlNode.setName(modelNode.getNodeId());
    	xmlNode.setRootNode(isRootNode(engine, modelNode));
    	xmlNode.setSyncUrl(modelNode.getSyncUrl());
    	return xmlNode;    	
    }
    
    private boolean isRootNode(ISymmetricEngine engine, org.jumpmind.symmetric.model.Node node) {
    	boolean isRootNode = false;
    	INodeService nodeService = engine.getNodeService(); 
    	org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity();    	
    	NetworkedNode rootNode = nodeService.getRootNetworkedNode();    	    	
    	if (rootNode.getNode().equals(modelNode)) {
    		isRootNode = true;
    	}    	
    	return isRootNode;
    }
    
    private NodeStatus nodeStatusImpl(ISymmetricEngine engine) {
    	
    	INodeService nodeService = engine.getNodeService();
    	org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity(false);
    	NodeSecurity nodeSecurity = nodeService.findNodeSecurity(modelNode.getNodeId());
    	List<NodeHost> nodeHost = nodeService.findNodeHosts(modelNode.getNodeId());
    	
    	NodeStatus status = new NodeStatus();
    	status.setStarted(engine.isStarted());
    	status.setRegistered(nodeSecurity.getRegistrationTime() != null);
    	status.setInitialLoaded(nodeSecurity.getInitialLoadTime() != null);
    	status.setNodeId(modelNode.getNodeId());
    	status.setNodeGroupId(modelNode.getNodeGroupId());
    	status.setExternalId(modelNode.getExternalId());
    	status.setSyncUrl(modelNode.getSyncUrl());
    	status.setDatabaseType(modelNode.getDatabaseType());
    	status.setDatabaseVersion(modelNode.getDatabaseVersion());
    	status.setSyncEnabled(modelNode.isSyncEnabled());
    	status.setCreatedAtNodeId(modelNode.getCreatedAtNodeId());
    	status.setBatchToSendCount(modelNode.getBatchToSendCount());
    	status.setBatchInErrorCount(modelNode.getBatchInErrorCount());
    	status.setDeploymentType(modelNode.getDeploymentType());
    	
    	if (nodeHost != null && nodeHost.size() > 0) {
    		status.setLastHeartbeat(nodeHost.get(0).getHeartbeatTime().toString());
    	}
    	return status;
    }
    
    private Set<ChannelStatus> channelStatusImpl(ISymmetricEngine engine) {
    	//TODO:implement
    	return new HashSet<ChannelStatus>();
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
        } else if (!engine.getParameterService().is(ParameterConstants.REST_API_ENABLED)) {
            throw new NotAllowedException("The REST API was not enabled for %s", engine.getEngineName());
        } else {
            return engine;
        }
    }
    
    protected ISymmetricEngine getSymmetricEngine() {
        ISymmetricEngine engine = null;
        SymmetricEngineHolder holder = getSymmetricEngineHolder();

        if (holder.getEngines().size() > 0) {
            engine = holder.getEngines().values().iterator().next();
        } 
        
        if (engine == null) {
        	throw new NotAllowedException();
        } else if (!engine.getParameterService().is(ParameterConstants.REST_API_ENABLED)) {
            throw new NotAllowedException("The REST API was not enabled for %s", engine.getEngineName());
        } else {
            return engine;
        }
        
        
    }

}
