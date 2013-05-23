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
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.NodeChannel;
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
import org.jumpmind.symmetric.web.rest.model.PullDataResults;
import org.jumpmind.symmetric.web.rest.model.QueryResults;
import org.jumpmind.symmetric.web.rest.model.RegistrationInfo;
import org.jumpmind.symmetric.web.rest.model.RestError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * This is a REST API for SymmetricDS. The API will be active only if
 * rest.api.enable=true. The property is turned off by default. The REST API is
 * available at http://hostname:port/api for the stand alone SymmetricDS
 * installation.  
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
 * <li>HTTP 401 - Unauthorized. You have not successfully authenticated.
 * Authentication details are in the response body.</li>
 * <li>HTTP 404 - Not Found. You attempted to perform an operation on a resource
 * that doesn't exist. I.E. you tried to start or stop an engine that doesn't
 * exist.</li>
 * <li>HTTP 405 - Method Not Allowed. I.E. you attempted a service call that
 * uses the default engine (/engine/identity vs engine/{engine}/identity) and
 * there was more than one engine found on the server.</li>
 * <li>HTTP 500 - Internal Server Error. Something went wrong on the server /
 * service, and we couldn't fulfill the request. Details are in the response
 * body.</li>
 * </ul>
 * </li>
 * <li>
 * GET Methods
 * <ul>
 * <li>HTTP 200 - Success with result contained in the response body.</li>
 * <li>HTTP 204 - Success with no results. Your GET request completed
 * successfully, but found no matching entities.</li>
 * </ul>
 * </ul>
 * </p>
 */
@Controller
public class RestService {

    protected final Logger log = LoggerFactory.getLogger(getClass());
	
    @Autowired
    ServletContext context;

    /**
     * Provides a list of {@link Engine} that are configured on the node.
     * 
     * @return {@link EngineList} - Engines configured on the node <br>
     * 
     *         <pre>
     * Example xml reponse is as follows:<br><br>
     *   {@code
     *   <enginelist>
     *      <engines>
     *         <name>RootSugarDB-root</name>
     *      </engines>
     *   </enginelist>
     *   }
     * <br>
     * Example json response is as follows:<br><br>
     *   {"engines":[{"name":"RootSugarDB-root"}]}
     * </pre>
     */
    @RequestMapping(value = "/enginelist", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
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
     *   <node>
     *      <batchInErrorCount>0</batchInErrorCount>
     *      <batchToSendCount>0</batchToSendCount>
     *      <externalId>server01</externalId>
     *      <initialLoaded>true</initialLoaded>
     *      <lastHeartbeat>2012-12-20T09:26:02-05:00</lastHeartbeat>
     *      <name>server01</name>
     *      <registered>true</registered>
     *      <registrationServer>true</registrationServer>
     *      <reverseInitialLoaded>false</reverseInitialLoaded>
     *      <syncUrl>http://machine-name:31415/sync/RootSugarDB-root</syncUrl>
     *    </node>
	 *   }
	 * <br>
	 * Example json response is as follows:<br><br>
	 * {"name":"server01","externalId":"server01","registrationServer":true,"syncUrl":"http://machine-name:31415/sync/RootSugarDB-root","batchToSendCount":0,"batchInErrorCount":0,"lastHeartbeat":1356013562000,"registered":true,"initialLoaded":true,"reverseInitialLoaded":false}
	 * </pre>
	 */
    @RequestMapping(value = "engine/node", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final Node getNode() {
        return nodeImpl(getSymmetricEngine());
    }

    /**
     * Provides Node information for the specified engine
     */
    @RequestMapping(value = "engine/{engine}/node", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final Node getNode(@PathVariable("engine") String engineName) {
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
     *   <nodelist>
     *      <nodes>
     *         <batchInErrorCount>0</batchInErrorCount>
     *         <batchToSendCount>0</batchToSendCount>
     *         <externalId>client01</externalId>
     *         <initialLoaded>true</initialLoaded>
     *         <name>client01</name>
     *         <registered>true</registered>
     *         <registrationServer>false</registrationServer>
     *         <reverseInitialLoaded>false</reverseInitialLoaded>
     *         <syncUrl>http://machine-name:31418/sync/ClientSugarDB-client01</syncUrl>
     *      </nodes>
     *   </nodelist>
     *   }
     * <br>
     * Example json response is as follows:<br><br>
     * {"nodes":[{"name":"client01","externalId":"client01","registrationServer":false,"syncUrl":"http://gwilmer-laptop:31418/sync/ClientSugarDB-client01","batchToSendCount":0,"batchInErrorCount":0,"lastHeartbeat":null,"registered":true,"initialLoaded":true,"reverseInitialLoaded":false}]}
     * </pre>
     */
    @RequestMapping(value = "engine/children", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final NodeList getChildren() {
        return childrenImpl(getSymmetricEngine());
    }

    /**
     * Provides a list of children {@link Node} that are registered with this
     * engine.
     */
    @RequestMapping(value = "engine/{engine}/children", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final NodeList getChildrenByEngine(@PathVariable("engine") String engineName) {
        return childrenImpl(getSymmetricEngine(engineName));
    }

    /**
     * Takes a snapshot for this engine and streams it to the client.  The result of this 
     * call is a stream that should be written to a zip file.  The zip contains configuration
     * and operational information about the installation and can be used to diagnose
     * state of the node 
     */
    @RequestMapping(value = "engine/snapshot", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final void getSnapshot(HttpServletResponse resp) {
        getSnapshot(getSymmetricEngine().getEngineName(), resp);
    }

    /**
     * Executes a select statement on the node and returns results.
     * <br>
     * Example json response is as follows:<br><br>
     * {"nbrResults":1,"results":[{"rowNum":1,"columnData":[{"ordinal":1,"name":"node_id","value":"root"}]}]}
     * 
     */
    @RequestMapping(value = "engine/querynode", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final QueryResults queryNode(@RequestParam(value = "query") String sql) {
        return queryNodeImpl(getSymmetricEngine(), sql);
    }

    /**
     * Executes a select statement on the node and returns results.
     */
    @RequestMapping(value = "engine/{engine}/querynode", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final QueryResults queryNode(@PathVariable("engine") String engineName,
    		@RequestParam(value = "query") String sql) {
        return queryNodeImpl(getSymmetricEngine(engineName), sql);
    }

    /**
     * Takes a snapshot for the specified engine and streams it to the client.
     */
    @RequestMapping(value = "engine/{engine}/snapshot", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final void getSnapshot(@PathVariable("engine") String engineName, HttpServletResponse resp) {
        BufferedInputStream bis = null;
        try {
            ISymmetricEngine engine = getSymmetricEngine(engineName);
            File file = engine.snapshot();
            resp.setHeader("Content-Disposition",
                    String.format("attachment; filename=%s", file.getName()));
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
     * @param file
     *            A file stream that contains the profile itself.
     */
    @RequestMapping(value = "engine/profile", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postProfile(@RequestParam MultipartFile file) {
        loadProfileImpl(getSymmetricEngine(), file);
    }

    /**
     * Loads a configuration profile for the specified engine on the node.
     * 
     * @param file
     *            A file stream that contains the profile itself.
     */
    @RequestMapping(value = "engine/{engine}/profile", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postProfileByEngine(@PathVariable("engine") String engineName,
            @RequestParam(value = "file") MultipartFile file) {

        loadProfileImpl(getSymmetricEngine(engineName), file);
    }

    /**
     * Starts the single engine on the node
     */
    @RequestMapping(value = "engine/start", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postStart() {
        startImpl(getSymmetricEngine());
    }

    /**
     * Starts the specified engine on the node
     */
    @RequestMapping(value = "engine/{engine}/start", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postStartByEngine(@PathVariable("engine") String engineName) {
        startImpl(getSymmetricEngine(engineName));
    }

    /**
     * Stops the single engine on the node
     */
    @RequestMapping(value = "engine/stop", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postStop() {
        stopImpl(getSymmetricEngine());
    }

    /**
     * Stops the specified engine on the node
     */
    @RequestMapping(value = "engine/{engine}/stop", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postStopByEngine(@PathVariable("engine") String engineName) {
        stopImpl(getSymmetricEngine(engineName));
    }

    /**
     * Creates instances of triggers for each entry configured table/trigger for
     * the single engine on the node
     */
    @RequestMapping(value = "engine/synctriggers", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postSyncTriggers(
            @RequestParam(required = false, value = "force") boolean force) {
        syncTriggersImpl(getSymmetricEngine(), force);
    }

    /**
     * Creates instances of triggers for each entry configured table/trigger for
     * the specified engine on the node
     */
    @RequestMapping(value = "engine/{engine}/synctriggers", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postSyncTriggersByEngine(@PathVariable("engine") String engineName,
            @RequestParam(required = false, value = "force") boolean force) {
        syncTriggersImpl(getSymmetricEngine(engineName), force);
    }

    /**
     * Removes instances of triggers for each entry configured table/trigger for
     * the single engine on the node
     */
    @RequestMapping(value = "engine/droptriggers", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postDropTriggers() {
        dropTriggersImpl(getSymmetricEngine());
    }

    /**
     * Removes instances of triggers for each entry configured table/trigger for
     * the specified engine on the node
     */
    @RequestMapping(value = "engine/{engine}/droptriggers", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postDropTriggersByEngine(@PathVariable("engine") String engineName) {
        dropTriggersImpl(getSymmetricEngine(engineName));
    }

    /**
     * Removes instances of triggers for the specified table for the single
     * engine on the node
     */
    @RequestMapping(value = "engine/table/{table}/droptriggers", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postDropTriggersByTable(@PathVariable("table") String tableName) {
        dropTriggersImpl(getSymmetricEngine(), tableName);
    }

    /**
     * Removes instances of triggers for the specified table for the single
     * engine on the node
     * 
     */
    @RequestMapping(value = "engine/{engine}/table/{table}/droptriggers", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postDropTriggersByEngineByTable(@PathVariable("engine") String engineName,
            @PathVariable("table") String tableName) {
        dropTriggersImpl(getSymmetricEngine(engineName), tableName);
    }

    /**
     * Uninstalls all SymmetricDS objects from the given node (database) for the
     * single engine on the node
     */
    @RequestMapping(value = "engine/uninstall", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postUninstall() {
        uninstallImpl(getSymmetricEngine());
    }

    /**
     * Uninstalls all SymmetricDS objects from the given node (database) for the
     * specified engine on the node
     * 
     */
    @RequestMapping(value = "engine/{engine}/uninstall", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postUninstallByEngine(@PathVariable("engine") String engineName) {
        uninstallImpl(getSymmetricEngine(engineName));
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
    public final void postClearCaches() {
        clearCacheImpl(getSymmetricEngine());
    }

    /**
     * Refreshes cache for the specified engine on the node node
     * 
     */
    @RequestMapping(value = "engine/{engine}/refreshcache", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postClearCachesByEngine(@PathVariable("engine") String engineName) {
        clearCacheImpl(getSymmetricEngine(engineName));
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
     * @return Set<{@link ChannelStatus}>
     */
    @RequestMapping(value = "/engine/{engine}/channelstatus", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> getChannelStatusByEngine(
            @PathVariable("engine") String engineName) {
        return channelStatusImpl(getSymmetricEngine(engineName));
    }

    /**
     * Removes (unregisters and cleans up) a node for the single engine 
     */
    @RequestMapping(value = "/engine/removenode", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postRemoveNode(@RequestParam(value = "nodeId") String nodeId) {
        postRemoveNodeByEngine(nodeId, getSymmetricEngine().getEngineName());
    }
    
    /**
     * Removes (unregisters and cleans up) a node for the single engine 
     */
    @RequestMapping(value = "/engine/{engine}/removenode", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postRemoveNodeByEngine(@RequestParam(value = "nodeId") String nodeId,
    	    @PathVariable("engine") String engineName) {
        getSymmetricEngine(engineName).removeAndCleanupNode(nodeId);
    }    

    /**
     * Requests the server to add this node to the synchronization scenario
     * @param externalId - The external id for this node
     * @param nodeGroup - The node group to which this node belongs
     * @param databaseType - The database type for this node
     * @param databaseVersion - the database version for this node
     * @return {@link RegistrationInfo}
     */
    @RequestMapping(value = "/engine/registernode", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final RegistrationInfo registerNode(@RequestParam(value = "externalId") String externalId,
    		@RequestParam(value = "nodeGroup") String nodeGroup,
    		@RequestParam(value = "databaseType") String databaseType,
    		@RequestParam(value = "databaseVersion") String databaseVersion 		
    		) {
        //TODO: implement
    	return null;
    }
    
    /**
     * 
     * @param nodeId - The node id of the node requesting to pull data
     * @param communicationType - The communication type, either (PULL, PUSH, FILE_PUSH, FILE_PULL)
     * @return 
     */
    @RequestMapping(value = "/engine/pulldata", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final PullDataResults pullData(@RequestParam(value = "nodeId") String nodeId,
    		@RequestParam(value = "communicationtype") String communicationType) {
        //TODO: implement
    	return null;
    }
    
    /**
     * Sends a heartbeat to the server for the given node.  
     * @param nodeID - Required - The client nodeId this to which this heartbeat belongs
     * param hostName - Optional - The hostName for the machine on which the client node is running
     * @param ipAddress - Optional - The IP address for the machine on which the client node is running
     * @param osUser - Optional - The logged in user for the machine on which the client node is running
     * @param osName - Optional - The name of the operating system on which the client node is running
     * @param osArchitecture - Optional - The operating system architecture (i.e. amd, intel) on which the client node is running
     * @param osVersion - Optional -The operating system version on which the client node is running 
     * @param availableProcessors - Optional - The number of available processors on the machine on which the client node is running
     * @param freeMemoryBytes - Optional - The amount of memory free (in bytes) on the machine on which the client node is running 
     * @param totalMemoryBytes - Optional - The total amount of memory (in bytes) on the machine on which the client node is running
     * @param maxMemoryBytes - Optional - The maximum amount of memory (in bytes) on the machine on which the client node is running
     * @param javaVersion - Optional - The version of java currently being run on the machine on which the client node is running
     * @param javaVendor - Optional - The java provided begin run on the machine on which the client node is running
     * @param symmetricVersion - Optional - The symmetric version on the machine on which the client node is running
     * @param timezoneOffset - Optional - The timezone offset on the machine on which the client node is running
     * @param heartbeatTime - Optional - The heartbeat time for which this heartbeat pertains
     * @param lastRestartTime - Optional - The last restart time of symmetric on the machine on which the client node is running
     * @param createTime - Optional - The Symmetric instance create time for the machine on which the client node is running 
     */
    @RequestMapping(value = "/engine/heartbeat", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postHeartbeat(@RequestParam(value = "nodeId") String nodeID, 
		@RequestParam(value = "hostName") String hostName,
		@RequestParam(value = "ipAddress") String ipAddress,
		@RequestParam(value = "osUser") String osUser,		
		@RequestParam(value = "osName") String osName,
		@RequestParam(value = "osArchitecture") String osArchitecture,
		@RequestParam(value = "osVersion") String osVersion,
		@RequestParam(value = "availableProcessors") Integer availableProcessors,
		@RequestParam(value = "freeMemoryBytes") Long freeMemoryBytes,
		@RequestParam(value = "totalMemoryBytes") Long totalMemoryBytes,
		@RequestParam(value = "maxMemoryBytes") Long maxMemoryBytes,		
		@RequestParam(value = "javaVersion") String javaVersion,
		@RequestParam(value = "javaVendor") String javaVendor,
		@RequestParam(value = "symmetricVersion") String symmetricVersion,
		@RequestParam(value = "timezoneOffset") String timezoneOffset,
		@RequestParam(value = "heartbeatTime") Date heartbeatTime,
		@RequestParam(value = "lastRestartTime") Date lastRestartTime,
		@RequestParam(value = "createTime") Date createTime
		) {    	
    	//TODO: implement
    }
    
    /**
     * Acknowledges a batch that has been pulled and processed on the client side.  Setting
     * the status to OK will render the batch complete.  Setting the status to anything other 
     * than OK will queue the batch on the server to be sent again on the next pull.
     * @param nodeID - The node id that is acknowledging the batch
     * @param batchId - The batch id that is being acknowledged
     * @param status - The status of the batch, either "OK" or "ER"
     * @param statusDescription - A description of the status.  This is particularly important 
     * if the status is "ER".  In error status the status description should contain relevant
     * information about the error on the client including SQL Error Number and description
     */
    @RequestMapping(value = "/engine/acknowledgebatch", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postAcknowledgeBatch(@RequestParam(value = "nodeId") String nodeID, 
		@RequestParam(value = "batchId") Long batchId,
		@RequestParam(value = "status") String status,
		@RequestParam(value = "statusDescription") String statusDescription
		) {    	
    	//TODO: implement
    }

    
    /**
     * Requests an initial load from the server for the node id provided.  The initial load requst
     * directs the server to queue up initial load data for the client node.  Data is obtained for 
     * the initial load by the client calling the pull method.
     * @param nodeID
     */
    @RequestMapping(value = "/engine/requestinitialload", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postRequestInitialLoad(@RequestParam(value = "nodeId") String nodeID) {    	
    	//TODO: implement
    }

    @ExceptionHandler(Exception.class)
    @ResponseBody
    protected RestError handleError(Exception ex, HttpServletRequest req) {
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

        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        StringBuilder buffer = new StringBuilder();
        triggerRouterService.syncTriggers(buffer, force);
    }

    private void dropTriggersImpl(ISymmetricEngine engine) {
        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        triggerRouterService.dropTriggers();
    }

    private void dropTriggersImpl(ISymmetricEngine engine, String tableName) {
        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        HashSet<String> tables = new HashSet<String>();
        tables.add(tableName);
        triggerRouterService.dropTriggers(tables);
    }

    private void uninstallImpl(ISymmetricEngine engine) {
        engine.uninstall();
    }

    private void reinitializeImpl(ISymmetricEngine engine) {
        INodeService nodeService = engine.getNodeService();
        org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity();

        if (!this.isRootNode(engine, modelNode)) {
            engine.uninstall();
        }

        engine.start();
    }

    private void clearCacheImpl(ISymmetricEngine engine) {
        engine.clearCaches();
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
        
        if (isRegistered(engine)) {
	        if (isRootNode(engine, modelNode)) {
	            NetworkedNode networkedNode = nodeService.getRootNetworkedNode();
	            Set<NetworkedNode> childNetwork = networkedNode.getChildren();
	            if (childNetwork != null) {
		            for (NetworkedNode child : childNetwork) {
		            	
		                List<NodeHost> nodeHosts = nodeService.findNodeHosts(child.getNode().getNodeId());
		                NodeSecurity nodeSecurity = nodeService.findNodeSecurity(child.getNode().getNodeId());        
		            	            	
		                xmlChildNode = new Node();
		                xmlChildNode.setNodeId(child.getNode().getNodeId());
		                xmlChildNode.setExternalId(child.getNode().getExternalId());
		                xmlChildNode.setRegistrationServer(false);
		                xmlChildNode.setSyncUrl(child.getNode().getSyncUrl());
		                
		                xmlChildNode.setBatchInErrorCount(child.getNode().getBatchInErrorCount());
		                xmlChildNode.setBatchToSendCount(child.getNode().getBatchToSendCount());
		                if (nodeHosts.size()>0) {
		                	xmlChildNode.setLastHeartbeat(nodeHosts.get(0).getHeartbeatTime());
		                }
		                xmlChildNode.setRegistered(nodeSecurity.hasRegistered());
		                xmlChildNode.setInitialLoaded(nodeSecurity.hasInitialLoaded());
		                xmlChildNode.setReverseInitialLoaded(nodeSecurity.hasReverseInitialLoaded());
		                if (child.getNode().getCreatedAtNodeId() == null) {
		                	xmlChildNode.setRegistrationServer(true);        	
		                }
		                children.addNode(xmlChildNode);
		            }
	            }
	        }
        } else {
        	throw new NotFoundException();
        }
        return children;
    }

    private Node nodeImpl(ISymmetricEngine engine) {
    	
        Node xmlNode = new Node();
    	if (isRegistered(engine)) {
	        INodeService nodeService = engine.getNodeService();
	        org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity(false);
	        List<NodeHost> nodeHosts = nodeService.findNodeHosts(modelNode.getNodeId());
	        NodeSecurity nodeSecurity = nodeService.findNodeSecurity(modelNode.getNodeId());
	        xmlNode.setNodeId(modelNode.getNodeId());
	        xmlNode.setExternalId(modelNode.getExternalId());
	        xmlNode.setSyncUrl(modelNode.getSyncUrl());
	        xmlNode.setRegistrationUrl(engine.getParameterService().getRegistrationUrl());
	        xmlNode.setBatchInErrorCount(modelNode.getBatchInErrorCount());
	        xmlNode.setBatchToSendCount(modelNode.getBatchToSendCount());
	        if (nodeHosts.size() > 0) {
	        	xmlNode.setLastHeartbeat(nodeHosts.get(0).getHeartbeatTime());
	        }
	        xmlNode.setHeartbeatInterval(engine.getParameterService().getInt("job.heartbeat.period.time.ms"));
	        xmlNode.setRegistered(nodeSecurity.hasRegistered());
	        xmlNode.setInitialLoaded(nodeSecurity.hasInitialLoaded());
	        xmlNode.setReverseInitialLoaded(nodeSecurity.hasReverseInitialLoaded());
	        if (modelNode.getCreatedAtNodeId() == null) {
	        	xmlNode.setRegistrationServer(true);       	
	        } else {
	        	xmlNode.setRegistrationServer(false);
	        }
	        xmlNode.setCreatedAtNodeId(modelNode.getCreatedAtNodeId());
    	} else {
    		throw new NotFoundException();
    	}
        return xmlNode;
    }

    private boolean isRootNode(ISymmetricEngine engine, org.jumpmind.symmetric.model.Node node) {
        INodeService nodeService = engine.getNodeService();
        org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity();
        if (modelNode.getCreatedAtNodeId() == null ||
        		modelNode.getCreatedAtNodeId().equalsIgnoreCase(modelNode.getExternalId())) {
        	return true;        	
        } else {
        	return false;
        }
    }

    private boolean isRegistered(ISymmetricEngine engine) {
    	boolean registered = true;
    	INodeService nodeService = engine.getNodeService();
		org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity(false);
		if (modelNode == null) {
			registered = false;    			
		} else {
	        NodeSecurity nodeSecurity = nodeService.findNodeSecurity(modelNode.getNodeId());
			if (nodeSecurity == null) {
				registered = false;
			}
		}    		
    	return registered;
    }
    
    private NodeStatus nodeStatusImpl(ISymmetricEngine engine) {

        NodeStatus status = new NodeStatus();
    	if (isRegistered(engine)) {
	        INodeService nodeService = engine.getNodeService();
	        org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity(false);
	        NodeSecurity nodeSecurity = nodeService.findNodeSecurity(modelNode.getNodeId());
	        List<NodeHost> nodeHost = nodeService.findNodeHosts(modelNode.getNodeId());
	        status.setStarted(engine.isStarted());
	        status.setRegistered(nodeSecurity.getRegistrationTime() != null);
	        status.setInitialLoaded(nodeSecurity.getInitialLoadTime() != null);
	        status.setReverseInitialLoaded(nodeSecurity.getRevInitialLoadTime() != null);
	        status.setNodeId(modelNode.getNodeId());
	        status.setNodeGroupId(modelNode.getNodeGroupId());
	        status.setExternalId(modelNode.getExternalId());
	        status.setSyncUrl(modelNode.getSyncUrl());
	        status.setRegistrationUrl(engine.getParameterService().getRegistrationUrl());
	        status.setDatabaseType(modelNode.getDatabaseType());
	        status.setDatabaseVersion(modelNode.getDatabaseVersion());
	        status.setSyncEnabled(modelNode.isSyncEnabled());
	        status.setCreatedAtNodeId(modelNode.getCreatedAtNodeId());
	        status.setBatchToSendCount(engine.getOutgoingBatchService().countOutgoingBatchesUnsent());
	        status.setBatchInErrorCount(engine.getOutgoingBatchService().countOutgoingBatchesInError());
	        status.setDeploymentType(modelNode.getDeploymentType());
	        if (modelNode.getCreatedAtNodeId() == null) {
	        	status.setRegistrationServer(true);        	
	        } else {
	        	status.setRegistrationServer(false);
	        }
	        if (nodeHost != null && nodeHost.size() > 0) {
	            status.setLastHeartbeat(nodeHost.get(0).getHeartbeatTime());
	        }
	        status.setHeartbeatInterval(engine.getParameterService().getInt("job.heartbeat.period.time.ms"));
	        if (status.getHeartbeatInterval() == 0) {
	        	status.setHeartbeatInterval(300000);
	        }
    	} else {
    		throw new NotFoundException();
    	}
        return status;
    }

    private Set<ChannelStatus> channelStatusImpl(ISymmetricEngine engine) {
        HashSet<ChannelStatus> channelStatus = new HashSet<ChannelStatus>();
        List<NodeChannel> channels = engine.getConfigurationService().getNodeChannels(false);
        for (NodeChannel nodeChannel : channels) {
            String channelId = nodeChannel.getChannelId();
            ChannelStatus status = new ChannelStatus();
            status.setChannelId(channelId);
            int outgoingInError = engine.getOutgoingBatchService().countOutgoingBatchesInError(
                    channelId);
            int incomingInError = engine.getIncomingBatchService().countIncomingBatchesInError(
                    channelId);
            status.setBatchInErrorCount(outgoingInError);
            status.setBatchToSendCount(engine.getOutgoingBatchService().countOutgoingBatchesUnsent(
                    channelId));
            status.setIncomingError(incomingInError > 0);
            status.setOutgoingError(outgoingInError > 0);
            status.setEnabled(nodeChannel.isEnabled());
            status.setIgnoreEnabled(nodeChannel.isIgnoreEnabled());
            status.setSuspendEnabled(nodeChannel.isSuspendEnabled());
        }
        return channelStatus;
    }

    private QueryResults queryNodeImpl(ISymmetricEngine engine, String sql) {
    	
    	QueryResults results = new QueryResults();
    	org.jumpmind.symmetric.web.rest.model.Row xmlRow = null;
    	org.jumpmind.symmetric.web.rest.model.Column xmlColumn = null;
    	
    	ISqlTemplate sqlTemplate = engine.getSqlTemplate();
    	try {
    		List<Row> rows = sqlTemplate.query(sql);
    		int nbrRows=0;
	    	for (Row row : rows) {
	    		xmlRow = new org.jumpmind.symmetric.web.rest.model.Row();
	    		Iterator<Map.Entry<String, Object>> itr = row.entrySet().iterator();
	    		int columnOrdinal=0;
	    		while (itr.hasNext()) {
	    			xmlColumn = new org.jumpmind.symmetric.web.rest.model.Column();
	    			xmlColumn.setOrdinal(++columnOrdinal);
	    			Map.Entry<String, Object> pair = (Map.Entry<String, Object>)itr.next();
	    			xmlColumn.setName(pair.getKey());
	    			if (pair.getValue()!= null) {
	    				xmlColumn.setValue(pair.getValue().toString());
	    			}
	    			xmlRow.getColumnData().add(xmlColumn);
	    		}
    			xmlRow.setRowNum(++nbrRows);
	    		results.getResults().add(xmlRow);
	    	}
	    	results.setNbrResults(nbrRows);
    	} catch (Exception ex) {
    		log.error("Exception while executing sql.", ex);
            throw new NotAllowedException("Error while executing sql %s.  Error is %s",
                    sql, ex.getCause().getMessage());
    	}
    	return results;
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
            throw new NotAllowedException("The REST API was not enabled for %s",
                    engine.getEngineName());
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
            throw new NotAllowedException("The REST API was not enabled for %s",
                    engine.getEngineName());
        } else {
            return engine;
        }

    }

}
