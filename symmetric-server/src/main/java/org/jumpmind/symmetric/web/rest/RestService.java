/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.MDC;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.writer.StructureDataWriter.PayloadType;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.BatchAckResult;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeHost;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchSummary;
import org.jumpmind.symmetric.model.OutgoingBatchWithPayload;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.web.ServerSymmetricEngine;
import org.jumpmind.symmetric.web.SymmetricEngineHolder;
import org.jumpmind.symmetric.web.WebConstants;
import org.jumpmind.symmetric.web.rest.model.Batch;
import org.jumpmind.symmetric.web.rest.model.BatchAckResults;
import org.jumpmind.symmetric.web.rest.model.BatchResult;
import org.jumpmind.symmetric.web.rest.model.BatchResults;
import org.jumpmind.symmetric.web.rest.model.BatchSummaries;
import org.jumpmind.symmetric.web.rest.model.BatchSummary;
import org.jumpmind.symmetric.web.rest.model.ChannelStatus;
import org.jumpmind.symmetric.web.rest.model.Engine;
import org.jumpmind.symmetric.web.rest.model.EngineList;
import org.jumpmind.symmetric.web.rest.model.Heartbeat;
import org.jumpmind.symmetric.web.rest.model.Node;
import org.jumpmind.symmetric.web.rest.model.NodeList;
import org.jumpmind.symmetric.web.rest.model.NodeStatus;
import org.jumpmind.symmetric.web.rest.model.PullDataResults;
import org.jumpmind.symmetric.web.rest.model.QueryResults;
import org.jumpmind.symmetric.web.rest.model.RegistrationInfo;
import org.jumpmind.symmetric.web.rest.model.RestError;
import org.jumpmind.symmetric.web.rest.model.SendSchemaRequest;
import org.jumpmind.symmetric.web.rest.model.SendSchemaResponse;
import org.jumpmind.symmetric.web.rest.model.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MultipartFile;

import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

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
    @ApiOperation(value = "Obtain a list of configured Engines")
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
    @ApiOperation(value = "Obtain node information for the single engine")
    @RequestMapping(value = "engine/node", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final Node getNode() {
        return nodeImpl(getSymmetricEngine());
    }

    /**
     * Provides Node information for the specified engine
     */
    @ApiOperation(value = "Obtain node information for he specified engine")
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
    @ApiOperation(value = "Obtain list of children for the single engine")
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
    @ApiOperation(value = "Obtain list of children for the specified engine")
    @RequestMapping(value = "engine/{engine}/children", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final NodeList getChildrenByEngine(@PathVariable("engine") String engineName) {
        return childrenImpl(getSymmetricEngine(engineName));
    }

    /**
     * Takes a snapshot for this engine and streams it to the client. The result
     * of this call is a stream that should be written to a zip file. The zip
     * contains configuration and operational information about the installation
     * and can be used to diagnose state of the node
     */
    @ApiOperation(value = "Take a diagnostic snapshot for the single engine")
    @RequestMapping(value = "engine/snapshot", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final void getSnapshot(HttpServletResponse resp) {
        getSnapshot(getSymmetricEngine().getEngineName(), resp);
    }

    /**
     * Executes a select statement on the node and returns results. <br>
     * Example json response is as follows:<br>
     * <br>
     * {"nbrResults":1,"results":[{"rowNum":1,"columnData":[{"ordinal":1,"name":
     * "node_id","value":"root"}]}]}
     * 
     */
    @ApiOperation(value = "Execute the specified SQL statement on the single engine")
    @RequestMapping(value = "engine/querynode", method = {RequestMethod.GET, RequestMethod.POST})
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final QueryResults getQueryNode(@RequestParam(value = "query") String sql) {
        return queryNodeImpl(getSymmetricEngine(), sql);
    }

    /**
     * Executes a select statement on the node and returns results.
     */
    @ApiOperation(value = "Execute the specified SQL statement for the specified engine")
    @RequestMapping(value = "engine/{engine}/querynode", method = {RequestMethod.GET, RequestMethod.POST})
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final QueryResults getQueryNode(@PathVariable("engine") String engineName,
            @RequestParam(value = "query") String sql) {
        return queryNodeImpl(getSymmetricEngine(engineName), sql);
    }

    /**
     * Takes a snapshot for the specified engine and streams it to the client.
     */
    @ApiOperation(value = "Take a diagnostic snapshot for the specified engine")
    @RequestMapping(value = "engine/{engine}/snapshot", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final void getSnapshot(@PathVariable("engine") String engineName,
            HttpServletResponse resp) {
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
    @ApiOperation(value = "Load a configuration file to the single engine")
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
    @ApiOperation(value = "Load a configuration file to the specified engine")
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
    @ApiOperation(value = "Start the single engine")
    @RequestMapping(value = "engine/start", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postStart() {
        startImpl(getSymmetricEngine());
    }

    /**
     * Starts the specified engine on the node
     */
    @ApiOperation(value = "Start the specified engine")
    @RequestMapping(value = "engine/{engine}/start", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postStartByEngine(@PathVariable("engine") String engineName) {
        startImpl(getSymmetricEngine(engineName));
    }

    /**
     * Stops the single engine on the node
     */
    @ApiOperation(value = "Stop the single engine")
    @RequestMapping(value = "engine/stop", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postStop() {
        stopImpl(getSymmetricEngine());
    }

    /**
     * Stops the specified engine on the node
     */
    @ApiOperation(value = "Stop the specified engine")
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
    @ApiOperation(value = "Sync triggers on the single engine")
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
    @ApiOperation(value = "Sync triggers on the specified engine")
    @RequestMapping(value = "engine/{engine}/synctriggers", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postSyncTriggersByEngine(@PathVariable("engine") String engineName,
            @RequestParam(required = false, value = "force") boolean force) {
        syncTriggersImpl(getSymmetricEngine(engineName), force);
    }

    @ApiOperation(value = "Sync triggers on the single engine for a table")
    @RequestMapping(value = "engine/synctriggers/{table}", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postSyncTriggersByTable(@PathVariable("table") String tableName,
            @RequestParam(required = false, value = "catalog") String catalogName,
            @RequestParam(required = false, value = "schema") String schemaName,
            @RequestParam(required = false, value = "force") boolean force) {
        syncTriggersByTableImpl(getSymmetricEngine(), catalogName, schemaName, tableName, force);
    }

    @ApiOperation(value = "Sync triggers on the specific engine for a table")
    @RequestMapping(value = "engine/{engine}/synctriggers/{table}", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postSyncTriggersByTable(@PathVariable("engine") String engineName,
            @PathVariable("table") String tableName,
            @RequestParam(required = false, value = "catalog") String catalogName,
            @RequestParam(required = false, value = "schema") String schemaName,
            @RequestParam(required = false, value = "force") boolean force) {
        syncTriggersByTableImpl(getSymmetricEngine(engineName), catalogName, schemaName, tableName,
                force);
    }

    /**
     * Send schema updates for all tables or a list of tables to a list of nodes
     * or to all nodes in a group.
     * <p>
     * Example json request to send all tables to all nodes in group:<br>
     * { "nodeGroupIdToSendTo": "target_group_name" }
     * <p>
     * Example json request to send all tables to a list of nodes:<br>
     * { "nodeIdsToSendTo": [ "1", "2" ] }
     * <p>
     * Example json request to send a table to a list of nodes:<br>
     * { "nodeIdsToSendTo": ["1", "2"], "tablesToSend": [ {  "catalogName": "", "schemaName": "", "tableName": "A" } ] }
     * <p>
     * Example json response:
     * { "nodeIdsSentTo": { "1": [ {  "catalogName": null, "schemaName": null, "tableName": "A" } ] } }
     * 
     * @param engineName
     * @param request
     * @return {@link SendSchemaResponse}
     */
    @ApiOperation(value = "Send schema updates for all tables or a list of tables to a list of nodes or to all nodes in a group.")
    @RequestMapping(value = "engine/{engine}/sendschema", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final SendSchemaResponse postSendSchema(@PathVariable("engine") String engineName,
            @RequestBody SendSchemaRequest request) {
        return sendSchemaImpl(getSymmetricEngine(engineName), request);
    }

    /**
     * Send schema updates for all tables or a list of tables to a list of nodes
     * or to all nodes in a group. See
     * {@link RestService#postSendSchema(String, SendSchemaRequest)} for
     * additional details.
     * 
     * @param request
     * @return {@link SendSchemaResponse}
     */
    @ApiOperation(value = "Send schema updates for all tables or a list of tables to a list of nodes or to all nodes in a group.")
    @RequestMapping(value = "engine/sendschema", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final SendSchemaResponse postSendSchema(@RequestBody SendSchemaRequest request) {
        return sendSchemaImpl(getSymmetricEngine(), request);
    }

    /**
     * Removes instances of triggers for each entry configured table/trigger for
     * the single engine on the node
     */
    @ApiOperation(value = "Drop triggers on the single engine")
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
    @ApiOperation(value = "Drop triggers on the specified engine")
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
    @ApiOperation(value = "Drop triggers for the specified table on the single engine")
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
    @ApiOperation(value = "Drop triggers for the specified table on the specified engine")
    @RequestMapping(value = "engine/{engine}/table/{table}/droptriggers", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postDropTriggersByEngineByTable(@PathVariable("engine") String engineName,
            @PathVariable("table") String tableName) {
        dropTriggersImpl(getSymmetricEngine(engineName), tableName);
    }
    
    /**
     * Installs and starts a new node
     * 
     * @param file
     *            A file stream that contains the node's properties.
     */
    @ApiOperation(value = "Load a configuration file to the single engine")
    @RequestMapping(value = "engine/install", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postInstall(@RequestParam MultipartFile file) {
        try {
            Properties properties = new Properties();
            properties.load(file.getInputStream());
            getSymmetricEngineHolder().install(properties);
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }   

    /**
     * Uninstalls all SymmetricDS objects from the given node (database) for the
     * single engine on the node
     */
    @ApiOperation(value = "Uninstall SymmetricDS on the single engine")
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
    @ApiOperation(value = "Uninstall SymmetricDS on the specified engine")
    @RequestMapping(value = "engine/{engine}/uninstall", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postUninstallByEngine(@PathVariable("engine") String engineName) {
        uninstallImpl(getSymmetricEngine(engineName));
    }

    /**
     * Reinitializes the given node (database) for the single engine on the node
     */
    @ApiOperation(value = "Reinitiailize SymmetricDS on the single engine")
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
    @ApiOperation(value = "Reinitiailize SymmetricDS on the specified engine")
    @RequestMapping(value = "engine/{engine}/reinitialize", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postReinitializeByEngine(@PathVariable("engine") String engineName) {
        reinitializeImpl(getSymmetricEngine(engineName));
    }

    /**
     * Refreshes cache for the single engine on the node
     */
    @ApiOperation(value = "Refresh caches on the single engine")
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
    @ApiOperation(value = "Refresh caches on the specified engine")
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
    @ApiOperation(value = "Obtain the status of the single engine")
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
    @ApiOperation(value = "Obtain the status of the specified engine")
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
    @ApiOperation(value = "Obtain the channel status of the single engine")
    @RequestMapping(value = "/engine/channelstatus", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> getChannelStatus() {
        return channelStatusImpl(getSymmetricEngine());
    }

    /**
     * Returns status of each channel for the specified engine of the node.
     * 
     * @return Set<{@link ChannelStatus}>
     */
    @ApiOperation(value = "Obtain the channel status of the specified engine")
    @RequestMapping(value = "/engine/{engine}/channelstatus", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> getChannelStatusByEngine(
            @PathVariable("engine") String engineName) {
        return channelStatusImpl(getSymmetricEngine(engineName));
    }

    /**
     * Removes (unregisters and cleans up) a node for the single engine
     */
    @ApiOperation(value = "Remove specified node (unregister and clean up) for the single engine")
    @RequestMapping(value = "/engine/removenode", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postRemoveNode(@RequestParam(value = "nodeId") String nodeId) {
        postRemoveNodeByEngine(nodeId, getSymmetricEngine().getEngineName());
    }

    /**
     * Removes (unregisters and cleans up) a node for the single engine
     */
    @ApiOperation(value = "Remove specified node (unregister and clean up) for the specified engine")
    @RequestMapping(value = "/engine/{engine}/removenode", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postRemoveNodeByEngine(@RequestParam(value = "nodeId") String nodeId,
            @PathVariable("engine") String engineName) {
        getSymmetricEngine(engineName).removeAndCleanupNode(nodeId);
    }

    /**
     * Requests the server to add this node to the synchronization scenario as a
     * "pull only" node
     * 
     * @param externalId
     *            The external id for this node
     * @param nodeGroup
     *            The node group to which this node belongs
     * @param databaseType
     *            The database type for this node
     * @param databaseVersion
     *            The database version for this node
     * @param hostName
     *            The host name of the machine on which the client is running
     * @return {@link RegistrationInfo}
     * 
     *         <pre>
     * Example json response is as follows:<br/><br/>
     * {"registered":false,"nodeId":null,"syncUrl":null,"nodePassword":null}<br>
     * In the above example, the node attempted to register, but was not able to successfully register
     * because registration was not open on the server.  Checking the "registered" element will allow you
     * to determine whether the node was successfully registered.<br/><br/>
     * The following example shows the results from the registration after registration has been opened
     * on the server for the given node.<br/><br/>
     * {"registered":true,"nodeId":"001","syncUrl":"http://myserverhost:31415/sync/server-000","nodePassword":"1880fbffd2bc2d00e1d58bd0c734ff"}<br/>
     * The nodeId, syncUrl and nodePassword should be stored for subsequent calls to the REST API.
     * </pre>
     */
    @ApiOperation(value = "Register the specified node for the single engine")
    @RequestMapping(value = "/engine/registernode", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final RegistrationInfo postRegisterNode(
            @RequestParam(value = "externalId") String externalId,
            @RequestParam(value = "nodeGroupId") String nodeGroupId,
            @RequestParam(value = "databaseType") String databaseType,
            @RequestParam(value = "databaseVersion") String databaseVersion,
            @RequestParam(value = "hostName") String hostName) {
        return postRegisterNode(getSymmetricEngine().getEngineName(), externalId, nodeGroupId,
                databaseType, databaseVersion, hostName);
    }

    @ApiOperation(value = "Register the specified node for the specified engine")
    @RequestMapping(value = "/engine/{engine}/registernode", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final RegistrationInfo postRegisterNode(@PathVariable("engine") String engineName,
            @RequestParam(value = "externalId") String externalId,
            @RequestParam(value = "nodeGroupId") String nodeGroupId,
            @RequestParam(value = "databaseType") String databaseType,
            @RequestParam(value = "databaseVersion") String databaseVersion,
            @RequestParam(value = "hostName") String hostName) {

        ISymmetricEngine engine = getSymmetricEngine(engineName);
        IRegistrationService registrationService = engine.getRegistrationService();
        INodeService nodeService = engine.getNodeService();
        RegistrationInfo regInfo = new org.jumpmind.symmetric.web.rest.model.RegistrationInfo();

        try {
            org.jumpmind.symmetric.model.Node processedNode = registrationService
                    .registerPullOnlyNode(externalId, nodeGroupId, databaseType, databaseVersion);
            regInfo.setRegistered(processedNode.isSyncEnabled());
            if (regInfo.isRegistered()) {
                regInfo.setNodeId(processedNode.getNodeId());
                NodeSecurity nodeSecurity = nodeService.findNodeSecurity(processedNode.getNodeId());
                regInfo.setNodePassword(nodeSecurity.getNodePassword());
                org.jumpmind.symmetric.model.Node modelNode = nodeService.findIdentity();
                regInfo.setSyncUrl(modelNode.getSyncUrl());

                // do an initial heartbeat
                Heartbeat heartbeat = new Heartbeat();
                heartbeat.setNodeId(regInfo.getNodeId());
                heartbeat.setHostName(hostName);
                Date now = new Date();
                heartbeat.setCreateTime(now);
                heartbeat.setLastRestartTime(now);
                heartbeat.setHeartbeatTime(now);
                this.heartbeatImpl(engine, heartbeat);
            }

            // TODO: Catch a RegistrationRedirectException and redirect.
        } catch (IOException e) {
            throw new IoException(e);
        }
        return regInfo;
    }

    /**
     * Pulls pending batches (data) for a given node.
     * 
     * @param nodeId
     *            The node id of the node requesting to pull data
     * @param securityToken
     *            The security token or password used to authenticate the pull.
     *            The security token is provided during the registration
     *            process.
     * @param useJdbcTimestampFormat
     * @param useUpsertStatements
     * @param useDelimitedIdentifiers
     * @param hostName
     *            The name of the host machine requesting the pull. Only
     *            required if you have the rest heartbeat on pull paramter set.
     * @return {@link PullDataResults}
     * 
     *         Example json response is as follows:<br/>
     * <br/>
     *         {"nbrBatches":2,"batches":[{"batchId":20,"sqlStatements":[
     *         "insert into table1 (field1, field2) values (value1,value2);"
     *         ,"update table1 set field1=value1;"
     *         ]},{"batchId":21,"sqlStatements"
     *         :["insert into table2 (field1, field2) values (value1,value2);"
     *         ,"update table2 set field1=value1;"]}]}<BR>
     * <br/>
     *         If there are no batches to be pulled, the json response will look
     *         as follows:<br/>
     * <br/>
     *         {"nbrBatches":0,"batches":[]} </pre>
     */
    @ApiOperation(value = "Pull pending batches for the specified node for the single engine")
    @RequestMapping(value = "/engine/pulldata", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final PullDataResults getPullData(
            @RequestParam(value = WebConstants.NODE_ID) String nodeId,
            @ApiParam(value="This the password for the nodeId being passed in.  The password is stored in the node_security table") 
            @RequestParam(value = WebConstants.SECURITY_TOKEN) String securityToken,
            @RequestParam(value = "useJdbcTimestampFormat", required = false, defaultValue = "true") boolean useJdbcTimestampFormat,
            @RequestParam(value = "useUpsertStatements", required = false, defaultValue = "false") boolean useUpsertStatements,
            @RequestParam(value = "useDelimitedIdentifiers", required = false, defaultValue = "true") boolean useDelimitedIdentifiers,
            @RequestParam(value = "hostName", required = false) String hostName) {
        return getPullData(getSymmetricEngine().getEngineName(), nodeId, securityToken,
                useJdbcTimestampFormat, useUpsertStatements, useDelimitedIdentifiers, hostName);
    }

    @ApiOperation(value = "Pull pending batches for the specified node for the specified engine")
    @RequestMapping(value = "/engine/{engine}/pulldata", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final PullDataResults getPullData(
            @PathVariable("engine") String engineName,
            @RequestParam(value = WebConstants.NODE_ID) String nodeId,
            @ApiParam(value="This the password for the nodeId being passed in.  The password is stored in the node_security table.") 
            @RequestParam(value = WebConstants.SECURITY_TOKEN) String securityToken,
            @RequestParam(value = "useJdbcTimestampFormat", required = false, defaultValue = "true") boolean useJdbcTimestampFormat,
            @RequestParam(value = "useUpsertStatements", required = false, defaultValue = "false") boolean useUpsertStatements,
            @RequestParam(value = "useDelimitedIdentifiers", required = false, defaultValue = "true") boolean useDelimitedIdentifiers,
            @RequestParam(value = "hostName", required = false) String hostName) {

        ISymmetricEngine engine = getSymmetricEngine(engineName);

        IDataExtractorService dataExtractorService = engine.getDataExtractorService();
        IStatisticManager statisticManager = engine.getStatisticManager();
        INodeService nodeService = engine.getNodeService();
        org.jumpmind.symmetric.model.Node targetNode = nodeService.findNode(nodeId);

        if (securityVerified(nodeId, engine, securityToken)) {
            ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(
                    nodeService.findIdentityNodeId(), nodeId, ProcessType.REST_PULL_HANLDER));
            try {

                PullDataResults results = new PullDataResults();
                List<OutgoingBatchWithPayload> extractedBatches = dataExtractorService
                        .extractToPayload(processInfo, targetNode, PayloadType.SQL,
                                useJdbcTimestampFormat, useUpsertStatements,
                                useDelimitedIdentifiers);
                List<Batch> batches = new ArrayList<Batch>();
                for (OutgoingBatchWithPayload outgoingBatchWithPayload : extractedBatches) {
                    if (outgoingBatchWithPayload.getStatus() == org.jumpmind.symmetric.model.OutgoingBatch.Status.LD
                            || outgoingBatchWithPayload.getStatus() == org.jumpmind.symmetric.model.OutgoingBatch.Status.IG) {
                        Batch batch = new Batch();
                        batch.setBatchId(outgoingBatchWithPayload.getBatchId());
                        batch.setChannelId(outgoingBatchWithPayload.getChannelId());
                        batch.setSqlStatements(outgoingBatchWithPayload.getPayload());
                        batches.add(batch);
                    }
                }
                results.setBatches(batches);
                results.setNbrBatches(batches.size());
                processInfo.setStatus(org.jumpmind.symmetric.model.ProcessInfo.Status.OK);

                if (engine.getParameterService().is(ParameterConstants.REST_HEARTBEAT_ON_PULL)
                        && hostName != null) {
                    Heartbeat heartbeat = new Heartbeat();
                    heartbeat.setNodeId(nodeId);
                    heartbeat.setHeartbeatTime(new Date());
                    heartbeat.setHostName(hostName);
                    this.heartbeatImpl(engine, heartbeat);
                }
                return results;
            } finally {
                if (processInfo.getStatus() != org.jumpmind.symmetric.model.ProcessInfo.Status.OK) {
                    processInfo.setStatus(org.jumpmind.symmetric.model.ProcessInfo.Status.ERROR);
                }
            }
        } else {
            throw new NotAllowedException();
        }
    }

    /**
     * Sends a heartbeat to the server for the given node.
     * 
     * @param nodeID
     *            - Required - The client nodeId this to which this heartbeat
     *            belongs See {@link Heartbeat} for request body requirements
     */
    @ApiOperation(value = "Send a heartbeat for the single engine")
    @RequestMapping(value = "/engine/heartbeat", method = RequestMethod.PUT)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void putHeartbeat(
            @ApiParam(value="This the password for the nodeId being passed in.  The password is stored in the node_security table.")
            @RequestParam(value = WebConstants.SECURITY_TOKEN) String securityToken,
            @RequestBody Heartbeat heartbeat) {
        if (securityVerified(heartbeat.getNodeId(), getSymmetricEngine(), securityToken)) {
            putHeartbeat(getSymmetricEngine().getEngineName(), securityToken, heartbeat);
        } else {
            throw new NotAllowedException();
        }
    }

    /**
     * Sends a heartbeat to the server for the given node.
     * 
     * @param nodeID
     *            - Required - The client nodeId this to which this heartbeat
     *            belongs See {@link Heartbeat} for request body requirements
     */
    @ApiOperation(value = "Send a heartbeat for the specified engine")
    @RequestMapping(value = "/engine/{engine}/heartbeat", method = RequestMethod.PUT)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void putHeartbeat(@PathVariable("engine") String engineName,
            @ApiParam(value="This the password for the nodeId being passed in.  The password is stored in the node_security table.") 
            @RequestParam(value = WebConstants.SECURITY_TOKEN) String securityToken,
            @RequestBody Heartbeat heartbeat) {

        ISymmetricEngine engine = getSymmetricEngine(engineName);
        if (securityVerified(heartbeat.getNodeId(), engine, securityToken)) {
            heartbeatImpl(engine, heartbeat);
        } else {
            throw new NotAllowedException();
        }
    }

    private void heartbeatImpl(ISymmetricEngine engine, Heartbeat heartbeat) {
        INodeService nodeService = engine.getNodeService();

        NodeHost nodeHost = new NodeHost();
        if (heartbeat.getAvailableProcessors() != null) {
            nodeHost.setAvailableProcessors(heartbeat.getAvailableProcessors());
        }
        if (heartbeat.getCreateTime() != null) {
            nodeHost.setCreateTime(heartbeat.getCreateTime());
        }
        if (heartbeat.getFreeMemoryBytes() != null) {
            nodeHost.setFreeMemoryBytes(heartbeat.getFreeMemoryBytes());
        }
        if (heartbeat.getHeartbeatTime() != null) {
            nodeHost.setHeartbeatTime(heartbeat.getHeartbeatTime());
        }
        if (heartbeat.getHostName() != null) {
            nodeHost.setHostName(heartbeat.getHostName());
        }
        if (heartbeat.getIpAddress() != null) {
            nodeHost.setIpAddress(heartbeat.getIpAddress());
        }
        if (heartbeat.getJavaVendor() != null) {
            nodeHost.setJavaVendor(heartbeat.getJavaVendor());
        }
        if (heartbeat.getJdbcVersion() != null) {
            nodeHost.setJdbcVersion(heartbeat.getJdbcVersion());
        }
        if (heartbeat.getJavaVersion() != null) {
            nodeHost.setJavaVersion(heartbeat.getJavaVersion());
        }
        if (heartbeat.getLastRestartTime() != null) {
            nodeHost.setLastRestartTime(heartbeat.getLastRestartTime());
        }
        if (heartbeat.getMaxMemoryBytes() != null) {
            nodeHost.setMaxMemoryBytes(heartbeat.getMaxMemoryBytes());
        }
        if (heartbeat.getNodeId() != null) {
            nodeHost.setNodeId(heartbeat.getNodeId());
        }
        if (heartbeat.getOsArchitecture() != null) {
            nodeHost.setOsArch(heartbeat.getOsArchitecture());
        }
        if (heartbeat.getOsName() != null) {
            nodeHost.setOsName(heartbeat.getOsName());
        }
        if (heartbeat.getOsUser() != null) {
            nodeHost.setOsUser(heartbeat.getOsUser());
        }
        if (heartbeat.getOsVersion() != null) {
            nodeHost.setOsVersion(heartbeat.getOsVersion());
        }
        if (heartbeat.getSymmetricVersion() != null) {
            nodeHost.setSymmetricVersion(heartbeat.getSymmetricVersion());
        }
        if (heartbeat.getTimezoneOffset() != null) {
            nodeHost.setTimezoneOffset(heartbeat.getTimezoneOffset());
        }
        if (heartbeat.getTotalMemoryBytes() != null) {
            nodeHost.setTotalMemoryBytes(heartbeat.getTotalMemoryBytes());
        }

        nodeService.updateNodeHost(nodeHost);
    }

    /**
     * Acknowledges a set of batches that have been pulled and processed on the
     * client side. Setting the status to OK will render the batch complete.
     * Setting the status to anything other than OK will queue the batch on the
     * server to be sent again on the next pull. if the status is "ER". In error
     * status the status description should contain relevant information about
     * the error on the client including SQL Error Number and description
     */
    @ApiOperation(value = "Acknowledge a set of batches for the single engine")
    @RequestMapping(value = "/engine/acknowledgebatch", method = RequestMethod.PUT)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final BatchAckResults putAcknowledgeBatch(
            @ApiParam(value="This the password for the nodeId being passed in.  The password is stored in the node_security table.")
            @RequestParam(value = WebConstants.SECURITY_TOKEN) String securityToken,
            @RequestBody BatchResults batchResults) {
        BatchAckResults results = putAcknowledgeBatch(getSymmetricEngine().getEngineName(),
                securityToken, batchResults);
        return results;
    }

    @ApiOperation(value = "Acknowledge a set of batches for the specified engine")
    @RequestMapping(value = "/engine/{engine}/acknowledgebatch", method = RequestMethod.PUT)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final BatchAckResults putAcknowledgeBatch(@PathVariable("engine") String engineName,
            @ApiParam(value="This the password for the nodeId being passed in.  The password is stored in the node_security table.")
            @RequestParam(value = WebConstants.SECURITY_TOKEN) String securityToken,
            @RequestBody BatchResults batchResults) {

        BatchAckResults finalResult = new BatchAckResults();
        ISymmetricEngine engine = getSymmetricEngine(engineName);
        List<BatchAckResult> results = null;
        if (batchResults.getBatchResults().size() > 0) {
            if (securityVerified(batchResults.getNodeId(), engine, securityToken)) {
                IAcknowledgeService ackService = engine.getAcknowledgeService();
                List<BatchAck> batchAcks = convertBatchResultsToAck(batchResults);
                results = ackService.ack(batchAcks);
            } else {
                throw new NotAllowedException();
            }
        }
        finalResult.setBatchAckResults(results);
        return finalResult;
    }

    private List<BatchAck> convertBatchResultsToAck(BatchResults batchResults) {
        BatchAck batchAck = null;
        List<BatchAck> batchAcks = new ArrayList<BatchAck>();
        long transferTimeInMillis = batchResults.getTransferTimeInMillis();
        if (transferTimeInMillis > 0) {
            transferTimeInMillis = transferTimeInMillis / batchResults.getBatchResults().size();
        }
        for (BatchResult batchResult : batchResults.getBatchResults()) {
            batchAck = new BatchAck(batchResult.getBatchId());
            batchAck.setNodeId(batchResults.getNodeId());
            batchAck.setNetworkMillis(transferTimeInMillis);
            batchAck.setDatabaseMillis(batchResult.getLoadTimeInMillis());
            if (batchResult.getStatus().equalsIgnoreCase("OK")) {
                batchAck.setOk(true);
            } else {
                batchAck.setOk(false);
                batchAck.setSqlCode(batchResult.getSqlCode());
                batchAck.setSqlState(batchResult.getSqlState().substring(0,
                        Math.min(batchResult.getSqlState().length(), 10)));
                batchAck.setSqlMessage(batchResult.getStatusDescription());
            }
            batchAcks.add(batchAck);
        }
        return batchAcks;
    }

    /**
     * Requests an initial load from the server for the node id provided. The
     * initial load requst directs the server to queue up initial load data for
     * the client node. Data is obtained for the initial load by the client
     * calling the pull method.
     * 
     * @param nodeID
     */
    @ApiOperation(value = "Request an initial load for the specified node for the single engine")
    @RequestMapping(value = "/engine/requestinitialload", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postRequestInitialLoad(@RequestParam(value = "nodeId") String nodeId) {
        postRequestInitialLoad(getSymmetricEngine().getEngineName(), nodeId);
    }

    /**
     * Requests an initial load from the server for the node id provided. The
     * initial load requst directs the server to queue up initial load data for
     * the client node. Data is obtained for the initial load by the client
     * calling the pull method.
     * 
     * @param nodeID
     */
    @ApiOperation(value = "Request an initial load for the specified node for the specified engine")
    @RequestMapping(value = "/engine/{engine}/requestinitialload", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ResponseBody
    public final void postRequestInitialLoad(@PathVariable("engine") String engineName,
            @RequestParam(value = "nodeId") String nodeId) {

        ISymmetricEngine engine = getSymmetricEngine(engineName);
        INodeService nodeService = engine.getNodeService();
        nodeService.setInitialLoadEnabled(nodeId, true, false, -1, "restapi");

    }

    @ApiOperation(value = "Outgoing summary of batches and data counts waiting for a node")
    @RequestMapping(value = "/engine/outgoingBatchSummary", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final BatchSummaries getOutgoingBatchSummary(
            @RequestParam(value = WebConstants.NODE_ID) String nodeId,
            @ApiParam(value="This the password for the nodeId being passed in.  The password is stored in the node_security table.")
            @RequestParam(value = WebConstants.SECURITY_TOKEN) String securityToken) {
        return getOutgoingBatchSummary(getSymmetricEngine().getEngineName(), nodeId, securityToken);
    }

    @ApiOperation(value = "Outgoing summary of batches and data counts waiting for a node")
    @RequestMapping(value = "/engine/{engine}/outgoingBatchSummary", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final BatchSummaries getOutgoingBatchSummary(
            @PathVariable("engine") String engineName,
            @RequestParam(value = WebConstants.NODE_ID) String nodeId,
            @ApiParam(value="This the password for the nodeId being passed in.  The password is stored in the node_security table.")
            @RequestParam(value = WebConstants.SECURITY_TOKEN) String securityToken) {

        
        ISymmetricEngine engine = getSymmetricEngine(engineName);

        if (securityVerified(nodeId, engine, securityToken)) {
            BatchSummaries summaries = new BatchSummaries();
            summaries.setNodeId(nodeId);

            IOutgoingBatchService outgoingBatchService = engine.getOutgoingBatchService();
            List<OutgoingBatchSummary> list = outgoingBatchService.findOutgoingBatchSummary(
                    OutgoingBatch.Status.RQ, OutgoingBatch.Status.QY, OutgoingBatch.Status.NE,
                    OutgoingBatch.Status.SE, OutgoingBatch.Status.LD, OutgoingBatch.Status.ER);
            for (OutgoingBatchSummary sum : list) {
                if (sum.getNodeId().equals(nodeId)) {
                    BatchSummary summary = new BatchSummary();
                    summary.setBatchCount(sum.getBatchCount());
                    summary.setDataCount(sum.getDataCount());
                    summary.setOldestBatchCreateTime(sum.getOldestBatchCreateTime());
                    summary.setStatus(sum.getStatus().name());
                    summaries.getBatchSummaries().add(summary);
                }
            }
            
            return summaries;
        } else {
            throw new NotAllowedException();
        }
    }

    @ApiOperation(value = "Read parameter value")
    @RequestMapping(value = "engine/parameter/{name}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final String getParameter(@PathVariable("name") String name) {
        return getSymmetricEngine().getParameterService().getString(name.replace('_', '.'));
    }

    @ApiOperation(value = "Read paramater value for the specified engine")
    @RequestMapping(value = "engine/{engine}/parameter/{name}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public final String getParameter(@PathVariable("engine") String engineName, @PathVariable("name") String name) {
        return getSymmetricEngine(engineName).getParameterService().getString(name.replace('_', '.'));
    }

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
        engine.getParameterService().saveParameter(ParameterConstants.AUTO_START_ENGINE, "true", Constants.SYSTEM_USER);
        if (!engine.start()) {
            throw new InternalServerErrorException();
        }
    }

    private void stopImpl(ISymmetricEngine engine) {
        engine.stop();
        engine.getParameterService().saveParameter(ParameterConstants.AUTO_START_ENGINE, "false", Constants.SYSTEM_USER);

    }

    private void syncTriggersImpl(ISymmetricEngine engine, boolean force) {

        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        StringBuilder buffer = new StringBuilder();
        triggerRouterService.syncTriggers(buffer, force);
    }

    private void syncTriggersByTableImpl(ISymmetricEngine engine, String catalogName,
            String schemaName, String tableName, boolean force) {

        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        Table table = getSymmetricEngine().getDatabasePlatform().getTableFromCache(catalogName,
                schemaName, tableName, true);
        if (table == null) {
            throw new NotFoundException();
        }
        triggerRouterService.syncTriggers(table, force);
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

    private SendSchemaResponse sendSchemaImpl(ISymmetricEngine engine, SendSchemaRequest request) {

        IConfigurationService configurationService = engine.getConfigurationService();
        INodeService nodeService = engine.getNodeService();
        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        IDataService dataService = engine.getDataService();

        SendSchemaResponse response = new SendSchemaResponse();

        org.jumpmind.symmetric.model.Node identity = nodeService.findIdentity();
        if (identity != null) {
            List<org.jumpmind.symmetric.model.Node> nodesToSendTo = new ArrayList<org.jumpmind.symmetric.model.Node>();

            List<String> nodeIds = request.getNodeIdsToSendTo();
            if (nodeIds == null || nodeIds.size() == 0) {
                nodeIds = new ArrayList<String>();
                String nodeGroupIdToSendTo = request.getNodeGroupIdToSendTo();
                if (isNotBlank(nodeGroupIdToSendTo)) {
                    NodeGroupLink link = configurationService.getNodeGroupLinkFor(
                            identity.getNodeGroupId(), nodeGroupIdToSendTo, false);
                    if (link != null) {
                        Collection<org.jumpmind.symmetric.model.Node> nodes = nodeService
                                .findEnabledNodesFromNodeGroup(nodeGroupIdToSendTo);
                        nodesToSendTo.addAll(nodes);
                    } else {
                        log.warn("Could not send schema to all nodes in the '"
                                + nodeGroupIdToSendTo + "' node group.  No node group link exists");
                    }
                } else {
                    log.warn("Could not send schema to nodes.  There are none that were provided and the nodeGroupIdToSendTo was also not provided");
                }
            } else {
                for (String nodeIdToValidate : nodeIds) {
                    org.jumpmind.symmetric.model.Node node = nodeService.findNode(nodeIdToValidate);
                    if (node != null) {
                        NodeGroupLink link = configurationService.getNodeGroupLinkFor(
                                identity.getNodeGroupId(), node.getNodeGroupId(), false);
                        if (link != null) {
                            nodesToSendTo.add(node);
                        } else {
                            log.warn("Could not send schema to node '" + nodeIdToValidate
                                    + "'. No node group link exists");
                        }
                    } else {
                        log.warn("Could not send schema to node '" + nodeIdToValidate
                                + "'.  It was not present in the database");
                    }
                }
            }

            Map<String, List<TableName>> results = response.getNodeIdsSentTo();
            List<String> nodeIdsToSendTo = toNodeIds(nodesToSendTo);
            for (String nodeId : nodeIdsToSendTo) {
                results.put(nodeId, new ArrayList<TableName>());
            }

            if (nodesToSendTo.size() > 0) {
                List<TableName> tablesToSend = request.getTablesToSend();
                List<TriggerRouter> triggerRouters = triggerRouterService.getTriggerRouters(false);
                for (TriggerRouter triggerRouter : triggerRouters) {
                    Trigger trigger = triggerRouter.getTrigger();
                    NodeGroupLink link = triggerRouter.getRouter().getNodeGroupLink();
                    if (link.getSourceNodeGroupId().equals(identity.getNodeGroupId())) {
                        for (org.jumpmind.symmetric.model.Node node : nodesToSendTo) {
                            if (link.getTargetNodeGroupId().equals(node.getNodeGroupId())) {
                                if (tablesToSend == null || tablesToSend.size() == 0
                                        || contains(trigger, tablesToSend)) {
                                    dataService.sendSchema(node.getNodeId(),
                                            trigger.getSourceCatalogName(),
                                            trigger.getSourceSchemaName(),
                                            trigger.getSourceTableName(), false);
                                    results.get(node.getNodeId()).add(
                                            new TableName(trigger.getSourceCatalogName(), trigger
                                                    .getSourceSchemaName(), trigger
                                                    .getSourceTableName()));
                                }
                            }
                        }
                    }
                }
            }
        }
        return response;
    }

    private boolean contains(Trigger trigger, List<TableName> tables) {
        for (TableName tableName : tables) {
            if (trigger.getFullyQualifiedSourceTableName().equals(
                    Table.getFullyQualifiedTableName(tableName.getCatalogName(),
                            tableName.getSchemaName(), tableName.getTableName()))) {
                return true;
            }
        }
        return false;
    }

    private List<String> toNodeIds(List<org.jumpmind.symmetric.model.Node> nodes) {
        List<String> nodeIds = new ArrayList<String>(nodes.size());
        for (org.jumpmind.symmetric.model.Node node : nodes) {
            nodeIds.add(node.getNodeId());
        }
        return nodeIds;
    }

    private void uninstallImpl(ISymmetricEngine engine) {
        getSymmetricEngineHolder().uninstallEngine(engine);
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

                        List<NodeHost> nodeHosts = nodeService.findNodeHosts(child.getNode()
                                .getNodeId());
                        NodeSecurity nodeSecurity = nodeService.findNodeSecurity(child.getNode()
                                .getNodeId());

                        xmlChildNode = new Node();
                        xmlChildNode.setNodeId(child.getNode().getNodeId());
                        xmlChildNode.setExternalId(child.getNode().getExternalId());
                        xmlChildNode.setRegistrationServer(false);
                        xmlChildNode.setSyncUrl(child.getNode().getSyncUrl());

                        xmlChildNode.setBatchInErrorCount(child.getNode().getBatchInErrorCount());
                        xmlChildNode.setBatchToSendCount(child.getNode().getBatchToSendCount());
                        if (nodeHosts.size() > 0) {
                            xmlChildNode.setLastHeartbeat(nodeHosts.get(0).getHeartbeatTime());
                        }
                        xmlChildNode.setRegistered(nodeSecurity.hasRegistered());
                        xmlChildNode.setInitialLoaded(nodeSecurity.hasInitialLoaded());
                        xmlChildNode
                                .setReverseInitialLoaded(nodeSecurity.hasReverseInitialLoaded());
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
            xmlNode.setHeartbeatInterval(engine.getParameterService().getInt(
                    ParameterConstants.HEARTBEAT_JOB_PERIOD_MS));
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
        if (modelNode.getCreatedAtNodeId() == null
                || modelNode.getCreatedAtNodeId().equalsIgnoreCase(modelNode.getExternalId())) {
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
            status.setBatchToSendCount(engine.getOutgoingBatchService()
                    .countOutgoingBatchesUnsent());
            status.setBatchInErrorCount(engine.getOutgoingBatchService()
                    .countOutgoingBatchesInError());
            status.setDeploymentType(modelNode.getDeploymentType());
            if (modelNode.getCreatedAtNodeId() == null) {
                status.setRegistrationServer(true);
            } else {
                status.setRegistrationServer(false);
            }
            if (nodeHost != null && nodeHost.size() > 0) {
                status.setLastHeartbeat(nodeHost.get(0).getHeartbeatTime());
            }
            status.setHeartbeatInterval(engine.getParameterService().getInt(
                    ParameterConstants.HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC));
            if (status.getHeartbeatInterval() == 0) {
                status.setHeartbeatInterval(600);
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
            channelStatus.add(status);
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
            int nbrRows = 0;
            for (Row row : rows) {
                xmlRow = new org.jumpmind.symmetric.web.rest.model.Row();
                Iterator<Map.Entry<String, Object>> itr = row.entrySet().iterator();
                int columnOrdinal = 0;
                while (itr.hasNext()) {
                    xmlColumn = new org.jumpmind.symmetric.web.rest.model.Column();
                    xmlColumn.setOrdinal(++columnOrdinal);
                    Map.Entry<String, Object> pair = (Map.Entry<String, Object>) itr.next();
                    xmlColumn.setName(pair.getKey());
                    if (pair.getValue() != null) {
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
            throw new NotAllowedException("Error while executing sql %s.  Error is %s", sql, ex
                    .getCause().getMessage());
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
            MDC.put("engineName", engine.getEngineName());
            return engine;
        }
    }

    protected boolean securityVerified(String nodeId, ISymmetricEngine engine, String securityToken) {

        INodeService nodeService = engine.getNodeService();
        boolean allowed = false;
        org.jumpmind.symmetric.model.Node targetNode = nodeService.findNode(nodeId);
        if (targetNode != null) {
            NodeSecurity security = nodeService.findNodeSecurity(nodeId);
            allowed = security.getNodePassword().equals(securityToken);
        }
        return allowed;
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
