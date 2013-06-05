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
 * under the License.  */


package org.jumpmind.symmetric.transport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.io.IoConstants;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.web.WebConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractTransportManager {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected Map<String, ISyncUrlExtension> extensionSyncUrlHandlers  = new HashMap<String, ISyncUrlExtension>();

    public AbstractTransportManager() {
    }
    
    public void addExtensionSyncUrlHandler(String name, ISyncUrlExtension handler) {
        if (extensionSyncUrlHandlers == null) {
            extensionSyncUrlHandlers = new HashMap<String, ISyncUrlExtension>();
        }
        if (extensionSyncUrlHandlers.containsKey(name)) {
            log.warn("Overriding an existing '{}' extension sync url handler with a second one.", name);
        }
        extensionSyncUrlHandlers.put(name, handler);
    }

    /**
     * Build the url for remote node communication. Use the remote sync_url
     * first, if it is null or blank, then use the registration url instead.
     */
    public String resolveURL(String syncUrl, String registrationUrl) {
        if (StringUtils.isBlank(syncUrl) || syncUrl.startsWith(Constants.PROTOCOL_NONE)) {
            log.debug("Using the registration URL to contact the remote node because the syncURL for the node is blank");
            return registrationUrl;
        } else if (syncUrl.startsWith(Constants.PROTOCOL_EXT)) {
            try {
                URI uri = new URI(syncUrl);
                ISyncUrlExtension handler = extensionSyncUrlHandlers.get(uri.getHost());
                if (handler == null) {
                    log.error("Could not find a registered extension sync url handler with the name of {} using the url {}", uri.getHost(), syncUrl);
                    return syncUrl;
                } else {
                    return handler.resolveUrl(uri);
                }
            } catch (URISyntaxException e) {
                log.error(e.getMessage(),e);
                return syncUrl;
            }
        } else {
            return syncUrl;
        }
    }

    protected String getAcknowledgementData(String nodeId, List<IncomingBatch> list) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (IncomingBatch batch : list) {
            long batchId = batch.getBatchId();
            Object value = null;
            if (batch.getStatus() == Status.OK) {
                value = WebConstants.ACK_BATCH_OK;
            } else {
                value = batch.getFailedRowNumber();
            }
            append(builder, WebConstants.ACK_BATCH_NAME + batch.getBatchId(), value);            
            append(builder, WebConstants.ACK_NODE_ID + batchId, nodeId);
            append(builder, WebConstants.ACK_NETWORK_MILLIS + batchId, batch.getNetworkMillis());
            append(builder, WebConstants.ACK_FILTER_MILLIS + batchId, batch.getFilterMillis());
            append(builder, WebConstants.ACK_DATABASE_MILLIS + batchId, batch.getDatabaseMillis());
            append(builder, WebConstants.ACK_BYTE_COUNT + batchId, batch.getByteCount());
            
            if (batch.getIgnoreCount() > 0) {
              append(builder, WebConstants.ACK_IGNORE_COUNT + batchId, batch.getIgnoreCount());
            }

            if (batch.getStatus() == Status.ER) {
                append(builder, WebConstants.ACK_SQL_STATE + batchId, batch.getSqlState());
                append(builder, WebConstants.ACK_SQL_CODE + batchId, batch.getSqlCode());
                append(builder, WebConstants.ACK_SQL_MESSAGE + batchId, batch.getSqlMessage());
            }
        }
        return builder.toString();
    }

    protected static void append(StringBuilder builder, String name, Object value) {
        try {
            int len = builder.length();
            if (len > 0 && builder.charAt(len - 1) != '?') {
                builder.append("&");
            }
            if (value == null) {
                value = "";
            }
            builder.append(name).append("=")
                    .append(URLEncoder.encode(value.toString(), IoConstants.ENCODING));
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public List<BatchAck> readAcknowledgement(String parameterString1, String parameterString2) throws IOException {
        return readAcknowledgement(parameterString1 + "&" + parameterString2);
    }

    public List<BatchAck> readAcknowledgement(String parameterString) throws IOException {
        Map<String, Object> parameters = getParametersFromQueryUrl(parameterString.replace("\n", ""));
        return readAcknowledgement(parameters);
    }

    public static List<BatchAck> readAcknowledgement(Map<String, ? extends Object> parameters) {
        List<BatchAck> batches = new ArrayList<BatchAck>();
        for (String parameterName : parameters.keySet()) {
            if (parameterName.startsWith(WebConstants.ACK_BATCH_NAME)) {
                long batchId = NumberUtils.toLong(parameterName.substring(WebConstants.ACK_BATCH_NAME.length()));
                BatchAck batchInfo = getBatchInfo(parameters, batchId);
                batches.add(batchInfo);
            }
        }
        return batches;
    }

    private static BatchAck getBatchInfo(Map<String, ? extends Object> parameters, long batchId) {
        BatchAck batchInfo = new BatchAck(batchId);
        batchInfo.setNodeId(getParam(parameters, WebConstants.ACK_NODE_ID + batchId));
        batchInfo.setNetworkMillis(getParamAsNum(parameters, WebConstants.ACK_NETWORK_MILLIS + batchId));
        batchInfo.setFilterMillis(getParamAsNum(parameters, WebConstants.ACK_FILTER_MILLIS + batchId));
        batchInfo.setDatabaseMillis(getParamAsNum(parameters, WebConstants.ACK_DATABASE_MILLIS + batchId));
        batchInfo.setByteCount(getParamAsNum(parameters, WebConstants.ACK_BYTE_COUNT + batchId));
        batchInfo.setIgnored(getParamAsBoolean(parameters, WebConstants.ACK_IGNORE_COUNT + batchId));
        String status = getParam(parameters, WebConstants.ACK_BATCH_NAME + batchId, "").trim();
        batchInfo.setOk(status.equalsIgnoreCase(WebConstants.ACK_BATCH_OK));

        if (!batchInfo.isOk()) {
            batchInfo.setErrorLine(NumberUtils.toLong(status));
            batchInfo.setSqlState(getParam(parameters, WebConstants.ACK_SQL_STATE + batchId));
            batchInfo.setSqlCode((int) getParamAsNum(parameters, WebConstants.ACK_SQL_CODE + batchId));
            batchInfo.setSqlMessage(getParam(parameters, WebConstants.ACK_SQL_MESSAGE + batchId));
        }
        return batchInfo;
    }

    protected static Map<String, Object> getParametersFromQueryUrl(String parameterString) throws IOException {
        Map<String, Object> parameters = new HashMap<String, Object>();
        String[] tokens = parameterString.split("&");
        for (String param : tokens) {
            String[] nameValuePair = param.split("=");
            if (nameValuePair.length == 2) {
                parameters.put(nameValuePair[0], URLDecoder.decode(nameValuePair[1], IoConstants.ENCODING));
            }
        }
        return parameters;
    }

    private static long getParamAsNum(Map<String, ? extends  Object> parameters, String parameterName) {
        return NumberUtils.toLong(getParam(parameters, parameterName));
    }
    
    private static boolean getParamAsBoolean(Map<String, ? extends  Object> parameters, String parameterName) {
        return getParamAsNum(parameters, parameterName) > 0;
    }    

    private static String getParam(Map<String, ? extends  Object> parameters, String parameterName, String defaultValue) {
        String value = getParam(parameters, parameterName);
        return value == null ? defaultValue : value;
    }

    private static String getParam(Map<String,  ? extends Object> parameters, String parameterName) {
        Object value = parameters.get(parameterName);
        if (value instanceof String[]) {
            String[] arrayValue = (String[]) value;
            if (arrayValue.length > 0) {
                value = StringUtils.trim(arrayValue[0]);                
            }
        }
        return (String) value;
    }

}
