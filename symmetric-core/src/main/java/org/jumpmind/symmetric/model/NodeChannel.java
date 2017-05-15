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
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * A composite parent for {@link Channel} and {@link NodeChannelControl}
 */
public class NodeChannel implements Serializable {

    private static final long serialVersionUID = 1L;

    private Channel channel;
    
    private NodeChannelControl nodeChannelControl;

    public NodeChannel() {
        this (new Channel());
    }
    
    public NodeChannel(Channel channel) {
        this.channel = channel;
        nodeChannelControl = new NodeChannelControl();
        nodeChannelControl.setChannelId(channel.getChannelId());
    }

    public NodeChannel(String channelId) {
        channel = new Channel();
        nodeChannelControl = new NodeChannelControl();
        nodeChannelControl.setChannelId(channelId);
        channel.setChannelId(channelId);
    }

    public String getChannelId() {
        return channel.getChannelId();
    }
    
    public void setDataEventActionShortName(String dataEventAction) {
        setDataEventAction(NodeGroupLinkAction.fromShortName(dataEventAction));
    }
    
    public String getDataEventActionShortName() {
        return channel.getDataEventAction() == null ? "" : channel.getDataEventAction().getShortName();
    }

    public void setDataEventAction(NodeGroupLinkAction dataEventAction) {
        channel.setDataEventAction(dataEventAction);
    }
    
    public NodeGroupLinkAction getDataEventAction() {
        return channel.getDataEventAction();
    }

    public int getMaxBatchSize() {
        return channel.getMaxBatchSize();
    }

    public void setMaxBatchSize(int maxBatchSize) {
        channel.setMaxBatchSize(maxBatchSize);
    }

    public int getMaxBatchToSend() {
        return channel.getMaxBatchToSend();
    }

    public void setMaxBatchToSend(int maxBatchToSend) {
        channel.setMaxBatchToSend(maxBatchToSend);
    }

    public void setMaxDataToRoute(int maxDataToRoute) {
        channel.setMaxDataToRoute(maxDataToRoute);
    }

    public int getMaxDataToRoute() {
        return channel.getMaxDataToRoute();
    }

    public void setUseOldDataToRoute(boolean useOldDataToRoute) {
        channel.setUseOldDataToRoute(useOldDataToRoute);
    }

    public boolean isUseOldDataToRoute() {
        return channel.isUseOldDataToRoute();
    }

    public void setUseRowDataToRoute(boolean useRowDataToRoute) {
        channel.setUseRowDataToRoute(useRowDataToRoute);
    }

    public boolean isUseRowDataToRoute() {
        return channel.isUseRowDataToRoute();
    }

    public void setUsePkDataToRoute(boolean usePkDataToRoute) {
        channel.setUsePkDataToRoute(usePkDataToRoute);
    }

    public boolean isUsePkDataToRoute() {
        return channel.isUsePkDataToRoute();
    }

    public int getProcessingOrder() {
        return channel.getProcessingOrder();
    }

    public String getBatchAlgorithm() {
        return channel.getBatchAlgorithm();
    }

    public void setBatchAlgorithm(String batchAlgorithm) {
        channel.setBatchAlgorithm(batchAlgorithm);
    }

    public void setEnabled(boolean enabled) {
        channel.setEnabled(enabled);
    }

    public boolean isEnabled() {
        return channel.isEnabled();
    }

    public boolean isSuspendEnabled() {
        return nodeChannelControl.isSuspendEnabled();
    }

    public boolean isIgnoreEnabled() {
        return nodeChannelControl.isIgnoreEnabled();
    }

    public String getNodeId() {
        return nodeChannelControl.getNodeId();
    }

    public void setNodeId(String nodeId) {
        nodeChannelControl.setNodeId(nodeId);
    }

    public void setLastExtractTime(Date lastExtractedTime) {
        nodeChannelControl.setLastExtractTime(lastExtractedTime);
    }

    public Date getLastExtractTime() {
        return nodeChannelControl.getLastExtractTime();
    }

    public void setIgnoreEnabled(boolean ignored) {
        nodeChannelControl.setIgnoreEnabled(ignored);
    }

    public void setProcessingOrder(int priority) {
        channel.setProcessingOrder(priority);
    }

    public void setChannelId(String id) {
        channel.setChannelId(id);
        nodeChannelControl.setChannelId(id);
    }
    
    public void setLastUpdateTime(Date date) {
        channel.setLastUpdateTime(date);
    }
    
    public void setCreateTime(Date date) {
        channel.setCreateTime(date);
    }
    
    public void setLastUpdateBy(String lastUpdateBy) {
        channel.setLastUpdateBy(lastUpdateBy);
    }

    public void setSuspendEnabled(boolean suspended) {
        nodeChannelControl.setSuspendEnabled(suspended);
    }

    public Channel getChannel() {
        return channel;
    }

    public NodeChannelControl getNodeChannelControl() {
        return nodeChannelControl;
    }

    public long getExtractPeriodMillis() {
        return channel.getExtractPeriodMillis();
    }

    public void setExtractPeriodMillis(long extractPeriodMillis) {
        channel.setExtractPeriodMillis(extractPeriodMillis);
    }

    public void setContainsBigLob(boolean containsBigLobs) {
        this.channel.setContainsBigLob(containsBigLobs);
    }

    public boolean isContainsBigLob() {
        return this.channel.isContainsBigLob();
    }
    
    public void setDataLoaderType(String type) {
        channel.setDataLoaderType(type);
    }
    
    public String getDataLoaderType() {
        return channel.getDataLoaderType();
    }
    
    public void setReloadFlag(boolean value) {
        this.channel.setReloadFlag(value);
    }
    
    public boolean isReloadFlag() {
        return this.channel.isReloadFlag();
    }
    
    public void setFileSyncFlag(boolean value) {
        this.channel.setFileSyncFlag(value);
    }
    
    public boolean isFileSyncFlag() {
        return this.channel.isFileSyncFlag();
    }
    
    public Date getCreateTime() {
        return this.channel.getCreateTime();
    }
    
    public String getLastUpdateBy() {
        return this.channel.getLastUpdateBy();
    }
    
    public Date getLastUpdateTime() {
        return this.channel.getLastUpdateTime();
    }
    
    public void setQueue(String queue) {
    	this.channel.setQueue(queue);
    }
    
    public String getQueue() {
    	return this.channel.getQueue();
    }
    
    public BigDecimal getMaxKBytesPerSecond() {
        return this.channel.getMaxKBytesPerSecond();
    }

    public void setMaxKBytesPerSecond(BigDecimal maxKBytesPerSecond) {
        this.channel.setMaxKBytesPerSecond(maxKBytesPerSecond);
    }
    
    @Override
    public String toString() {
        return "Channel: '" + getChannelId() + "' Node: '" + getNodeId() + "' " +  super.toString();
    }

}