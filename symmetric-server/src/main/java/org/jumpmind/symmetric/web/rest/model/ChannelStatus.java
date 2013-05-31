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
package org.jumpmind.symmetric.web.rest.model;

public class ChannelStatus {

    /**
     * The ID or name of the channel. (e.g., 'employee')
     */
	String channelId;

	/**
	 * Is the channel enabled. Disabling the channel prevents all communication on the channel.
	 */
	boolean enabled;

	/**
	 * An outgoing batch is in error.
	 */
	boolean outgoingError;

	/**
	 * An incoming batch is in error.
	 */
	boolean incomingError;

	/**
	 * The number of batches waiting to be sent.
	 */
	private int batchToSendCount;

	/**
	 * The number of batches in the error state.
	 */
	private int batchInErrorCount;
	
	private boolean ignoreEnabled;
	
	private boolean suspendEnabled;

	/**
	 * @return The ID or name of the channel. (e.g., 'employee')
	 */
	public String getChannelId() {
		return channelId;
	}

	/**
	 * @param channelId The ID or name of the channel. (e.g., 'employee')
	 */
	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

	/**
	 * @return Is the channel enabled. Disabling the channel prevents all communication on the channel.
	 */
	public boolean isEnabled() {
		return enabled;
	}

	/**
	 * @param enabled Is the channel enabled. Disabling the channel prevents all communication on the channel.
	 */
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

    /**
     * @return An outgoing batch is in error.
     */
	public boolean isOutgoingError() {
		return outgoingError;
	}

    /**
     * @param enabled An outgoing batch is in error.
     */
	public void setOutgoingError(boolean outgoingError) {
		this.outgoingError = outgoingError;
	}

    /**
     * @return An incoming batch is in error.
     */
	public boolean isIncomingError() {
		return incomingError;
	}

    /**
     * @param enabled An incoming batch is in error.
     */
	public void setIncomingError(boolean incomingError) {
		this.incomingError = incomingError;
	}

    /**
     * @return The number of batches waiting to be sent.
     */
	public int getBatchToSendCount() {
		return batchToSendCount;
	}

    /**
     * @param enabled The number of batches waiting to be sent.
     */
	public void setBatchToSendCount(int batchToSendCount) {
		this.batchToSendCount = batchToSendCount;
	}

    /**
     * @return The number of batches in the error state.
     */
	public int getBatchInErrorCount() {
		return batchInErrorCount;
	}

    /**
     * @param enabled The number of batches in the error state.
     */
	public void setBatchInErrorCount(int batchInErrorCount) {
		this.batchInErrorCount = batchInErrorCount;
	}
	
	public void setIgnoreEnabled(boolean ignoreEnabled) {
        this.ignoreEnabled = ignoreEnabled;
    }
	
	public boolean isIgnoreEnabled() {
        return ignoreEnabled;
    }
	
	public void setSuspendEnabled(boolean suspendEnabled) {
        this.suspendEnabled = suspendEnabled;
    }
	
	public boolean isSuspendEnabled() {
        return suspendEnabled;
    }

}
