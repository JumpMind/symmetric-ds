package org.jumpmind.symmetric.web.rest;

public class ChannelStatus {

        String channelId;
        
        boolean enabled;
        
        boolean outgoingError;
        
        boolean incomingError;
        
    private int batchToSendCount;
    
    private int batchInErrorCount;

        public String getChannelId() {
                return channelId;
        }

        public void setChannelId(String channelId) {
                this.channelId = channelId;
        }

        public boolean isEnabled() {
                return enabled;
        }

        public void setEnabled(boolean enabled) {
                this.enabled = enabled;
        }

        public boolean isOutgoingError() {
                return outgoingError;
        }

        public void setOutgoingError(boolean outgoingError) {
                this.outgoingError = outgoingError;
        }

        public boolean isIncomingError() {
                return incomingError;
        }

        public void setIncomingError(boolean incomingError) {
                this.incomingError = incomingError;
        }

        public int getBatchToSendCount() {
                return batchToSendCount;
        }

        public void setBatchToSendCount(int batchToSendCount) {
                this.batchToSendCount = batchToSendCount;
        }

        public int getBatchInErrorCount() {
                return batchInErrorCount;
        }

        public void setBatchInErrorCount(int batchInErrorCount) {
                this.batchInErrorCount = batchInErrorCount;
        }
        
}
