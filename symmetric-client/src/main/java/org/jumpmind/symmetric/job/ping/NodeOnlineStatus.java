package org.jumpmind.symmetric.job.ping;

import java.io.Serializable;
import java.util.Date;

public class NodeOnlineStatus implements Serializable {
    public enum PossibleStatus { Unknown, Online, Offline, UnknownHost}

    private PossibleStatus Status;
    private Date WentOnline;

    public NodeOnlineStatus(PossibleStatus status) {
        this.Status = status;
    }

    public PossibleStatus getStatus() {
        return Status;
    }

    public void setStatus(PossibleStatus status) {
        Status = status;
    }

    public Date getWentOnline() {
        return WentOnline;
    }

    public void setWentOnline(Date wentOnline) {
        WentOnline = wentOnline;
    }
}
