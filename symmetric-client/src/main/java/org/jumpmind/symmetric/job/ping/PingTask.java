package org.jumpmind.symmetric.job.ping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

public class PingTask implements Callable<PingResult> {

    final Logger log = LoggerFactory.getLogger(getClass());
    private String nodeId;
    private String hostname;

    public PingTask(String nodeId, String hostname) {
        this.hostname = hostname;
        this.nodeId = nodeId;
    }

    @Override
    public PingResult call() {
        InetAddress inet = null;
        try {
            inet = InetAddress.getByName(hostname);
            if(! inet.isReachable(3000)) {
                return new PingResult(nodeId, hostname, NodeOnlineStatus.PossibleStatus.Offline);
            }
            // ccnsider expired dns entries
            byte[] rawIpAddress = inet.getAddress();
            InetAddress inet2 =  InetAddress.getByAddress(rawIpAddress);
            if(inet2.isReachable(3000)) {
                String newHostName = inet2.getHostName();
                if(newHostName.equalsIgnoreCase(hostname)
                        || newHostName.toUpperCase().startsWith(hostname.toUpperCase() + ".")) { // consider possible domain name
                    return new PingResult(nodeId, hostname, NodeOnlineStatus.PossibleStatus.Online);
                }
            }
            return new PingResult(nodeId, hostname, NodeOnlineStatus.PossibleStatus.Offline);
        } catch (UnknownHostException uh) {
            return new PingResult(nodeId, hostname, NodeOnlineStatus.PossibleStatus.UnknownHost);
        }
        catch (Exception e) {
            log.error("Exception for PingTask:", e);
            return new PingResult(nodeId, hostname, NodeOnlineStatus.PossibleStatus.Unknown);
        }
    }
}