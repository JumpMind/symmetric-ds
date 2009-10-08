package org.jumpmind.symmetric.ext.tray;

import java.awt.AWTException;
import java.awt.Image;
import java.awt.SystemTray;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import java.util.Set;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IParameterService;

public class SystemTrayStatusListener implements IHeartbeatListener {

    private final ILog logger = LogFactory.getLog(getClass());

    private long timeBetweenHeartbeatsInSeconds = 5;
    private boolean enabled = true;
    private TrayIcon trayIcon = null;
    private IParameterService parameterService;

    private Image AT_REST;
    private Image WORKING;
    private Image ERROR;

    @Override
    public long getTimeBetweenHeartbeatsInSeconds() {
        return this.timeBetweenHeartbeatsInSeconds;
    }

    protected void init() {
        if (SystemTray.isSupported()) {
            try {
                SystemTray tray = SystemTray.getSystemTray();
                AT_REST = Toolkit.getDefaultToolkit().getImage(getClass().getResource("symmetric-ds-blank-16x16.gif"));
                WORKING = Toolkit.getDefaultToolkit().getImage(getClass().getResource("symmetric-ds-work-16x16.gif"));
                ERROR = Toolkit.getDefaultToolkit().getImage(getClass().getResource("symmetric-ds-stop-16x16.gif"));

                trayIcon = new TrayIcon(AT_REST, parameterService.getString(ParameterConstants.ENGINE_NAME));
                tray.add(trayIcon);
            } catch (AWTException e) {
                logger.error(e);
            }
        }

    }

    @Override
    public void heartbeat(Node me, Set<Node> children) {
        if (trayIcon != null) {
            if (me.getBatchInErrorCount() > 0) {
                trayIcon.setImage(ERROR);
            } else if (me.getBatchToSendCount() > 0) {
                trayIcon.setImage(WORKING);
            } else {
                trayIcon.setImage(AT_REST);
            }
        }
    }

    @Override
    public boolean isAutoRegister() {
        return enabled;
    }

    public void setTimeBetweenHeartbeatsInSeconds(long timeBetweenHeartbeatsInSeconds) {
        this.timeBetweenHeartbeatsInSeconds = timeBetweenHeartbeatsInSeconds;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

}
