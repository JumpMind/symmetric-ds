package org.jumpmind.symmetric.ext.tray;

import java.awt.AWTException;
import java.awt.Image;
import java.awt.SystemTray;
import java.awt.Toolkit;
import java.awt.TrayIcon;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.IAcknowledgeEventListener;

public class SystemTrayStatusListener implements IExtractorFilter, IDataLoaderFilter,
        IBatchListener, IAcknowledgeEventListener {

    private final ILog logger = LogFactory.getLog(getClass());

    private boolean enabled = true;
    private TrayIcon trayIcon = null;
    private IParameterService parameterService;

    private Image AT_REST;
    private Image WORKING;
    private Image ERROR;

    private Image currentImage;


    protected void init() {
        if (SystemTray.isSupported()) {
            try {
                SystemTray tray = SystemTray.getSystemTray();
                AT_REST = Toolkit.getDefaultToolkit().getImage(getClass().getResource("symmetric-ds-blank-16x16.gif"));
                WORKING = Toolkit.getDefaultToolkit().getImage(getClass().getResource("symmetric-ds-work-16x16.gif"));
                ERROR = Toolkit.getDefaultToolkit().getImage(getClass().getResource("symmetric-ds-stop-16x16.gif"));

                trayIcon = new TrayIcon(AT_REST, parameterService.getString(ParameterConstants.ENGINE_NAME));
                tray.add(trayIcon);
                setTrayIcon(AT_REST);
            } catch (AWTException e) {
                logger.error(e);
            }
        }
    }

    protected void setTrayIcon(Image image) {
        if (currentImage == null
                || (!image.equals(currentImage) && !(currentImage.equals(ERROR) && image.equals(AT_REST)))) {
            currentImage = image;
            trayIcon.setImage(image);
        }
    }

    @Override
    public void onAcknowledgeEvent(BatchInfo batchInfo) {
        if (batchInfo.isOk()) {
            setTrayIcon(AT_REST);
        } else {
            setTrayIcon(ERROR);
        }

    }

    @Override
    public boolean filterData(Data data, String routerId, DataExtractorContext ctx) {
        setTrayIcon(WORKING);
        return true;
    }

    @Override
    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        setTrayIcon(WORKING);
        return true;
    }

    @Override
    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        setTrayIcon(WORKING);
        return true;
    }

    @Override
    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        setTrayIcon(WORKING);
        return true;
    }

    @Override
    public void batchCommitted(IDataLoader loader, IncomingBatch batch) {
        setTrayIcon(AT_REST);
    }

    @Override
    public void batchComplete(IDataLoader loader, IncomingBatch batch) {
        setTrayIcon(AT_REST);
    }

    @Override
    public void batchRolledback(IDataLoader loader, IncomingBatch batch) {
        setTrayIcon(ERROR);
    }

    @Override
    public void earlyCommit(IDataLoader loader, IncomingBatch batch) {
    }

    @Override
    public boolean isAutoRegister() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

}
