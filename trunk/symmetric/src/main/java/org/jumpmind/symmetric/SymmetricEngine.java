package org.jumpmind.symmetric;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.PropertiesConstants;
import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.impl.BootstrapService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * This is the preferred way to create, configure, start and manage an instance of
 * symmetric.
 * 
 * @author chenson
 */
public class SymmetricEngine {

    protected static final Log logger = LogFactory
            .getLog(SymmetricEngine.class);

    private ApplicationContext applicationContext;

    private BootstrapService bootstrapService;

    private IRuntimeConfig runtimeConfig;

    private INodeService nodeService;

    private IRegistrationService registrationService;

    private IPurgeService purgeService;

    private boolean started = false;

    Properties properties;

    private static Map<String, SymmetricEngine> registeredEnginesByUrl = new HashMap<String, SymmetricEngine>();

    public SymmetricEngine(String overridePropertiesResource1,
            String overridePropertiesResource2) {
        // Setting system properties is probably not the best way to accomplish this setup.
        // Synchronizing on the class so creating multiple engines is thread safe.
        synchronized (SymmetricEngine.class) {
            System.setProperty("symmetric.override.properties.file.1",
                    overridePropertiesResource1);
            System.setProperty("symmetric.override.properties.file.2",
                    overridePropertiesResource2);
            this.init(createContext());
        }
    }

    public SymmetricEngine() {
        init(createContext());
    }

    protected SymmetricEngine(ApplicationContext ctx) {
        init(ctx);
    }

    private ApplicationContext createContext() {
        return new ClassPathXmlApplicationContext("classpath:/symmetric.xml");
    }

    private void init(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        properties = (Properties) applicationContext
                .getBean(Constants.PROPERTIES);
        bootstrapService = (BootstrapService) applicationContext
                .getBean(Constants.BOOTSTRAP_SERVICE);
        runtimeConfig = (IRuntimeConfig) applicationContext
                .getBean(Constants.RUNTIME_CONFIG);
        nodeService = (INodeService) applicationContext
                .getBean(Constants.NODE_SERVICE);
        registrationService = (IRegistrationService) applicationContext
                .getBean(Constants.REGISTRATION_SERVICE);
        purgeService = (IPurgeService) applicationContext
                .getBean(Constants.PURGE_SERVICE);
        registerEngine();
    }

    /**
     * Register this instance of the engine so it can be found by other processes in the JVM.
     */
    private void registerEngine() {
        registeredEnginesByUrl.put(runtimeConfig.getMyUrl(), this);
    }

    private void initDb() {
        bootstrapService.init();
    }

    private void startJobs() {
        if (Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PUSH_JOB))) {
            applicationContext.getBean(Constants.PUSH_JOB_TIMER);
        }
        if (Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PULL_JOB))) {
            applicationContext.getBean(Constants.PULL_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PURGE_JOB))) {
            applicationContext.getBean(Constants.PURGE_JOB_TIMER);
        }

        if (Boolean.TRUE
                .toString()
                .equalsIgnoreCase(
                        properties
                                .getProperty(PropertiesConstants.START_HEARTBEAT_JOB))) {
            applicationContext.getBean(Constants.HEARTBEAT_JOB_TIMER);
        }

        if (Boolean.TRUE
                .toString()
                .equalsIgnoreCase(
                        properties
                                .getProperty(PropertiesConstants.START_SYNCTRIGGERS_JOB))) {
            applicationContext.getBean(Constants.SYNC_TRIGGERS_JOB_TIMER);
        }
    }

    public synchronized void start() {
        if (!started) {
            initDb();
            bootstrapService.register();
            bootstrapService.syncTriggers();
            startJobs();
            started = true;
        }
    }

    public void push() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PUSH_JOB))) {
            ((IPushService) applicationContext.getBean(Constants.PUSH_SERVICE))
                    .pushData();
        } else {
            throw new UnsupportedOperationException(
                    "Cannot actuate a push if it is already scheduled.");
        }
    }

    public void syncTriggers() {
        bootstrapService.syncTriggers();
    }

    public void pull() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PULL_JOB))) {
            ((IPullService) applicationContext.getBean(Constants.PULL_SERVICE))
                    .pullData();
        } else {
            throw new UnsupportedOperationException(
                    "Cannot actuate a push if it is already scheduled.");
        }
    }

    public void purge() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PURGE_JOB))) {
            purgeService.purge();
        } else {
            throw new UnsupportedOperationException(
                    "Cannot actuate a purge if it is already scheduled.");
        }
    }

    public void heartbeat() {
        bootstrapService.heartbeat();
    }

    public void openRegistration(String domainName, String domainId) {
        registrationService.openRegistration(domainName, domainId);
    }

    public boolean isRegistered() {
        return nodeService.findIdentity() != null;
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public static SymmetricEngine findEngineByUrl(String url) {
        return registeredEnginesByUrl.get(url);
    }

}
