import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import groovy.util.ConfigSlurper;

import org.codehaus.groovy.grails.commons.ConfigurationHolder;
import org.jumpmind.symmetric.SpringWireableSymmetricEngine;
import grails.util.GrailsUtil;
import groovy.lang.GroovyClassLoader;

import java.io.File;

class SymmetricdsGrailsPlugin {
    // the plugin version
    def version = "0.1"
    // the version or versions of Grails the plugin is designed for
    def grailsVersion = "1.2-M3 > *"
    // the other plugins this plugin depends on
    def dependsOn = [:]
    // resources that are excluded from plugin packaging
    def pluginExcludes = [
            "grails-app/views/error.gsp"
    ]

    // TODO Fill in these fields
    def author = "Chris Henson"
    def authorEmail = "chenson42@sourceforge.users.net"
    def title = "Plugin for SymmetricDS data synchronization software"
    def description = '''\\
Brief description of the plugin.
'''

    // URL to the plugin's documentation
    def documentation = "http://grails.org/plugin/symmetricds"

    def doWithWebDescriptor = { webXml ->
        def filters = webXml.'filter'
        def lastFilter = filters[filters.size()-1]
        lastFilter + {
            'filter' {
                'filter-name'("symmetricfilter")
                'filter-class'(org.jumpmind.symmetric.web.SymmetricFilter.getName())
            }
        }
        
        def filterMappings = webXml.'filter-mapping'
        def lastFilterMapping = filterMappings[filterMappings.size()-1]
        lastFilterMapping + {
            'filter-mapping' {
                'filter-name'("symmetricfilter")
                'url-pattern'("/sync/*")
            }
        }    
        
        def servlets = webXml.'servlet'
        def lastServlet = servlets[servlets.size()-1]
        lastServlet + {
            'servlet' {
                'servlet-name'("symmetricds")
                'servlet-class'(org.jumpmind.symmetric.web.SymmetricServlet.getName())
                'load-on-startup'('1')
            }
        } 
        
        def servletMappings = webXml.'servlet-mapping'
        def lastServletMapping = servletMappings[servletMappings.size()-1]
        lastServletMapping + {
            'servlet-mapping' {
                'servlet-name'("symmetricds")
                'url-pattern'("/sync/*")
            }
        }           
    
    }

    def doWithSpring = {
        def config = loadConfig()
        
        symmetricEngine(SpringWireableSymmetricEngine) {
            properties = config.toProperties()
        }
    }

    def doWithDynamicMethods = { ctx ->
        // TODO Implement registering dynamic methods to classes (optional)
    }

    def doWithApplicationContext = { applicationContext ->
        def config = loadConfig()
        ISymmetricEngine engine = applicationContext.symmetricEngine;
        if (engine.configured && config.auto.startup) {
            engine.start();
        } else if (config.auto.startup){
            log.warn 'Not configured to auto startup'
        } else {
            log.warn 'The symmetric engine is not configured.  Not starting.'
        }
    }

    def onChange = { event ->
        // TODO Implement code that is executed when any artefact that this plugin is
        // watching is modified and reloaded. The event contains: event.source,
        // event.application, event.manager, event.ctx, and event.plugin.
        
    }

    def onConfigChange = { event ->
        // TODO Implement code that is executed when the project configuration changes.
        // The event is the same as for 'onChange'.
    }
    
    private ConfigObject loadConfig() {
        
        def config = ConfigurationHolder.config
        
        GroovyClassLoader classLoader = new GroovyClassLoader(getClass().classLoader)
        
        // merging default config into main application config
        config.merge(new ConfigSlurper(GrailsUtil.environment).parse(classLoader.loadClass('DefaultSymmetricConfig')))
        
        // merging user-defined config into main application config if provided
        try {
            config.merge(new ConfigSlurper(GrailsUtil.environment).parse(classLoader.loadClass('SymmetricConfig')))
        } catch (Exception ignored) {
            // ignore, just use the defaults
        }
        
        return config.symmetric
    }
        
    
    
}
