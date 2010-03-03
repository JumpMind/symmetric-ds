package org.jumpmind.symmetric.grails

import org.codehaus.groovy.grails.commons.ConfigurationHolder

class NodeHost implements Serializable {

  private @Delegate org.jumpmind.symmetric.model.NodeHost delegate = new org.jumpmind.symmetric.model.NodeHost()

  static mapping = {
    def config = ConfigurationHolder.config
    table config.symmetric.sync.table.prefix + '_node_host'
    version false
    id composite: ['nodeId', 'hostName'], generator: 'assigned'
    autoTimestamp false
  }

  static constraints = {

  }

}