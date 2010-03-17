package org.jumpmind.symmetric.server

class NodeGroupLinkService {

  boolean transactional = true

  def lookupNodeGroupLink(params) {
    def id = params.nodeGroupLink
    if (!id) {
      id = params.id
    }
    if (id) {
      def nodeGroupLink = id.split(',')
      org.jumpmind.symmetric.grails.NodeGroup source = org.jumpmind.symmetric.grails.NodeGroup.findByNodeGroupId(nodeGroupLink[0])
      org.jumpmind.symmetric.grails.NodeGroup target = org.jumpmind.symmetric.grails.NodeGroup.findByNodeGroupId(nodeGroupLink[1])
      return org.jumpmind.symmetric.grails.NodeGroupLink.get(new org.jumpmind.symmetric.grails.NodeGroupLink(sourceNodeGroup: source, targetNodeGroup: target))
    }
  }
}
