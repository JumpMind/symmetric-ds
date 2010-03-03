package org.jumpmind.symmetric.server

class TriggerRouterService {

  boolean transactional = true

  def nodeGroupLinkService

  def userService

  def saveNewRouter(routerInstance, params, triggerTables) {
    org.jumpmind.symmetric.grails.NodeGroupLink nodeGroupLink = nodeGroupLinkService.lookupNodeGroupLink(params)
    if (nodeGroupLink) {
      routerInstance.sourceNodeGroupId = nodeGroupLink.sourceNodeGroup.nodeGroupId
      routerInstance.targetNodeGroupId = nodeGroupLink.targetNodeGroup.nodeGroupId
    }
    routerInstance.createTime = new Date()
    routerInstance.lastUpdateTime = routerInstance.createTime
    // TODO
    routerInstance.lastUpdateBy = userService.currentUser()
    def results = routerInstance.save(flush: true, failOnError: true)
    if (results) {
      saveTriggerRouters(routerInstance, params, triggerTables)
    }
    return results
  }

  def updateRouter(routerInstance, params, triggerTables) {
    routerInstance.properties = params
    org.jumpmind.symmetric.grails.NodeGroupLink nodeGroupLink = nodeGroupLinkService.lookupNodeGroupLink(params)
    if (nodeGroupLink) {
      routerInstance.sourceNodeGroupId = nodeGroupLink.sourceNodeGroup.nodeGroupId
      routerInstance.targetNodeGroupId = nodeGroupLink.targetNodeGroup.nodeGroupId
    }
    routerInstance.lastUpdateTime = new Date()
    // TODO
    routerInstance.lastUpdateBy = userService.currentUser()
    def results = routerInstance.save(flush: true, failOnError: true)
    if (results) {
      saveTriggerRouters(routerInstance, params, triggerTables)
    }
    return results
  }

  def saveTriggerRouters(org.jumpmind.symmetric.grails.Router routerInstance, params, triggerTables) {

    def triggerRouters = org.jumpmind.symmetric.grails.TriggerRouter.findAllByRouter(routerInstance)
    def mapByTriggerId = [:]
    triggerRouters.each {
      mapByTriggerId.put it.trigger.triggerId, it
    }

    def assignedTriggerIds = []
    triggerTables.each {
      if (it.selected) {
        assignedTriggerIds.add(it.triggerId)
        mapByTriggerId.get(it.triggerId)?.initialLoadOrder = it.order
      }
    }

    // need to be deleted
    def deletedTriggerIds = []
    deletedTriggerIds.addAll(mapByTriggerId.keySet())
    deletedTriggerIds.removeAll(assignedTriggerIds)
    deletedTriggerIds.each {
      mapByTriggerId.get(it)?.delete(flush: true, failOnError: true)
    }

    // need to be added
    assignedTriggerIds.removeAll(mapByTriggerId.keySet())
    assignedTriggerIds.each {
      org.jumpmind.symmetric.grails.Trigger trigger = org.jumpmind.symmetric.grails.Trigger.findByTriggerId(it)
      if (trigger) {
        org.jumpmind.symmetric.grails.TriggerRouter triggerRouter = new org.jumpmind.symmetric.grails.TriggerRouter(router: routerInstance, trigger: trigger)
        triggerRouter.save(flush: true, failOnError: true)
      }
    }

    triggerTables.each {
      def tableName = it.tableName
      def selected = it.selected
      def id = it.triggerId
      if (!id && tableName && selected) {
        org.jumpmind.symmetric.grails.Trigger trigger = new org.jumpmind.symmetric.grails.Trigger()
        trigger.createTime = new Date()
        trigger.lastUpdateTime = trigger.createTime
        trigger.lastUpdateBy = userService.currentUser()
        trigger.sourceCatalogName = it.catalogName
        trigger.sourceSchemaName = it.schemaName
        trigger.channelId = it.channelId
        trigger.sourceTableName = tableName
        trigger.triggerId = trigger.fullTableName
        trigger.save(flush: true, failOnError: true)
        org.jumpmind.symmetric.grails.TriggerRouter triggerRouter = new org.jumpmind.symmetric.grails.TriggerRouter(router: routerInstance, trigger: trigger, initialLoadOrder:it.order)
        triggerRouter.save(flush: true, failOnError: true)
      }
    }


  }

}
