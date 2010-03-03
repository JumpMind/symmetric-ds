package org.jumpmind.symmetric.server

import grails.validation.ValidationException

class RouterController {

  static allowedMethods = [save: "POST", update: "POST", delete: "GET"]

  def symmetricEngine

  def triggerRouterService

  def nodeGroupLinkService

  def index = {
    redirect(action: "list", params: params)
  }

  def list = {
    params.max = Math.min(params.max ? params.max.toInteger() : 10, 100)
    [routerInstanceList: org.jumpmind.symmetric.grails.Router.list(params), routerInstanceTotal: org.jumpmind.symmetric.grails.Router.count()]
  }

  def create = {
    def routerInstance = new org.jumpmind.symmetric.grails.Router()
    routerInstance.routerId = null
    routerInstance.properties = params
    return [routerInstance: routerInstance, routers: symmetricEngine.applicationContext.getBean('routers'), triggers: lookupTriggerTables()]
  }

  def save = {
    def routerInstance = new org.jumpmind.symmetric.grails.Router(params)
    def triggerTables = bindTriggerTables(params)
    try {
      triggerRouterService.saveNewRouter(routerInstance, params, triggerTables)
      symmetricEngine.syncTriggers()
      flash.message = "${message(code: 'default.created.message', args: [message(code: 'router.label', default: 'Router'), routerInstance.routerId])}"
      redirect(action: "show", id: routerInstance.routerId)
    } catch (ValidationException ex) {
      render(view: "create", model: [routerInstance: routerInstance, nodeGroupLink: nodeGroupLinkService.lookupNodeGroupLink(params), routers: symmetricEngine.applicationContext.getBean('routers'), triggers: triggerTables, errors: ex.errors])
    }
  }

  def show = {
    def routerInstance = org.jumpmind.symmetric.grails.Router.findByRouterId(params.id)
    if (!routerInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'router.label', default: 'Router'), params.id])}"
      redirect(action: "list")
    }
    else {
      return [routerInstance: routerInstance, routers: symmetricEngine.applicationContext.getBean('routers'), triggers: lookupTriggerTables(routerInstance, true)]
    }
  }

  def edit = {
    def routerInstance = org.jumpmind.symmetric.grails.Router.findByRouterId(params.id)
    if (!routerInstance) {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'router.label', default: 'Router'), params.id])}"
      redirect(action: "list")
    }
    else {
      return [routerInstance: routerInstance, nodeGroupLink: nodeGroupLinkService.lookupNodeGroupLink([nodeGroupLink: "${routerInstance.sourceNodeGroupId},${routerInstance.targetNodeGroupId}"]), routers: symmetricEngine.applicationContext.getBean('routers'), triggers: lookupTriggerTables(routerInstance)]
    }
  }

  def update = {
    def routerInstance = org.jumpmind.symmetric.grails.Router.get(params.routerId)
    def triggerTables = bindTriggerTables(params)
    if (routerInstance) {
      try {
        triggerRouterService.updateRouter(routerInstance, params, triggerTables)
        symmetricEngine.syncTriggers()
        flash.message = "${message(code: 'default.updated.message', args: [message(code: 'router.label', default: 'Router'), routerInstance.routerId])}"
        redirect(action: "show", id: routerInstance.routerId)
      } catch (ValidationException ex) {
        render(view: "edit", model: [routerInstance: routerInstance, nodeGroupLink: triggerRouterService.lookupNodeGroupLink(params), routers: symmetricEngine.applicationContext.getBean('routers'), triggers: triggerTables, errors: ex.errors])
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'router.label', default: 'Router'), params.routerId])}"
      redirect(action: "list")
    }
  }

  def delete = {
    def routerInstance = org.jumpmind.symmetric.grails.Router.findByRouterId(params.routerId)
    if (routerInstance) {
      try {
        def triggerRouters = org.jumpmind.symmetric.grails.TriggerRouter.findAllByRouter(routerInstance)
        triggerRouters.each {
          it.delete(flush: true)
        }
        routerInstance.delete(flush: true)
        flash.message = "${message(code: 'default.deleted.message', args: [message(code: 'router.label', default: 'Router'), params.routerId])}"
        redirect(action: "list")
      }
      catch (org.springframework.dao.DataIntegrityViolationException e) {
        flash.message = "${message(code: 'default.not.deleted.message', args: [message(code: 'router.label', default: 'Router'), params.routerId])}"
        redirect(action: "show", id: params.routerId)
      }
    }
    else {
      flash.message = "${message(code: 'default.not.found.message', args: [message(code: 'router.label', default: 'Router'), params.routerId])}"
      redirect(action: "list")
    }
  }

  private def bindTriggerTables(params) {
    def triggerTables = []
    for (i in 0..Integer.parseInt(params.triggersCount) - 1) {
      def cmd = new TriggerMapping()
      bindData(cmd, params["triggers[${i}]"], "_selected")
      triggerTables.add cmd
    }
    return triggerTables
  }

  private def lookupTriggerTables(org.jumpmind.symmetric.grails.Router routerInstance = null, boolean onlyAttached = false) {
    def triggerTables = [:]
    if (!onlyAttached) {
      // TODO cache for performance??
      org.apache.ddlutils.model.Database database = symmetricEngine.dbDialect.readPlatformDatabase(false)
      database.getTables().each {
        def catalogName = it.getCatalog()?.toUpperCase()
        def schemaName = it.getSchema()?.toUpperCase()
        def tableName = it.getName().toUpperCase()
        TriggerMapping table = new TriggerMapping(tableName: tableName, schemaName: schemaName, catalogName: catalogName)
        triggerTables.put(table.getFullTableName(), table)
      }

      org.jumpmind.symmetric.grails.Trigger.list().each {
        addTriggerToTriggerTables(100, it, triggerTables, false)
      }
    }

    if (routerInstance) {
      org.jumpmind.symmetric.grails.TriggerRouter.findAllByRouter(routerInstance).each {
        addTriggerToTriggerTables(it.initialLoadOrder, it.trigger, triggerTables, true)
      }
    }

    return triggerTables.values().sort { a,b ->
      if (a.order == b.order) {
        return a.fullTableName.compareTo(b.fullTableName)
      } else {
        return a.order.compareTo(b.order)
      }
    }

  }

  private def addTriggerToTriggerTables(initialLoadOrder, org.jumpmind.symmetric.grails.Trigger trigger, triggerTables, selected) {
    def catalogName = trigger.sourceCatalogName?.toUpperCase()
    def schemaName = trigger.sourceSchemaName?.toUpperCase()
    def tableName = trigger.sourceTableName.toUpperCase()
    TriggerMapping table = new TriggerMapping(tableName: tableName, schemaName: schemaName, catalogName: catalogName, channelId: trigger.channelId)
    if (triggerTables.containsKey(table.getFullTableName())) {
      table = triggerTables.get(table.getFullTableName())
    } else if (triggerTables.containsKey(tableName)) {
      table = triggerTables.get(tableName)
    } else {
      triggerTables.put(table.getFullTableName(), table)
    }

    table.order = initialLoadOrder
    table.channelId = trigger.channelId
    table.triggerId = trigger.triggerId
    table.selected = selected
  }

}



