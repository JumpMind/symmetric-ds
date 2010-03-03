<%@ page import="org.jumpmind.symmetric.grails.Channel; org.jumpmind.symmetric.grails.Router" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'router.label', default: 'Router')}"/>
  <title><g:message code="default.show.label" args="[entityName]"/></title>
</head>
<body>
<g:render template="/common/showMenu" model="[id:routerInstance.routerId]"/>
<div class="body">
  <h1><g:message code="default.show.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="dialog">
    <table>
      <tbody>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.routerId.label" default="Router Id"/></td>

        <td valign="top" class="value">${fieldValue(bean: routerInstance, field: "routerId")}</td>

      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.sourceNodeGroupId.label" default="Source Node Group Id"/></td>

        <td valign="top" class="value">${fieldValue(bean: routerInstance, field: "sourceNodeGroupId")}</td>

      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.targetNodeGroupId.label" default="Target Node Group Id"/></td>

        <td valign="top" class="value">${fieldValue(bean: routerInstance, field: "targetNodeGroupId")}</td>

      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.syncOnInsert.label" default="Sync On Insert"/></td>

        <td valign="top" class="value"><g:formatBoolean boolean="${routerInstance?.syncOnInsert}"/></td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.syncOnUpdate.label" default="Sync On Update"/></td>

        <td valign="top" class="value"><g:formatBoolean boolean="${routerInstance?.syncOnUpdate}"/></td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.syncOnDelete.label" default="Sync On Delete"/></td>

        <td valign="top" class="value"><g:formatBoolean boolean="${routerInstance?.syncOnDelete}"/></td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.routerType.label" default="Router Type"/></td>

        <td valign="top" class="value">${fieldValue(bean: routerInstance, field: "routerType")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.routerExpression.label" default="Router Expression"/></td>

        <td valign="top" class="value">${fieldValue(bean: routerInstance, field: "routerExpression")}</td>

      </tr>

      <!--      
      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.targetCatalogName.label" default="Target Catalog Name"/></td>

        <td valign="top" class="value">${fieldValue(bean: routerInstance, field: "targetCatalogName")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.targetSchemaName.label" default="Target Schema Name"/></td>

        <td valign="top" class="value">${fieldValue(bean: routerInstance, field: "targetSchemaName")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.targetTableName.label" default="Target Table Name"/></td>

        <td valign="top" class="value">${fieldValue(bean: routerInstance, field: "targetTableName")}</td>

      </tr>
      -->

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.lastUpdateBy.label" default="Last Update By"/></td>

        <td valign="top" class="value">${fieldValue(bean: routerInstance, field: "lastUpdateBy")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.lastUpdateTime.label" default="Last Update Time"/></td>

        <td valign="top" class="value"><g:formatDate date="${routerInstance?.lastUpdateTime}"/></td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="router.createTime.label" default="Create Time"/></td>

        <td valign="top" class="value"><g:formatDate date="${routerInstance?.createTime}"/></td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name" colspan="2">
          <div class="list">
            <table>
              <thead>
              <tr>
                <th>${message(code: 'router.triggers.fullTableName.label', default: 'Table Name')}</th>
                <th>${message(code: 'router.triggers.channelId.label', default: 'Channel')}</th>
                <th>${message(code: 'router.triggers.loadOrder.label', default: 'Load Order')}</th>
              </tr>
              </thead>
              <tbody>
              <g:hiddenField name="triggersCount" value="${triggers?.size()}"/>
              <g:each in="${triggers}" status="i" var="trigger">
                <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                  <td>
                    <g:link controller="trigger" action="show" id="${trigger.triggerId}">${fieldValue(bean: trigger, field: "fullTableName")}</g:link>
                  </td>
                  <td>
                    ${trigger?.channelId}
                  </td>
                  <td>${trigger?.order}</td>        
                </tr>
              </g:each>
              </tbody>
            </table>
          </div>
        </td>
      </tr>

      </tbody>
    </table>
  </div>
</div>
</body>
</html>
