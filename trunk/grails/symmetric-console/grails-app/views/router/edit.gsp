<%@ page import="org.jumpmind.symmetric.grails.NodeGroupLink; org.jumpmind.symmetric.grails.Channel; org.jumpmind.symmetric.grails.Router" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'router.label', default: 'Router')}"/>
  <title><g:message code="default.edit.label" args="[entityName]"/></title>
</head>
<body>

<div class="body">
<g:render template="/common/editMenu" model="[id:routerInstance.routerId]"/>
  <h1><g:message code="default.edit.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <g:hasErrors bean="${errors}">
    <div class="errors">
      <g:renderErrors bean="${errors}" as="list"/>
    </div>
  </g:hasErrors>
  <g:form name="form" controller="router" action="update" method="post">
    <g:hiddenField name="id" value="${routerInstance?.id}"/>
    <g:hiddenField name="version" value="${routerInstance?.version}"/>
    <div class="dialog">
      <table>
        <tbody>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="routerId"><g:message code="router.routerId.label" default="Router Id"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: routerInstance, field: 'routerId', 'errors')}">
            <g:hiddenField name="routerId" value="${routerInstance?.routerId}"/>
            ${routerInstance?.routerId}
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="nodeGroupLink"><g:message code="router.nodeGroupLink.label" default="Node Group Link"/></label>
          </td>
          <td valign="top" class="value ${routerInstance.errors.getFieldErrorCount('sourceNodeGroupId') > 0 || routerInstance.errors.getFieldErrorCount('targetNodeGroupId') > 0 ? 'errors' : ''}">
            <g:select name="nodeGroupLink" noSelection="${['':'Select a link ...']}" from="${NodeGroupLink.list()}" optionKey="PKAsCSV" value="${nodeGroupLink.PKAsCSV}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="routerType"><g:message code="router.routerType.label" default="Router Type"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: routerInstance, field: 'routerType', 'errors')}">
            <g:select name="routerType" from="${routers}" optionKey="key" optionValue="key" value="${routerInstance?.routerType}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="routerExpression"><g:message code="router.routerExpression.label" default="Router Expression"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: routerInstance, field: 'routerExpression', 'errors')}">
            <g:textField name="routerExpression" value="${routerInstance?.routerExpression}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnInsert"><g:message code="router.syncOnInsert.label" default="Sync On Insert"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: routerInstance, field: 'syncOnInsert', 'errors')}">
            <g:checkBox name="syncOnInsert" value="${routerInstance?.syncOnInsert}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnUpdate"><g:message code="router.syncOnUpdate.label" default="Sync On Update"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: routerInstance, field: 'syncOnUpdate', 'errors')}">
            <g:checkBox name="syncOnUpdate" value="${routerInstance?.syncOnUpdate}"/>
          </td>
        </tr>


        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnDelete"><g:message code="router.syncOnDelete.label" default="Sync On Delete"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: routerInstance, field: 'syncOnDelete', 'errors')}">
            <g:checkBox name="syncOnDelete" value="${routerInstance?.syncOnDelete}"/>
          </td>
        </tr>

        <!--
                        <tr class="prop">
                            <td valign="top" class="name">
                                <label for="targetCatalogName"><g:message code="router.targetCatalogName.label" default="Target Catalog Name"/></label>
                            </td>
                            <td valign="top" class="value ${hasErrors(bean: routerInstance, field: 'targetCatalogName', 'errors')}">
                                <g:textField name="targetCatalogName" value="${routerInstance?.targetCatalogName}"/>
                            </td>
                        </tr>

                        <tr class="prop">
                            <td valign="top" class="name">
                                <label for="targetSchemaName"><g:message code="router.targetSchemaName.label" default="Target Schema Name"/></label>
                            </td>
                            <td valign="top" class="value ${hasErrors(bean: routerInstance, field: 'targetSchemaName', 'errors')}">
                                <g:textField name="targetSchemaName" value="${routerInstance?.targetSchemaName}"/>
                            </td>
                        </tr>

                        <tr class="prop">
                            <td valign="top" class="name">
                                <label for="targetTableName"><g:message code="router.targetTableName.label" default="Target Table Name"/></label>
                            </td>
                            <td valign="top" class="value ${hasErrors(bean: routerInstance, field: 'targetTableName', 'errors')}">
                                <g:textField name="targetTableName" value="${routerInstance?.targetTableName}"/>
                            </td>
                        </tr>
                        -->

        <tr class="prop">
          <td valign="top" class="name" colspan="2">
            <div class="list">
              <table>
                <thead>
                <tr>
                  <th>${message(code: 'router.triggers.fullTableName.label', default: 'Table Name')}</th>
                  <th>${message(code: 'router.triggers.channelId.label', default: 'Channel')}</th>
                  <th>${message(code: 'router.triggers.loadOrder.label', default: 'Load Order')}</th>
                  <th>${message(code: 'router.triggers.selected.label', default: 'Selected')}</th>
                </tr>
                </thead>
                <tbody>
                <g:hiddenField name="triggersCount" value="${triggers?.size()}"/>
                <g:each in="${triggers}" status="i" var="trigger">
                  <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                    <td>${fieldValue(bean: trigger, field: "fullTableName")}
                    <g:hiddenField name="triggers[${i}].triggerId" value="${trigger?.triggerId}"/>
                    <g:hiddenField name="triggers[${i}].catalogName" value="${trigger?.catalogName}"/>
                    <g:hiddenField name="triggers[${i}].schemaName" value="${trigger?.schemaName}"/>
                    <g:hiddenField name="triggers[${i}].tableName" value="${trigger?.tableName}"/>
                    </td>
                    <td>
                      <g:if test="${!trigger.triggerId}">
                        <g:select name="triggers[${i}].channelId" from="${Channel.filterOutSystemChannels(Channel.list())}" optionKey="channelId" optionValue="channelId" noSelection="${['':'Select a channel ...']}" value="${trigger?.channelId}"/>
                      </g:if>
                      <g:else>
                        ${trigger?.channelId}
                      </g:else>
                    </td>
                    <td><g:textField name="triggers[${i}].order" value="${trigger.order}"/></td>
                    <td><g:checkBox name="triggers[${i}].selected" value="${trigger.selected}"/></td>
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
  </g:form>
</div>
</body>
</html>
