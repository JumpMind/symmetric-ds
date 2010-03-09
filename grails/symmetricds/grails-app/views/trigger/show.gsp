<%@ page import="org.jumpmind.symmetric.grails.Trigger" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'trigger.label', default: 'Trigger')}"/>
  <title><g:message code="default.show.label" args="[entityName]"/></title>
</head>
<body>

<div class="body">
<g:render template="/common/showMenu" model="[id:triggerInstance.triggerId]"/>
  <h1><g:message code="default.show.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="dialog">
    <table>
      <tbody>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.id.label" default="Trigger Id"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "triggerId")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.sourceCatalogName.label" default="Source Catalog Name"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "sourceCatalogName")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.sourceSchemaName.label" default="Source Schema Name"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "sourceSchemaName")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.sourceTableName.label" default="Source Table Name"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "sourceTableName")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.channelId.label" default="Channel Id"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "channelId")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.syncOnInsert.label" default="Sync On Insert"/></td>
        <td valign="top" class="value"><g:formatBoolean boolean="${triggerInstance?.syncOnInsert}"/></td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.syncOnUpdate.label" default="Sync On Update"/></td>
        <td valign="top" class="value"><g:formatBoolean boolean="${triggerInstance?.syncOnUpdate}"/></td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.syncOnDelete.label" default="Sync On Delete"/></td>
        <td valign="top" class="value"><g:formatBoolean boolean="${triggerInstance?.syncOnDelete}"/></td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.syncOnIncomingBatch.label" default="Sync On Incoming Batch"/></td>
        <td valign="top" class="value"><g:formatBoolean boolean="${triggerInstance?.syncOnIncomingBatch}"/></td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.syncOnInsertCondition.label" default="Sync On Insert Condition"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "syncOnInsertCondition")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.syncOnUpdateCondition.label" default="Sync On Update Condition"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "syncOnUpdateCondition")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.syncOnDeleteCondition.label" default="Sync On Delete Condition"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "syncOnDeleteCondition")}</td>
      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.nameForInsertTrigger.label" default="Name For Insert Trigger"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "nameForInsertTrigger")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.nameForUpdateTrigger.label" default="Name For Update Trigger"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "nameForUpdateTrigger")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.nameForDeleteTrigger.label" default="Name For Delete Trigger"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "nameForDeleteTrigger")}</td></tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.txIdExpression.label" default="Tx Id Expression"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "txIdExpression")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.excludedColumnNames.label" default="Excluded Column Names"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "excludedColumnNames")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.lastUpdateBy.label" default="Last Update By"/></td>
        <td valign="top" class="value">${fieldValue(bean: triggerInstance, field: "lastUpdateBy")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.lastUpdateTime.label" default="Last Update Time"/></td>
        <td valign="top" class="value"><g:formatDate date="${triggerInstance?.lastUpdateTime}"/></td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="trigger.createTime.label" default="Create Time"/></td>
        <td valign="top" class="value"><g:formatDate date="${triggerInstance?.createTime}"/></td>
      </tr>

      </tbody>
    </table>
  </div>
</div>
</body>
</html>
