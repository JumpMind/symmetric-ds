<%@ page import="org.jumpmind.symmetric.grails.Channel; org.jumpmind.symmetric.grails.Trigger" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'trigger.label', default: 'Trigger')}"/>
  <title><g:message code="default.edit.label" args="[entityName]"/></title>
</head>
<body>

<div class="body">
<g:render template="/common/editMenu" model="[id:triggerInstance.triggerId]"/>
  <h1><g:message code="default.edit.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <g:hasErrors bean="${triggerInstance}">
    <div class="errors">
      <g:renderErrors bean="${triggerInstance}" as="list"/>
    </div>
  </g:hasErrors>
  <g:form name="form" controller="trigger" action="update" method="post">
    <g:hiddenField name="id" value="${triggerInstance?.id}"/>
    <g:hiddenField name="version" value="${triggerInstance?.version}"/>
    <div class="dialog">
      <table>
        <tbody>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="triggerId"><g:message code="trigger.triggerId.label" default="Trigger Id"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'triggerId', 'errors')}">
            <g:textField name="triggerId" value="${triggerInstance?.triggerId}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="sourceTableName"><g:message code="trigger.sourceTableName.label" default="Source Table Name"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'sourceTableName', 'errors')}">
            <g:textField name="sourceTableName" value="${triggerInstance?.sourceTableName}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="sourceCatalogName"><g:message code="trigger.sourceCatalogName.label" default="Source Catalog Name"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'sourceCatalogName', 'errors')}">
            <g:textField name="sourceCatalogName" value="${triggerInstance?.sourceCatalogName}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="sourceSchemaName"><g:message code="trigger.sourceSchemaName.label" default="Source Schema Name"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'sourceSchemaName', 'errors')}">
            <g:textField name="sourceSchemaName" value="${triggerInstance?.sourceSchemaName}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="channelId"><g:message code="trigger.channelId.label" default="Channel Id"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'channelId', 'errors')}">
            <g:select name="channelId" from="${Channel.filterOutSystemChannels(Channel.list())}" optionKey="channelId" optionValue="channelId" noSelection="${['':'Select a channel ...']}" value="${triggerInstance?.channelId}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnInsert"><g:message code="trigger.syncOnInsert.label" default="Sync On Insert"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'syncOnInsert', 'errors')}">
            <g:checkBox name="syncOnInsert" value="${triggerInstance?.syncOnInsert}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnUpdate"><g:message code="trigger.syncOnUpdate.label" default="Sync On Update"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'syncOnUpdate', 'errors')}">
            <g:checkBox name="syncOnUpdate" value="${triggerInstance?.syncOnUpdate}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnDelete"><g:message code="trigger.syncOnDelete.label" default="Sync On Delete"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'syncOnDelete', 'errors')}">
            <g:checkBox name="syncOnDelete" value="${triggerInstance?.syncOnDelete}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnIncomingBatch"><g:message code="trigger.syncOnIncomingBatch.label" default="Sync On Incoming Batch"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'syncOnIncomingBatch', 'errors')}">
            <g:checkBox name="syncOnIncomingBatch" value="${triggerInstance?.syncOnIncomingBatch}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnInsertCondition"><g:message code="trigger.syncOnInsertCondition.label" default="Sync On Insert Condition"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'syncOnInsertCondition', 'errors')}">
            <g:textField name="syncOnInsertCondition" value="${triggerInstance?.syncOnInsertCondition}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnUpdateCondition"><g:message code="trigger.syncOnUpdateCondition.label" default="Sync On Update Condition"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'syncOnUpdateCondition', 'errors')}">
            <g:textField name="syncOnUpdateCondition" value="${triggerInstance?.syncOnUpdateCondition}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncOnDeleteCondition"><g:message code="trigger.syncOnDeleteCondition.label" default="Sync On Delete Condition"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'syncOnDeleteCondition', 'errors')}">
            <g:textField name="syncOnDeleteCondition" value="${triggerInstance?.syncOnDeleteCondition}"/>
          </td>
        </tr>
        
        <tr class="prop">
          <td valign="top" class="name">
            <label for="nameForInsertTrigger"><g:message code="trigger.nameForInsertTrigger.label" default="Name For Insert Trigger"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'nameForInsertTrigger', 'errors')}">
            <g:textField name="nameForInsertTrigger" value="${triggerInstance?.nameForInsertTrigger}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="nameForUpdateTrigger"><g:message code="trigger.nameForUpdateTrigger.label" default="Name For Update Trigger"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'nameForUpdateTrigger', 'errors')}">
            <g:textField name="nameForUpdateTrigger" value="${triggerInstance?.nameForUpdateTrigger}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="nameForDeleteTrigger"><g:message code="trigger.nameForDeleteTrigger.label" default="Name For Delete Trigger"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'nameForDeleteTrigger', 'errors')}">
            <g:textField name="nameForDeleteTrigger" value="${triggerInstance?.nameForDeleteTrigger}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="txIdExpression"><g:message code="trigger.txIdExpression.label" default="Tx Id Expression"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'txIdExpression', 'errors')}">
            <g:textField name="txIdExpression" value="${triggerInstance?.txIdExpression}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="excludedColumnNames"><g:message code="trigger.excludedColumnNames.label" default="Excluded Column Names"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: triggerInstance, field: 'excludedColumnNames', 'errors')}">
            <g:textField name="excludedColumnNames" value="${triggerInstance?.excludedColumnNames}"/>
          </td>
        </tr>

        </tbody>
      </table>
    </div>
  </g:form>
</div>
</body>
</html>
