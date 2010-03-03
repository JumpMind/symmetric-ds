<%@ page import="org.jumpmind.symmetric.grails.Trigger" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'trigger.label', default: 'Trigger')}"/>
  <title><g:message code="default.list.label" args="[entityName]"/></title>
</head>
<body>
<g:render template="/common/listMenu"/>
<div class="body">
  <h1><g:message code="default.list.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="list">
    <table>
      <thead>
      <tr>

        <g:sortableColumn property="triggerId" title="${message(code: 'trigger.id.label', default: 'Trigger Id')}"/>

        <g:sortableColumn property="channelId" title="${message(code: 'trigger.channelId.label', default: 'Channel Id')}"/>

        <g:sortableColumn property="nameForInsertTrigger" title="${message(code: 'trigger.nameForInsertTrigger.label', default: 'Name For Insert Trigger')}"/>

        <g:sortableColumn property="nameForUpdateTrigger" title="${message(code: 'trigger.nameForUpdateTrigger.label', default: 'Name For Update Trigger')}"/>

        <g:sortableColumn property="nameForDeleteTrigger" title="${message(code: 'trigger.nameForDeleteTrigger.label', default: 'Name For Delete Trigger')}"/>

        <g:sortableColumn property="txIdExpression" title="${message(code: 'trigger.txIdExpression.label', default: 'Tx Id Expression')}"/>

      </tr>
      </thead>
      <tbody>
      <g:each in="${triggerInstanceList}" status="i" var="triggerInstance">
        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">

          <td><g:link action="show" id="${triggerInstance.triggerId}">${fieldValue(bean: triggerInstance, field: "triggerId")}</g:link></td>

          <td>${fieldValue(bean: triggerInstance, field: "channelId")}</td>

          <td>${fieldValue(bean: triggerInstance, field: "nameForInsertTrigger")}</td>

          <td>${fieldValue(bean: triggerInstance, field: "nameForUpdateTrigger")}</td>

          <td>${fieldValue(bean: triggerInstance, field: "nameForDeleteTrigger")}</td>

          <td>${fieldValue(bean: triggerInstance, field: "txIdExpression")}</td>

        </tr>
      </g:each>
      </tbody>
    </table>
  </div>
  <div class="paginateButtons">
    <g:paginate total="${triggerInstanceTotal}"/>
  </div>
</div>
</body>
</html>
