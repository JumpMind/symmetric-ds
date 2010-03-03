<%@ page import="org.jumpmind.symmetric.grails.NodeGroup" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'nodeGroup.label', default: 'Group')}"/>
  <title><g:message code="default.show.label" args="[entityName]"/></title>
</head>
<body>
<g:render template="/common/showMenu" model="[id:nodeGroupInstance.nodeGroupId]"/>
<div class="body">
  <h1><g:message code="default.show.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="dialog">
    <table>
      <tbody>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="nodeGroup.nodeGroupId.label" default="Node Group Id"/></td>
        <td valign="top" class="value">${fieldValue(bean: nodeGroupInstance, field: "nodeGroupId")}</td>
      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="nodeGroup.description.label" default="Description"/></td>
        <td valign="top" class="value">${fieldValue(bean: nodeGroupInstance, field: "description")}</td>
      </tr>

      </tbody>
    </table>
  </div>
</div>
</body>
</html>
