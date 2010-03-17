<%@ page import="org.jumpmind.symmetric.grails.NodeGroupLink" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'nodeGroupLink.label', default: 'Link')}"/>
  <title><g:message code="default.list.label" args="[entityName]"/></title>
</head>
<body>

<div class="body">
<g:render template="/common/listMenu"/>
  <h1><g:message code="default.list.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="list">
    <table>
      <thead>
      <tr>

        <th><g:message code="nodeGroupLink.sourceNodeGroup.label" default="Source Node Group"/></th>

        <th><g:message code="nodeGroupLink.targetNodeGroup.label" default="Target Node Group"/></th>

        <g:sortableColumn property="dataEventAction" title="${message(code: 'nodeGroupLink.dataEventAction.label', default: 'Data Event Action')}"/>

      </tr>
      </thead>
      <tbody>
      <g:each in="${nodeGroupLinkInstanceList}" status="i" var="nodeGroupLinkInstance">
        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">

          <td><g:link action="show" id="${nodeGroupLinkInstance.PKAsCSV}">${fieldValue(bean: nodeGroupLinkInstance, field: "sourceNodeGroup.nodeGroupId")}</g:link></td>

          <td>${fieldValue(bean: nodeGroupLinkInstance, field: "targetNodeGroup.nodeGroupId")}</td>

          <td>${fieldValue(bean: nodeGroupLinkInstance, field: "dataEventAction")}</td>

        </tr>
      </g:each>
      </tbody>
    </table>
  </div>
  <div class="paginateButtons">
    <g:paginate total="${nodeGroupLinkInstanceTotal}"/>
  </div>
</div>
</body>
</html>
