<%@ page import="org.jumpmind.symmetric.grails.NodeGroupLink" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'nodeGroupLink.label', default: 'Link')}"/>
  <title><g:message code="default.show.label" args="[entityName]"/></title>
</head>
<body>
<g:render template="/common/showMenu" model="[id:nodeGroupLinkInstance.PKAsCSV]"/>
<div class="body">
  <h1><g:message code="default.show.label" args="['Link']"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="dialog">
    <table>
      <tbody>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="nodeGroupLink.sourceNodeGroup.label" default="Source Node Group"/></td>

        <td valign="top" class="value"><g:link controller="nodeGroup" action="show" id="${nodeGroupLinkInstance?.sourceNodeGroup?.nodeGroupId}">${nodeGroupLinkInstance?.sourceNodeGroup?.nodeGroupId}</g:link></td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="nodeGroupLink.targetNodeGroup.label" default="Target Node Group"/></td>

        <td valign="top" class="value"><g:link controller="nodeGroup" action="show" id="${nodeGroupLinkInstance?.targetNodeGroup?.nodeGroupId}">${nodeGroupLinkInstance?.targetNodeGroup?.nodeGroupId}</g:link></td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="nodeGroupLink.dataEventAction.label" default="Data Event Action"/></td>

        <td valign="top" class="value">${nodeGroupLinkInstance?.dataEventAction?.encodeAsHTML()}</td>

      </tr>

      </tbody>
    </table>
  </div>
</div>
</body>
</html>
