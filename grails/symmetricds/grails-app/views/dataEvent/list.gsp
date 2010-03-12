
<%@ page import="org.jumpmind.symmetric.grails.DataEvent" %>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="layout" content="main" />
        <g:set var="entityName" value="${message(code: 'dataEvent.label', default: 'DataEvent')}" />
        <title><g:message code="default.list.label" args="[entityName]" /></title>
    </head>
    <body>
        <div class="body">
            <h1><g:message code="default.list.label" args="[entityName]" /></h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <div class="list">
                <table>
                    <thead>
                        <tr>
                            <g:sortableColumn property="dataDataId" title="${message(code: 'data.id.label', default: 'Data Id')}" />
                            <g:sortableColumn property="dataTableName" title="${message(code: 'data.tableName.label', default: 'Table Name')}" />
                            <g:sortableColumn property="dataEventType" title="${message(code: 'data.eventType.label', default: 'Event Type')}" />
                            <g:sortableColumn property="dataRowData" title="${message(code: 'data.rowData.label', default: 'Row Data')}" />
                            <g:sortableColumn property="dataPkData" title="${message(code: 'data.pkData.label', default: 'PK Data')}" />
                            <g:sortableColumn property="dataOldData" title="${message(code: 'data.oldData.label', default: 'Old Data')}" />                                                    
                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${dataEventInstanceList}" status="i" var="dataEventInstance">
                        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                            <td><g:link controller="data" action="show" id="${dataEventInstance?.data?.dataId}">${dataEventInstance?.data?.dataId}</g:link></td>
                            <td>${dataEventInstance?.data?.tableName}</td>
                            <td>${dataEventInstance?.data?.eventType}</td>
                            <td>${dataEventInstance?.data?.rowData}</td>
                            <td>${dataEventInstance?.data?.pkData}</td>
                            <td>${dataEventInstance?.data?.oldData}</td>
                        </tr>
                    </g:each>
                    </tbody>
                </table>
            </div>
        </div>
    </body>
</html>
