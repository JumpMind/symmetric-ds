
<%@ page import="org.jumpmind.symmetric.grails.IncomingBatch" %>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="layout" content="main" />
        <g:set var="entityName" value="${message(code: 'incomingBatch.label', default: 'IncomingBatch')}" />
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
                        
                            <g:sortableColumn property="id" title="${message(code: 'incomingBatch.id.label', default: 'Id')}" />
                        
                            <g:sortableColumn property="persistable" title="${message(code: 'incomingBatch.persistable.label', default: 'Persistable')}" />
                        
                            <g:sortableColumn property="createTime" title="${message(code: 'incomingBatch.createTime.label', default: 'Create Time')}" />
                        
                            <g:sortableColumn property="sqlCode" title="${message(code: 'incomingBatch.sqlCode.label', default: 'Sql Code')}" />
                        
                            <g:sortableColumn property="filterMillis" title="${message(code: 'incomingBatch.filterMillis.label', default: 'Filter Millis')}" />
                        
                            <g:sortableColumn property="batchId" title="${message(code: 'incomingBatch.batchId.label', default: 'Batch Id')}" />
                        
                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${incomingBatchInstanceList}" status="i" var="incomingBatchInstance">
                        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                        
                            <td><g:link action="show" id="${incomingBatchInstance.id}">${fieldValue(bean: incomingBatchInstance, field: "id")}</g:link></td>
                        
                            <td><g:formatBoolean boolean="${incomingBatchInstance.persistable}" /></td>
                        
                            <td><g:formatDate date="${incomingBatchInstance.createTime}" /></td>
                        
                            <td>${fieldValue(bean: incomingBatchInstance, field: "sqlCode")}</td>
                        
                            <td>${fieldValue(bean: incomingBatchInstance, field: "filterMillis")}</td>
                        
                            <td>${fieldValue(bean: incomingBatchInstance, field: "batchId")}</td>
                        
                        </tr>
                    </g:each>
                    </tbody>
                </table>
            </div>
            <div class="paginateButtons">
                <g:paginate total="${incomingBatchInstanceTotal}" />
            </div>
        </div>
    </body>
</html>
