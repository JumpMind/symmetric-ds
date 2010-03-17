
<%@ page import="org.jumpmind.symmetric.grails.IncomingBatch" %>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="layout" content="main" />
        <g:set var="entityName" value="${message(code: 'incomingBatch.label', default: 'IncomingBatch')}" />
        <title><g:message code="default.create.label" args="[entityName]" /></title>
    </head>
    <body>
        <div class="nav">
            <span class="menuButton"><a class="home" href="${createLink(uri: '/')}">Home</a></span>
            <span class="menuButton"><g:link class="list" action="list"><g:message code="default.list.label" args="[entityName]" /></g:link></span>
        </div>
        <div class="body">
            <h1><g:message code="default.create.label" args="[entityName]" /></h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <g:hasErrors bean="${incomingBatchInstance}">
            <div class="errors">
                <g:renderErrors bean="${incomingBatchInstance}" as="list" />
            </div>
            </g:hasErrors>
            <g:form action="save" method="post" >
                <div class="dialog">
                    <table>
                        <tbody>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="persistable"><g:message code="incomingBatch.persistable.label" default="Persistable" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'persistable', 'errors')}">
                                    <g:checkBox name="persistable" value="${incomingBatchInstance?.persistable}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="createTime"><g:message code="incomingBatch.createTime.label" default="Create Time" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'createTime', 'errors')}">
                                    <g:datePicker name="createTime" precision="day" value="${incomingBatchInstance?.createTime}"  />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="sqlCode"><g:message code="incomingBatch.sqlCode.label" default="Sql Code" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'sqlCode', 'errors')}">
                                    <g:textField name="sqlCode" value="${fieldValue(bean: incomingBatchInstance, field: 'sqlCode')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="filterMillis"><g:message code="incomingBatch.filterMillis.label" default="Filter Millis" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'filterMillis', 'errors')}">
                                    <g:textField name="filterMillis" value="${fieldValue(bean: incomingBatchInstance, field: 'filterMillis')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="batchId"><g:message code="incomingBatch.batchId.label" default="Batch Id" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'batchId', 'errors')}">
                                    <g:textField name="batchId" value="${fieldValue(bean: incomingBatchInstance, field: 'batchId')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="statementCount"><g:message code="incomingBatch.statementCount.label" default="Statement Count" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'statementCount', 'errors')}">
                                    <g:textField name="statementCount" value="${fieldValue(bean: incomingBatchInstance, field: 'statementCount')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="nodeId"><g:message code="incomingBatch.nodeId.label" default="Node Id" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'nodeId', 'errors')}">
                                    <g:textField name="nodeId" value="${incomingBatchInstance?.nodeId}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="byteCount"><g:message code="incomingBatch.byteCount.label" default="Byte Count" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'byteCount', 'errors')}">
                                    <g:textField name="byteCount" value="${fieldValue(bean: incomingBatchInstance, field: 'byteCount')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="databaseMillis"><g:message code="incomingBatch.databaseMillis.label" default="Database Millis" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'databaseMillis', 'errors')}">
                                    <g:textField name="databaseMillis" value="${fieldValue(bean: incomingBatchInstance, field: 'databaseMillis')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="nodeBatchId"><g:message code="incomingBatch.nodeBatchId.label" default="Node Batch Id" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'nodeBatchId', 'errors')}">
                                    <g:textField name="nodeBatchId" value="${incomingBatchInstance?.nodeBatchId}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="failedRowNumber"><g:message code="incomingBatch.failedRowNumber.label" default="Failed Row Number" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'failedRowNumber', 'errors')}">
                                    <g:textField name="failedRowNumber" value="${fieldValue(bean: incomingBatchInstance, field: 'failedRowNumber')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="lastUpdatedHostName"><g:message code="incomingBatch.lastUpdatedHostName.label" default="Last Updated Host Name" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'lastUpdatedHostName', 'errors')}">
                                    <g:textField name="lastUpdatedHostName" value="${incomingBatchInstance?.lastUpdatedHostName}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="status"><g:message code="incomingBatch.status.label" default="Status" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'status', 'errors')}">
                                    <g:select name="status" from="${org.jumpmind.symmetric.model.IncomingBatch$Status?.values()}" value="${incomingBatchInstance?.status}"  />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="channelId"><g:message code="incomingBatch.channelId.label" default="Channel Id" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'channelId', 'errors')}">
                                    <g:textField name="channelId" value="${incomingBatchInstance?.channelId}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="sqlState"><g:message code="incomingBatch.sqlState.label" default="Sql State" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'sqlState', 'errors')}">
                                    <g:textField name="sqlState" value="${incomingBatchInstance?.sqlState}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="skipCount"><g:message code="incomingBatch.skipCount.label" default="Skip Count" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'skipCount', 'errors')}">
                                    <g:textField name="skipCount" value="${fieldValue(bean: incomingBatchInstance, field: 'skipCount')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="sqlMessage"><g:message code="incomingBatch.sqlMessage.label" default="Sql Message" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'sqlMessage', 'errors')}">
                                    <g:textField name="sqlMessage" value="${incomingBatchInstance?.sqlMessage}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="lastUpdatedTime"><g:message code="incomingBatch.lastUpdatedTime.label" default="Last Updated Time" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'lastUpdatedTime', 'errors')}">
                                    <g:datePicker name="lastUpdatedTime" precision="day" value="${incomingBatchInstance?.lastUpdatedTime}"  />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="fallbackInsertCount"><g:message code="incomingBatch.fallbackInsertCount.label" default="Fallback Insert Count" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'fallbackInsertCount', 'errors')}">
                                    <g:textField name="fallbackInsertCount" value="${fieldValue(bean: incomingBatchInstance, field: 'fallbackInsertCount')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="fallbackUpdateCount"><g:message code="incomingBatch.fallbackUpdateCount.label" default="Fallback Update Count" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'fallbackUpdateCount', 'errors')}">
                                    <g:textField name="fallbackUpdateCount" value="${fieldValue(bean: incomingBatchInstance, field: 'fallbackUpdateCount')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="networkMillis"><g:message code="incomingBatch.networkMillis.label" default="Network Millis" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'networkMillis', 'errors')}">
                                    <g:textField name="networkMillis" value="${fieldValue(bean: incomingBatchInstance, field: 'networkMillis')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="missingDeleteCount"><g:message code="incomingBatch.missingDeleteCount.label" default="Missing Delete Count" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'missingDeleteCount', 'errors')}">
                                    <g:textField name="missingDeleteCount" value="${fieldValue(bean: incomingBatchInstance, field: 'missingDeleteCount')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="retry"><g:message code="incomingBatch.retry.label" default="Retry" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: incomingBatchInstance, field: 'retry', 'errors')}">
                                    <g:checkBox name="retry" value="${incomingBatchInstance?.retry}" />
                                </td>
                            </tr>
                        
                        </tbody>
                    </table>
                </div>
                <div class="buttons">
                    <span class="button"><g:submitButton name="create" class="save" value="${message(code: 'default.button.create.label', default: 'Create')}" /></span>
                </div>
            </g:form>
        </div>
    </body>
</html>
