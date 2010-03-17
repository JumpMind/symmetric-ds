
<%@ page import="org.jumpmind.symmetric.grails.DataEvent" %>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="layout" content="main" />
        <g:set var="entityName" value="${message(code: 'data.label', default: 'Data')}" />
        <title><g:message code="default.list.label" args="[entityName]" /></title>
    </head>
    <body>
        <div class="body">
            <h1><g:message code="default.list.label" args="[entityName]" /></h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <div class="list">
                <table class="list">
                    <thead>
                        <tr>
                            <g:sortableColumn property="dataDataId" title="${message(code: 'data.id.label', default: 'Data Id')}" />
                            <g:sortableColumn property="dataTableName" title="${message(code: 'data.channelId.label', default: 'Channel Id')}" />
                            <g:sortableColumn property="dataEventType" title="${message(code: 'data.createTime.label', default: 'Create Time')}" />
                            <g:sortableColumn property="dataTableName" title="${message(code: 'data.tableName.label', default: 'Table Name')}" />
                            <g:sortableColumn property="dataEventType" title="${message(code: 'data.eventType.label', default: 'Event Type')}" />
                            <g:sortableColumn property="dataEventType" title="${message(code: 'data.transactionId.label', default: 'Transaction Id')}" />
                            <g:sortableColumn property="dataEventType" title="${message(code: 'data.sourceNode.label', default: 'Source Node')}" />
                            <g:sortableColumn property="dataEventType" title="${message(code: 'data.externalData.label', default: 'External Data')}" />
                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${dataEventInstanceList}" status="index" var="dataEventInstance">
                        <tr class="${(index % 2) == 0 ? 'odd' : 'even'}">
                            <td>
                                
                                <div onmouseover="showPopup(this);" onmouseout="hidePopup(this);" id="popup-container">
                                    <span><g:link controller="data" action="show" id="${dataEventInstance?.data?.dataId}">${dataEventInstance?.data?.dataId}</g:link></span>
                                
                                    <div class="data-panel popup" >
                                    <table class="data-table-popup">
                                        <%
                                        columns = dataEventInstance?.data?.triggerHist?.columnNames?.split(",") 
                                        old = dataEventInstance?.data?.toParsedOldData() 
                                        rows = dataEventInstance?.data?.toParsedRowData()
                                        pk = dataEventInstance?.data?.toParsedPkData()
                                        %>
                                        <tr>
                                            <td colspan="${columns?.size() == null ? 1 : columns.size() + 1}" class="data-table-name">${dataEventInstance?.data?.tableName}&nbsp;(${dataEventInstance?.data?.eventType?.encodeAsHTML()})</span></td>
                                        </tr>
                                        <tr class="data-table-columns">
                                               <td class="data-column data-label">&nbsp;</td>
                                            <g:each in="${columns}" var="c" status="i">
                                                <td class="data-coulumn${(i % 2) == 0 ? ' alt' : ''}">${c}</td>
                                            </g:each>
                                        </tr><tr>
                                                <td class="data-column data-label">PK</td>
                                             <%pkCounter = 1%>
                                            <g:each in="${pk}" var="p" status="i">
                                                <%pkCounter = i%>
                                                <td class="data-coulumn${(i % 2) == 0 ? ' alt' : ''}">${p}</td>
                                            </g:each>
                                            <%pkLength = pk != null ? pk.length : 0 %>
                                            <g:while test="${pkLength < columns?.length}">
                                                <%pkLength++
                                                pkCounter++%>
                                                <td class="data-coulumn${(pkCounter % 2) == 0 ? ' alt' : ''}">&nbsp;</td>
                                            </g:while>
                                        </tr><tr>
                                                <td class="data-column data-label">New</td>
                                            <g:each in="${rows}" var="r" status="i">
                                                <td class="data-coulumn${(i % 2) == 0 ? ' alt' : ''}${((old == null || rows[i] != old[i]) && dataEventInstance?.data?.eventType?.encodeAsHTML() == "UPDATE") ? ' diff' : ''}">${r}</td>
                                            </g:each>
                                            <%rowsLength = rows != null ? rows.length : 0 %>
                                            <g:while test="${rowsLength < columns?.length}">
                                                <td class="data-coulumn${(rowsLength % 2) == 0 ? ' alt' : ''}">&nbsp;</td>
                                                <%rowsLength++%>
                                            </g:while>
                                        </tr><tr>
                                                <td class="data-column data-label">Old</td>
                                            <g:each in="${old}" var="o" status="i">
                                                <td class="data-coulumn${(i % 2) == 0 ? ' alt' : ''}">${o}</td>
                                            </g:each>
                                            <%oldLength = old != null ? old.length : 0 %>
                                            <g:while test="${oldLength < columns?.length}">
                                                <td class="data-coulumn${(oldLength % 2) == 0 ? ' alt' : ''}">&nbsp;</td>
                                                <%oldLength++%>
                                            </g:while>
                                        </tr>
                                    </table>
                                </div>
                            </div>
                     

                            </td>
                            <td><g:link controller="channel" action="show" id="${dataEventInstance?.data?.channelId}">${dataEventInstance?.data?.channelId}</g:link></td>
                            <td>${dataEventInstance?.data?.createTime}</td>
                            <td>${dataEventInstance?.data?.tableName}</td>
                            <td>${dataEventInstance?.data?.eventType}</td>
                            <td>${dataEventInstance?.data?.transactionId}</td>
                            <td>${dataEventInstance?.data?.sourceNodeId}</td>
                            <td>${dataEventInstance?.data?.externalData}</td>                            
                        </tr>
                    </g:each>
                    </tbody>
                </table>
            </div>
        </div>
    </body>
</html>
