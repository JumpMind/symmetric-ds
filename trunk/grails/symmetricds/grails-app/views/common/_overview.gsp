<div id="overview">
    <span class="meta-first">
        <span class="label"><g:message code="symmetric.node.id.label" default="My Node"/></span>
        <span class="value"><g:link controller="node" action="show" id="${session.overview?.nodeId}">${session.overview?.nodeId}</g:link></span>
    </span>   
    <span class="meta">
        <span class="label"><g:message code="symmetric.status.label" default="Status"/></span>
        <span class="value"><g:if test="${session.overview?.started}">
                <g:message code="symmetric.running.label" default="Running"/>
              </g:if>
              <g:else>
                <g:message code="symmetric.running.label" default="Stopped"/>
              </g:else></span>
    </span>  
    <span class="meta">
        <span class="label"><g:message code="symmetric.node.group.label" default="Group"/></span>
        <span class="value"><g:link controller="nodeGroup" action="show" id="${session.overview?.nodeGroupId}">${session.overview?.nodeGroupId}</g:link></span>
    </span> 
    <span class="meta">
        <span class="label"><g:message code="symmetric.number.of.nodes.label" default="# Nodes"/></span>
        <span class="value"><g:link controller="node" action="list">${session.overview?.numberOfNodes}</g:link></span>
    </span> 
    <span class="meta">
        <span class="label"><g:message code="symmetric.number.of.nodes.label" default="# Clients"/></span>
        <span class="value"><g:link controller="node" action="list">${session.overview?.numberOfClients}</g:link></span>
    </span>
</div>