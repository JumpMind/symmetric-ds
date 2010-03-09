<g:if test="${session.menu?.startsWith('monitor')}">
<ul class="vertical tabs"> 
    
        <li class="<g:if test="${session.menu?.endsWith('dashboard')}">active </g:if>first"> 
            <g:link controller="dashboard" class="browse-tab"><strong>Dashboard</strong></g:link></a> 
        </li> 
    
        <li class="<g:if test="${session.menu?.endsWith('nodes')}">active</g:if>"> 
            <g:link controller="node" class="browse-tab"><strong>Nodes</strong></g:link></a> 
        </li> 
    
        <li class="<g:if test="${session.menu?.endsWith('outgoing')}">active</g:if>"> 
            <a class="browse-tab" id="roadmap-panel-panel" href="" hidefocus><strong>Outgoing</strong></a> 
        </li> 
    
        <li class="<g:if test="${session.menu?.endsWith('incoming')}">active</g:if>"> 
            <a class="browse-tab" id="changelog-panel-panel" href="" hidefocus><strong>Incoming</strong></a> 
        </li> 
</ul> 
</g:if>
<g:if test="${session.menu?.startsWith('control')}">
<ul class="vertical tabs"> 
    
        <li class="<g:if test="${session.menu?.endsWith('channels')}">active </g:if>first"> 
            <g:link controller="channel" class="browse-tab"><strong>Channels</strong></g:link></a> 
        </li> 
    
        <li class="<g:if test="${session.menu?.endsWith('jobs')}">active</g:if>"> 
            <g:link controller="node" class="browse-tab"><strong>Jobs</strong></g:link></a> 
        </li> 
    
        <li class="<g:if test="${session.menu?.endsWith('settings')}">active</g:if>"> 
            <a class="browse-tab" id="roadmap-panel-panel" href="" hidefocus><strong>Settings</strong></a> 
        </li> 
</ul> 
</g:if>
<g:if test="${session.menu?.startsWith('configuration')}">
<ul class="vertical tabs"> 
    
        <li class="<g:if test="${session.menu?.endsWith('nodeGroups')}">active </g:if>first"> 
            <g:link controller="nodeGroup" class="browse-tab"><strong>Node Groups</strong></g:link></a> 
        </li> 
    
        <li class="<g:if test="${session.menu?.endsWith('nodeGroupLink')}">active</g:if>"> 
            <g:link controller="nodeGroupLink" class="browse-tab"><strong>Links</strong></g:link></a> 
        </li> 
    
        <li class="<g:if test="${session.menu?.endsWith('routers')}">active</g:if>"> 
            <g:link controller="router" class="browse-tab"><strong>Routers</strong></g:link></a> 
        </li> 
        
        <li class="<g:if test="${session.menu?.endsWith('triggers')}">active</g:if>"> 
            <g:link controller="trigger" class="browse-tab"><strong>Triggers</strong></g:link></a> 
        </li> 
</ul> 
</g:if>

