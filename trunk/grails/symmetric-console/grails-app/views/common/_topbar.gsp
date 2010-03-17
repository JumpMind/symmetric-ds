<div id="logo">
    <a href="http://symmetricds.org"><img id="logoImage" src="${resource(dir: 'images', file: 'logo-text.png')}" alt="SymmetricDS" border="0" height="48"/></a>
</div>

<div id="quicklinks">
  <g:link controller='sync' action='alert'><img src="${resource(dir: 'images', file: 'rss.png')}" border="0"/></g:link>
  <g:link url="${resource(plugin:'symmetricds', dir: '/symmetricds-doc/html-single', file: 'user-guide.html')}"><img src="${resource(dir: 'images', file: 'book_open.png')}" border="0"/></g:link>  
</div>


<div id="user">
  Welcome Ebenezer Scrooge | <a href="#">logout</a>
</div>

<div class="clearie" >&nbsp;</div>

<div id="menu">
    <ul class="menulinks">
        <li <g:if test="${session.menu?.startsWith('monitor')}">class="selected"</g:if>><g:link controller="dashboard">monitor</g:link></li>
        <li <g:if test="${session.menu?.startsWith('control')}">class="selected"</g:if>><g:link controller="channel">control</g:link></li>
        <li <g:if test="${session.menu?.startsWith('configuration')}">class="selected"</g:if>><g:link controller="nodeGroup">configuration</g:link></li>
        <li <g:if test="${session.menu?.startsWith('utilities')}">class="selected"</g:if>><g:link controller="db">utilities</g:link></li>
    </ul>
</div>