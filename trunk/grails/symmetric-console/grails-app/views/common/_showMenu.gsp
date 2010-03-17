<div class="menu">
  <ul class="navaction" id="navaction_list">
    <li class="navitem">
      <g:link class="navedit" action="edit" id="${id}"><g:message code="edit.label" default="Edit"/></g:link>
    </li>
    <li class="navitem">
      <g:link class="navdelete" action="delete" id="${id}" onclick="return confirm('${message(code: 'default.button.delete.confirm.message', default: 'Are you sure?')}');"><g:message code="delete.label" default="Delete"/></g:link>
    </li>
  </ul>
</div>