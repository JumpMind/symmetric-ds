<div class="menu">
  <ul class="navaction" id="navaction_list">
    <li class="navaction_first">
      <g:link action="edit" id="${id}"><g:message code="edit.label" default="Edit"/></g:link>
    </li>
    <li class="navaction_last">
      <g:link action="delete" id="${id}" onclick="return confirm('${message(code: 'default.button.delete.confirm.message', default: 'Are you sure?')}');"><g:message code="delete.label" default="Delete"/></g:link>
    </li>
  </ul>
</div>