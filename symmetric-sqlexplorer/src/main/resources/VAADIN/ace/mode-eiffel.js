/*
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
ace.define("ace/mode/eiffel_highlight_rules",["require","exports","module","ace/lib/oop","ace/mode/text_highlight_rules"],function(e,t,n){"use strict";var r=e("../lib/oop"),i=e("./text_highlight_rules").TextHighlightRules,s=function(){var e="across|agent|alias|all|attached|as|assign|attribute|check|class|convert|create|debug|deferred|detachable|do|else|elseif|end|ensure|expanded|export|external|feature|from|frozen|if|inherit|inspect|invariant|like|local|loop|not|note|obsolete|old|once|Precursor|redefine|rename|require|rescue|retry|select|separate|some|then|undefine|until|variant|when",t="and|implies|or|xor",n="Void",r="True|False",i="Current|Result",s=this.createKeywordMapper({"constant.language":n,"constant.language.boolean":r,"variable.language":i,"keyword.operator":t,keyword:e},"identifier",!0),o=/(?:[^"%\b\f\v]|%[A-DFHLNQR-V%'"()<>]|%\/(?:0[xX][\da-fA-F](?:_*[\da-fA-F])*|0[cC][0-7](?:_*[0-7])*|0[bB][01](?:_*[01])*|\d(?:_*\d)*)\/)+?/;this.$rules={start:[{token:"string.quoted.other",regex:/"\[/,next:"aligned_verbatim_string"},{token:"string.quoted.other",regex:/"\{/,next:"non-aligned_verbatim_string"},{token:"string.quoted.double",regex:/"(?:[^%\b\f\n\r\v]|%[A-DFHLNQR-V%'"()<>]|%\/(?:0[xX][\da-fA-F](?:_*[\da-fA-F])*|0[cC][0-7](?:_*[0-7])*|0[bB][01](?:_*[01])*|\d(?:_*\d)*)\/)*?"/},{token:"comment.line.double-dash",regex:/--.*/},{token:"constant.character",regex:/'(?:[^%\b\f\n\r\t\v]|%[A-DFHLNQR-V%'"()<>]|%\/(?:0[xX][\da-fA-F](?:_*[\da-fA-F])*|0[cC][0-7](?:_*[0-7])*|0[bB][01](?:_*[01])*|\d(?:_*\d)*)\/)'/},{token:"constant.numeric",regex:/\b0(?:[xX][\da-fA-F](?:_*[\da-fA-F])*|[cC][0-7](?:_*[0-7])*|[bB][01](?:_*[01])*)\b/},{token:"constant.numeric",regex:/(?:\d(?:_*\d)*)?\.(?:(?:\d(?:_*\d)*)?[eE][+-]?)?\d(?:_*\d)*|\d(?:_*\d)*\.?/},{token:"paren.lparen",regex:/[\[({]|<<|\|\(/},{token:"paren.rparen",regex:/[\])}]|>>|\|\)/},{token:"keyword.operator",regex:/:=|->|\.(?=\w)|[;,:?]/},{token:"keyword.operator",regex:/\\\\|\|\.\.\||\.\.|\/[~\/]?|[><\/]=?|[-+*^=~]/},{token:function(e){var t=s(e);return t==="identifier"&&e===e.toUpperCase()&&(t="entity.name.type"),t},regex:/[a-zA-Z][a-zA-Z\d_]*\b/},{token:"text",regex:/\s+/}],aligned_verbatim_string:[{token:"string",regex:/]"/,next:"start"},{token:"string",regex:o}],"non-aligned_verbatim_string":[{token:"string.quoted.other",regex:/}"/,next:"start"},{token:"string.quoted.other",regex:o}]}};r.inherits(s,i),t.EiffelHighlightRules=s}),ace.define("ace/mode/eiffel",["require","exports","module","ace/lib/oop","ace/mode/text","ace/mode/eiffel_highlight_rules","ace/range"],function(e,t,n){"use strict";var r=e("../lib/oop"),i=e("./text").Mode,s=e("./eiffel_highlight_rules").EiffelHighlightRules,o=e("../range").Range,u=function(){this.HighlightRules=s};r.inherits(u,i),function(){this.lineCommentStart="--",this.$id="ace/mode/eiffel"}.call(u.prototype),t.Mode=u})