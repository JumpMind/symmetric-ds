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
ace.define("ace/mode/ini_highlight_rules",["require","exports","module","ace/lib/oop","ace/mode/text_highlight_rules"],function(e,t,n){"use strict";var r=e("../lib/oop"),i=e("./text_highlight_rules").TextHighlightRules,s="\\\\(?:[\\\\0abtrn;#=:]|x[a-fA-F\\d]{4})",o=function(){this.$rules={start:[{token:"punctuation.definition.comment.ini",regex:"#.*",push_:[{token:"comment.line.number-sign.ini",regex:"$|^",next:"pop"},{defaultToken:"comment.line.number-sign.ini"}]},{token:"punctuation.definition.comment.ini",regex:";.*",push_:[{token:"comment.line.semicolon.ini",regex:"$|^",next:"pop"},{defaultToken:"comment.line.semicolon.ini"}]},{token:["keyword.other.definition.ini","text","punctuation.separator.key-value.ini"],regex:"\\b([a-zA-Z0-9_.-]+)\\b(\\s*)(=)"},{token:["punctuation.definition.entity.ini","constant.section.group-title.ini","punctuation.definition.entity.ini"],regex:"^(\\[)(.*?)(\\])"},{token:"punctuation.definition.string.begin.ini",regex:"'",push:[{token:"punctuation.definition.string.end.ini",regex:"'",next:"pop"},{token:"constant.language.escape",regex:s},{defaultToken:"string.quoted.single.ini"}]},{token:"punctuation.definition.string.begin.ini",regex:'"',push:[{token:"constant.language.escape",regex:s},{token:"punctuation.definition.string.end.ini",regex:'"',next:"pop"},{defaultToken:"string.quoted.double.ini"}]}]},this.normalizeRules()};o.metaData={fileTypes:["ini","conf"],keyEquivalent:"^~I",name:"Ini",scopeName:"source.ini"},r.inherits(o,i),t.IniHighlightRules=o}),ace.define("ace/mode/folding/ini",["require","exports","module","ace/lib/oop","ace/range","ace/mode/folding/fold_mode"],function(e,t,n){"use strict";var r=e("../../lib/oop"),i=e("../../range").Range,s=e("./fold_mode").FoldMode,o=t.FoldMode=function(){};r.inherits(o,s),function(){this.foldingStartMarker=/^\s*\[([^\])]*)]\s*(?:$|[;#])/,this.getFoldWidgetRange=function(e,t,n){var r=this.foldingStartMarker,s=e.getLine(n),o=s.match(r);if(!o)return;var u=o[1]+".",a=s.length,f=e.getLength(),l=n,c=n;while(++n<f){s=e.getLine(n);if(/^\s*$/.test(s))continue;o=s.match(r);if(o&&o[1].lastIndexOf(u,0)!==0)break;c=n}if(c>l){var h=e.getLine(c).length;return new i(l,a,c,h)}}}.call(o.prototype)}),ace.define("ace/mode/ini",["require","exports","module","ace/lib/oop","ace/mode/text","ace/mode/ini_highlight_rules","ace/mode/folding/ini"],function(e,t,n){"use strict";var r=e("../lib/oop"),i=e("./text").Mode,s=e("./ini_highlight_rules").IniHighlightRules,o=e("./folding/ini").FoldMode,u=function(){this.HighlightRules=s,this.foldingRules=new o};r.inherits(u,i),function(){this.lineCommentStart=";",this.blockComment={start:"/*",end:"*/"},this.$id="ace/mode/ini"}.call(u.prototype),t.Mode=u})