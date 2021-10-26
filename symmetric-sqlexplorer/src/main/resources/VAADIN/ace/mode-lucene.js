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
ace.define("ace/mode/lucene_highlight_rules",["require","exports","module","ace/lib/oop","ace/lib/lang","ace/mode/text_highlight_rules"],function(e,t,n){"use strict";var r=e("../lib/oop"),i=e("../lib/lang"),s=e("./text_highlight_rules").TextHighlightRules,o=function(){this.$rules={start:[{token:"constant.character.negation",regex:"[\\-]"},{token:"constant.character.interro",regex:"[\\?]"},{token:"constant.character.asterisk",regex:"[\\*]"},{token:"constant.character.proximity",regex:"~[0-9]+\\b"},{token:"keyword.operator",regex:"(?:AND|OR|NOT)\\b"},{token:"paren.lparen",regex:"[\\(]"},{token:"paren.rparen",regex:"[\\)]"},{token:"keyword",regex:"[\\S]+:"},{token:"string",regex:'".*?"'},{token:"text",regex:"\\s+"}]}};r.inherits(o,s),t.LuceneHighlightRules=o}),ace.define("ace/mode/lucene",["require","exports","module","ace/lib/oop","ace/mode/text","ace/mode/lucene_highlight_rules"],function(e,t,n){"use strict";var r=e("../lib/oop"),i=e("./text").Mode,s=e("./lucene_highlight_rules").LuceneHighlightRules,o=function(){this.HighlightRules=s};r.inherits(o,i),function(){this.$id="ace/mode/lucene"}.call(o.prototype),t.Mode=o})