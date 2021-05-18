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
ace.define("ace/snippets/abc",["require","exports","module"],function(e,t,n){"use strict";t.snippetText='\nsnippet zupfnoter.print\n	%%%%hn.print {"startpos": ${1:pos_y}, "t":"${2:title}", "v":[${3:voices}], "s":[[${4:syncvoices}1,2]], "f":[${5:flowlines}],  "sf":[${6:subflowlines}], "j":[${7:jumplines}]}\n\nsnippet zupfnoter.note\n	%%%%hn.note {"pos": [${1:pos_x},${2:pos_y}], "text": "${3:text}", "style": "${4:style}"}\n\nsnippet zupfnoter.annotation\n	%%%%hn.annotation {"id": "${1:id}", "pos": [${2:pos}], "text": "${3:text}"}\n\nsnippet zupfnoter.lyrics\n	%%%%hn.lyrics {"pos": [${1:x_pos},${2:y_pos}]}\n\nsnippet zupfnoter.legend\n	%%%%hn.legend {"pos": [${1:x_pos},${2:y_pos}]}\n\n\n\nsnippet zupfnoter.target\n	"^:${1:target}"\n\nsnippet zupfnoter.goto\n	"^@${1:target}@${2:distance}"\n\nsnippet zupfnoter.annotationref\n	"^#${1:target}"\n\nsnippet zupfnoter.annotation\n	"^!${1:text}@${2:x_offset},${3:y_offset}"\n\n\n',t.scope="abc"})