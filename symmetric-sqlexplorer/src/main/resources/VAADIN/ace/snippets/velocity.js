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
ace.define("ace/snippets/velocity",["require","exports","module"],function(e,t,n){"use strict";t.snippetText='# macro\nsnippet #macro\n	#macro ( ${1:macroName} ${2:\\$var1, [\\$var2, ...]} )\n		${3:## macro code}\n	#end\n# foreach\nsnippet #foreach\n	#foreach ( ${1:\\$item} in ${2:\\$collection} )\n		${3:## foreach code}\n	#end\n# if\nsnippet #if\n	#if ( ${1:true} )\n		${0}\n	#end\n# if ... else\nsnippet #ife\n	#if ( ${1:true} )\n		${2}\n	#else\n		${0}\n	#end\n#import\nsnippet #import\n	#import ( "${1:path/to/velocity/format}" )\n# set\nsnippet #set\n	#set ( $${1:var} = ${0} )\n',t.scope="velocity",t.includeScopes=["html","javascript","css"]})