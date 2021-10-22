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
ace.define("ace/snippets/io",["require","exports","module"],function(e,t,n){"use strict";t.snippets=[{content:"assertEquals(${1:expected}, ${2:expr})",name:"assertEquals",scope:"io",tabTrigger:"ae"},{content:"${1:${2:newValue} := ${3:Object} }clone do(\n	$0\n)",name:"clone do",scope:"io",tabTrigger:"cdo"},{content:'docSlot("${1:slotName}", "${2:documentation}")',name:"docSlot",scope:"io",tabTrigger:"ds"},{content:"(${1:header,}\n	${2:body}\n)$0",keyEquivalent:"@(",name:"Indented Bracketed Line",scope:"io",tabTrigger:"("},{content:"\n	$0\n",keyEquivalent:"\r",name:"Special: Return Inside Empty Parenthesis",scope:"io meta.empty-parenthesis.io, io meta.comma-parenthesis.io"},{content:"${1:methodName} := method(${2:args,}\n	$0\n)",name:"method",scope:"io",tabTrigger:"m"},{content:'newSlot("${1:slotName}", ${2:defaultValue}, "${3:docString}")$0',name:"newSlot",scope:"io",tabTrigger:"ns"},{content:"${1:name} := Object clone do(\n	$0\n)",name:"Object clone do",scope:"io",tabTrigger:"ocdo"},{content:"test${1:SomeFeature} := method(\n	$0\n)",name:"testMethod",scope:"io",tabTrigger:"ts"},{content:"${1:Something}Test := ${2:UnitTest} clone do(\n	$0\n)",name:"UnitTest",scope:"io",tabTrigger:"ut"}],t.scope="io"})