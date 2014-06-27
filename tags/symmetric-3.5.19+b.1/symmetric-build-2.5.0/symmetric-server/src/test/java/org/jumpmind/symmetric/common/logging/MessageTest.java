/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */
package org.jumpmind.symmetric.common.logging;

import java.util.Locale;

import org.jumpmind.symmetric.common.Message;
import org.junit.Assert;
import org.junit.Test;

public class MessageTest {

    @Test
    public void getBasicMessages() {
        Message.setLocale(new Locale("en_US"));
        long l = 4l;
        String s = Message.get("BatchCompleting");
        Assert.assertEquals("Completing batch null", s);
        s = Message.get("BatchCompleting", l);
        Assert.assertEquals("Completing batch 4", s);

    }

    @Test
    public void getSpanishMessages() {
        Message.setLocale(new Locale("es"));
        long l = 4l;
        String s = Message.get("BatchCompleting");
        Assert.assertEquals("Realizaci—n de la hornada null", s);
        s = Message.get("BatchCompleting", l);
        Assert.assertEquals("Realizaci—n de la hornada 4", s);
    }

    @Test
    public void getBadLanguageMessages() {
        Locale.setDefault(new Locale("en_US"));
        Message.setLocale(new Locale("zz"));
        long l = 4l;
        String s = Message.get("BatchCompleting");
        Assert.assertEquals("Completing batch null", s);
        s = Message.get("BatchCompleting", l);
        Assert.assertEquals("Completing batch 4", s);

    }
}