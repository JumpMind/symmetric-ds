/**
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
package org.jumpmind.db.model;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class DatabaseTest {

    @Test
    public void testOrderingOfFourTables() {
        
        List<Table> list = new ArrayList<Table>(4);
        Table t1 = new Table("1");
        Table t2 = new Table("2");
        Table t3 = new Table("3");
        Table t4 = new Table("4");
        
        t1.addForeignKey(new ForeignKey("2","2"));
        t2.addForeignKey(new ForeignKey("3","3"));
        t3.addForeignKey(new ForeignKey("4","4"));
        
        list.add(t2);
        list.add(t1);
        list.add(t4);
        list.add(t3);
        
        list = Database.sortByForeignKeys(list);
        
        Assert.assertTrue(list.toString(), list.indexOf(t4) < list.indexOf(t1));
        Assert.assertTrue(list.toString(), list.indexOf(t2) < list.indexOf(t1));
        Assert.assertTrue(list.toString(), list.indexOf(t3) < list.indexOf(t2));
        Assert.assertTrue(list.toString(), list.indexOf(t4) < list.indexOf(t3));
        
    }
    
    @Test
    public void testOrderingOfTenTables() {
        
        List<Table> list = new ArrayList<Table>(10);
        Table t0 = new Table("0");
        Table t1 = new Table("1");
        Table t2 = new Table("2");
        Table t3 = new Table("3");
        Table t4 = new Table("4");
        Table t5 = new Table("5");
        Table t6 = new Table("6");
        Table t7 = new Table("7");
        Table t8 = new Table("8");
        Table t9 = new Table("9");
        
        t5.addForeignKey(new ForeignKey("4","4"));
        t4.addForeignKey(new ForeignKey("3","3"));
        t3.addForeignKey(new ForeignKey("2","2"));

        list.add(t5);
        list.add(t7);
        list.add(t1);
        list.add(t4);
        list.add(t3);
        list.add(t6);
        list.add(t9);
        list.add(t0);
        list.add(t2);
        list.add(t8);
        
        
        list = Database.sortByForeignKeys(list);
        
        Assert.assertTrue(list.toString(), list.indexOf(t4) < list.indexOf(t5));
        Assert.assertTrue(list.toString(), list.indexOf(t3) < list.indexOf(t4));
        Assert.assertTrue(list.toString(), list.indexOf(t2) < list.indexOf(t3));
        Assert.assertTrue(list.toString(), list.indexOf(t2) < list.indexOf(t5));
        
    }    
    
    @Test
    public void testCyclicalReferences() {
        
        List<Table> list = new ArrayList<Table>(4);
        Table t1 = new Table("1");
        Table t2 = new Table("2");
        Table t3 = new Table("3");

        t1.addForeignKey(new ForeignKey("2","2"));
        t2.addForeignKey(new ForeignKey("3","3"));
        t3.addForeignKey(new ForeignKey("1","1"));
        
        list.add(t3);
        list.add(t2);
        list.add(t1);
        
        list = Database.sortByForeignKeys(list);
        
        // for now just make sure it doesn't blow up
        
    }
}
