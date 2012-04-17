package org.jumpmind.db.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class SortByForeignKeyComparatorTest {

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
        
        Collections.sort(list,new SortByForeignKeyComparator());
        
        Assert.assertEquals(t1, list.get(0));
        Assert.assertEquals(t2, list.get(1));
        Assert.assertEquals(t3, list.get(2));
        Assert.assertEquals(t4, list.get(3));
        
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
        
        Collections.sort(list,new SortByForeignKeyComparator());
        
        // for now just make sure it doesn't blow up
        
    }
}
