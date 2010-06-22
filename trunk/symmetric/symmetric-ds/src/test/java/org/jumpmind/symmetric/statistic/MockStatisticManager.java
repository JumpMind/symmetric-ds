package org.jumpmind.symmetric.statistic;

public class MockStatisticManager implements IStatisticManager {

    public void flush() {
    }

    public Statistic getStatistic(String name) {
        return new Statistic(name,"some pig");
    }

}
