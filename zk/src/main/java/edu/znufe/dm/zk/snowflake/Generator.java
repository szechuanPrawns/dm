package edu.znufe.dm.zk.snowflake;

public interface Generator {

    /**
     * millisecond + machineId + sequence
     *
     * @return
     */
    String nextVal();
}
