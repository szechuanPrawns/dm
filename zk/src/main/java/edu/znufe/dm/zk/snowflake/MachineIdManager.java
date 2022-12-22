package edu.znufe.dm.zk.snowflake;

public interface MachineIdManager {
    /**
     * init：
     *  1. get permanent sequential node % 1024
     *  2.
     * 查询 ephemeral node里machineId
     * 判断时钟未大范围回拨
     * @return
     */
    String getMachineId();
}
