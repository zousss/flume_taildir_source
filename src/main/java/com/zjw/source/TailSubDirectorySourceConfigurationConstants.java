package com.zjw.source;

/**
 * Created by zjw on 2017/6/19.
 * 修改flume1.7 taildir 源码，增加以下功能
 * 1.修改filegroups为spooldir，监控spooldir下的所有子文件夹里面的文件。
 * 2.增加路径配置，即文件的每一层目录对应的关系。
 * 3.增加event header信息
 * 4.读取配置文件信息，和日志文件数据对应，放入event body中
 */

public class TailSubDirectorySourceConfigurationConstants {

    /** Directory where files are deposited. */
    public static final String SPOOL_DIRECTORY = "spoolDirectory";

    /*status file*/
    public static final String STATUS_FN = "status_fn";
    public static final String DEFAULT_STATUS_FN = "/.flume/status_fn.txt";

    /** Path of position file. */
    public static final String POSITION_FILE = "positionFile";
    public static final String DEFAULT_POSITION_FILE = "/.flume/taildir_position.json";

    /** What size to batch with before sending to ChannelProcessor. */
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;
    /** Whether to skip the position to EOF in the case of files not written on the position file. */
    public static final String SKIP_TO_END = "skipToEnd";
    public static final boolean DEFAULT_SKIP_TO_END = false;
    /** Time (ms) to close idle files. */
    public static final String IDLE_TIMEOUT = "idleTimeout";
    public static final int DEFAULT_IDLE_TIMEOUT = 120000;
    /** Interval time (ms) to write the last position of each file on the position file. */
    public static final String WRITE_POS_INTERVAL = "writePosInterval";
    public static final int DEFAULT_WRITE_POS_INTERVAL = 3000;
    /** Whether to add the byte offset of a tailed line to the header */
    public static final String BYTE_OFFSET_HEADER = "byteOffsetHeader";
    public static final String BYTE_OFFSET_HEADER_KEY = "byteoffset";
    public static final boolean DEFAULT_BYTE_OFFSET_HEADER = false;
    public static final String BACKOFF_SLEEP_INCREMENT = "backoffSleepIncrement";
    public static final String MAX_BACKOFF_SLEEP = "maxBackoffSleep";
    public static final long DEFAULT_BACKOFF_SLEEP_INCREMENT = 1000;
    public static final long DEFAULT_MAX_BACKOFF_SLEEP = 5000;

    //增加目录配置规则
    public static final String DIRECTORY_PATTERN = "directoryPattern";
    public static final String DEFAULT_DIRECTORY_PATTERN = "/";
}
