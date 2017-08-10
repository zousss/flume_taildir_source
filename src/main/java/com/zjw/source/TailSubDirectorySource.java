package com.zjw.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Table;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.zjw.source.TailSubDirectorySourceConfigurationConstants.*;

public class TailSubDirectorySource extends AbstractSource implements
        PollableSource, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(TailSubDirectorySource.class);

    /*config params*/
    private String spoolDirectory;
    private String directoryPattern;
    private Map<String, String> filePaths;
    private Table<String, String, String> headerTable;
    private int batchSize;
    private String positionFilePath;
    private boolean skipToEnd;
    private boolean byteOffsetHeader;

    /*rfeader params*/
    private SourceCounter sourceCounter;
    private ReliableTaildirEventReader reader;
    private ScheduledExecutorService idleFileChecker;
    private ScheduledExecutorService positionWriter;
    private int retryInterval = 1000;
    private int maxRetryInterval = 5000;
    private int idleTimeout;
    private int checkIdleInterval = 5000;
    private int writePosInitDelay = 5000;
    private int writePosInterval;

    private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();
    private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();
    private Long backoffSleepIncrement;
    private Long maxBackOffSleepInterval;

    private Map<String, String> last_pos_map = new HashMap<String, String>();
    private static final String COMMA = ",";
    private static final String EMPTY = "";

    private File status_file;
    Writer status_writer;
    Reader status_reader;
    private String status_fn;

    @Override
    public synchronized void start() {
        logger.info("{} TaildirSource source starting with directory: {}", getName(), spoolDirectory);
        File directory = new File(spoolDirectory);
        try {
            reader = new ReliableTaildirEventReader.Builder()
                    .spoolDirectory(directory)
                    .positionFilePath(positionFilePath)
                    .skipToEnd(skipToEnd)
                    .addByteOffset(byteOffsetHeader)
                    .directoryPattern(directoryPattern)
                    .build();
        } catch (IOException e) {
            throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
        }

        super.start();
        logger.debug("TaildirSource started");
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        try {
            super.stop();
            ExecutorService[] services = {idleFileChecker, positionWriter};
            for (ExecutorService service : services) {
                service.shutdown();
                if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
                    service.shutdownNow();
                }
            }
            // write the last position
            reader.close();
        } catch (InterruptedException e) {
            logger.info("Interrupted while awaiting termination", e);
        } catch (IOException e) {
            logger.info("Failed: " + e.getMessage(), e);
        }
        sourceCounter.stop();
        logger.info("Taildir source {} stopped. Metrics: {}", getName(), sourceCounter);
    }

    @Override
    public String toString() {
        return String.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
                        + "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }",
                positionFilePath, skipToEnd, byteOffsetHeader, idleTimeout, writePosInterval);
    }


    public synchronized void configure(Context context) {

        spoolDirectory = context.getString(SPOOL_DIRECTORY);
        Preconditions.checkState(spoolDirectory != null, "Configuration must specify a spooling directory");

        status_fn = context.getString(STATUS_FN);
        status_file = new File(status_fn);
        directoryPattern = context.getString(DIRECTORY_PATTERN,DEFAULT_DIRECTORY_PATTERN);
        String homePath = System.getProperty("user.home").replace('\\', '/');
        positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
        byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);
        idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
        writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);

        backoffSleepIncrement = context.getLong(BACKOFF_SLEEP_INCREMENT
                , DEFAULT_BACKOFF_SLEEP_INCREMENT);
        maxBackOffSleepInterval = context.getLong(MAX_BACKOFF_SLEEP
                , DEFAULT_MAX_BACKOFF_SLEEP);

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
        return sourceCounter;
    }

    public Status process() {
        Status status = Status.READY;
        try {
            /*添加待监控的文件*/
            //读取status_file,获取该文件夹下最后读取的文件
            //获取到所有文件夹，循环读取文件夹下的内容
            List<String> directories = reader.getMatchDirectories(new File(spoolDirectory));
            for (String tailDirectory : directories){
                //logger.info("******tailDirectory******** {}",tailDirectory);
                String file_loc = getLastPos(tailDirectory);
                if (file_loc.equals("")){
                    //如果位置文件是空的，则从头开始读
                    existingInodes.clear();
                    existingInodes.addAll(reader.updateTailFiles(tailDirectory,""));
                }else{
                    //如果位置文件中记录的偏移量信息，则从记录的地方开始读
                    String file_name = file_loc.split(COMMA)[0];
                    existingInodes.clear();
                    existingInodes.addAll(reader.updateTailFiles(tailDirectory,file_name));
                }
                //logger.info("********existingInodes******** {}",existingInodes.toString());
                for (long inode : existingInodes) {
                    TailFile tf = reader.getTailFiles().get(inode);
                    //logger.info("**********Inodes********** {}",inode);
                    //logger.info("**********getTailFiles********** {}",reader.getTailFiles().toString());
                    //如果文件日期大于记录文件中的日志,则读取文件内容
                    //logger.info("******Compare File******** {}",tf.toString());
                    String parentDir = tf.getPath().substring(0, tf.getPath().lastIndexOf(File.separator));
                    String fileName = tf.getPath().substring(tf.getPath().lastIndexOf(File.separator));
                    if (tf.getPath().compareToIgnoreCase((tailDirectory+file_loc))> 0) {
                        //logger.info("******Compare File******** {}",tf.getPath());
                        /*读取文件生成events*/
                        tf.setLine_pos(0L);
                        tailFileProcess(tf,true);
                        //记录该文件夹下读到的最后一个文件的位置
                        //logger.info("******Path {},FileName {},{}*******",tf.getPath(),tf.getPos());
                        String fileLoc = fileName + COMMA + tf.getLine_pos();
                        setLastPos(parentDir,fileLoc);
                    }
                    //如果记录文件等于日志文件，则继续判断大小
                    if (tf.getPath().compareToIgnoreCase((tailDirectory+file_loc.split(",")[0]))== 0) {

                        tf.setLine_pos(Long.parseLong(file_loc.split(",")[1]));
                        tailFileProcess(tf,false);
                        String fileLoc = fileName + COMMA + tf.getLine_pos();
                        setLastPos(parentDir,fileLoc);
                    }
                }
            }
            try {
                TimeUnit.MILLISECONDS.sleep(retryInterval);
            } catch (InterruptedException e) {
                logger.info("Interrupted while sleeping");
            }
        } catch (Throwable t) {
            logger.error("Unable to tail files", t);
            status = Status.BACKOFF;
        }
        return status;
    }

    /*读取文件生成events*/
    private void tailFileProcess(TailFile tf, boolean backoffWithoutNL)
            throws IOException, InterruptedException {
        while (true) {
            reader.setCurrentFile(tf);
            //logger.info("******FilePath******** {}",tf.getPath());
            List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
            //logger.info("-----events---- {}", events.size());
            if (events.isEmpty()) {
                break;
            }
            sourceCounter.addToEventReceivedCount(events.size());
            sourceCounter.incrementAppendBatchReceivedCount();
            try {
                getChannelProcessor().processEventBatch(events);
                reader.commit();
            } catch (ChannelException ex) {
                logger.warn("The channel is full or unexpected failure. " +
                        "The source will try again after " + retryInterval + " ms");
                TimeUnit.MILLISECONDS.sleep(retryInterval);
                retryInterval = retryInterval << 1;
                retryInterval = Math.min(retryInterval, maxRetryInterval);
                continue;
            }
            retryInterval = 1000;
            sourceCounter.addToEventAcceptedCount(events.size());
            sourceCounter.incrementAppendBatchAcceptedCount();
            if (events.size() < batchSize) {
                break;
            }
        }
    }

    //20170807写入信息到状态文件
    public boolean setLastPos(String dir, String v) {

        //放到map中
        last_pos_map.put(dir,v);
        //写入到文件
        try {
            status_writer = new FileWriter(status_file);

            Iterator<String> it = last_pos_map.keySet().iterator();
            while (it.hasNext()) {
                String tn = it.next();
                String pos = last_pos_map.get(tn);
                status_writer.write(tn + COMMA + pos + "\n");
            }
            status_writer.flush();
            status_writer.close();

        } catch (Exception e) {
            logger.error("Error write pos value to status file!!!");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String getLastPos(String dir) {
        //读取文件
        try {
            status_reader = new FileReader(status_file);
            char[] chars = new char[(int) status_file.length()];
            status_reader.read(chars);
            status_reader.close();

            String[] dir_info = new String(chars).split("\n");
            for (int i = 0; i < dir_info.length; i++) {
                String[] pos_arr = dir_info[i].trim().split(COMMA);
                if (pos_arr.length == 3) {
                    last_pos_map.put(pos_arr[0], pos_arr[1] + COMMA
                            + pos_arr[2]);
                }
            }

        } catch (Exception e) {
            logger.error("Error reading pos value from status file!!!");
            e.printStackTrace();
            return EMPTY;
        }

        // 从map中获取
        String ret = last_pos_map.get(dir);
        if (ret == null) {
            ret = EMPTY;
        }
        return ret;
    }
}
