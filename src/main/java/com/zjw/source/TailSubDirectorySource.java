package com.zjw.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

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
        /*check idle file*/
        idleFileChecker = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("idleFileChecker").build());
        idleFileChecker.scheduleWithFixedDelay(new idleFileCheckerRunnable(),
                idleTimeout, checkIdleInterval, TimeUnit.MILLISECONDS);
        /*write position*/
        positionWriter = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
        positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
                writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);

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
            writePosition();
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
            existingInodes.clear();
            existingInodes.addAll(reader.updateTailFiles());
            for (long inode : existingInodes) {
                TailFile tf = reader.getTailFiles().get(inode);
                /*如果文件需要被监控*/
                if (tf.needTail()) {
                    /*读取文件生成events*/
                    tailFileProcess(tf, true);
                }
            }
            closeTailFiles();
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
            List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
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

    private void closeTailFiles() throws IOException, InterruptedException {
        for (long inode : idleInodes) {
            TailFile tf = reader.getTailFiles().get(inode);
            if (tf.getRaf() != null) { // when file has not closed yet
                tailFileProcess(tf, false);
                tf.close();
                logger.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
    }
}
        idleInodes.clear();
    }

    /**
     * Runnable class that checks whether there are files which should be closed.
     */
    private class idleFileCheckerRunnable implements Runnable {
        public void run() {
            try {
                long now = System.currentTimeMillis();
                for (TailFile tf : reader.getTailFiles().values()) {
                    if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {
                        idleInodes.add(tf.getInode());
                    }
                }
            } catch (Throwable t) {
                logger.error("Uncaught exception in IdleFileChecker thread", t);
            }
        }
    }

    /**
     * Runnable class that writes a position file which has the last read position
     * of each file.
     */
    private class PositionWriterRunnable implements Runnable {
        public void run() {
            writePosition();
        }
    }

    private void writePosition() {
        File file = new File(positionFilePath);
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            if (!existingInodes.isEmpty()) {
                String json = toPosInfoJson();
                writer.write(json);
            }
        } catch (Throwable t){
            logger.error("Failed writing positionFile", t);
        } finally {
            try {
                if (writer != null) writer.close();
            } catch (IOException e) {
                logger.error("Error: " + e.getMessage(), e);
            }
        }
    }

    private String toPosInfoJson() {
        @SuppressWarnings("rawtypes")
        List<Map> posInfos = Lists.newArrayList();
        for (Long inode : existingInodes) {
            TailFile tf = reader.getTailFiles().get(inode);
            posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
        }
        return new Gson().toJson(posInfos);
    }
}
