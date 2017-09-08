package com.zjw.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import java.util.concurrent.TimeUnit;

import static com.zjw.source.TailSubDirectorySourceConfigurationConstants.*;

public class TailSubDirectorySource extends AbstractSource implements
        PollableSource, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(TailSubDirectorySource.class);

    /*config params*/
    private String spoolDirectory;
    private String directoryPattern;
    private int batchSize;
    private boolean byteOffsetHeader;

    /*rfeader params*/
    private SourceCounter sourceCounter;
    private ReliableTaildirEventReader reader;
    private int retryInterval = 1000;
    private int maxRetryInterval = 5000;

    private List<String> existingInodes = new CopyOnWriteArrayList<>();

    private Map<String, String> last_pos_map = new HashMap<>();
    private static final String COMMA = ",";
    private static final String EMPTY = "";

    private File status_file;
    Writer status_writer;
    Reader status_reader;
    private String status_fn;
    private String product;

    private String current_dir;
    private String file_loc;


    @Override
    public synchronized void start() {
        logger.info("{} TaildirSource source starting with directory: {}", getName(), spoolDirectory);
        File directory = new File(spoolDirectory);
        try {
            reader = new ReliableTaildirEventReader.Builder()
                    .spoolDirectory(directory)
                    .addByteOffset(byteOffsetHeader)
                    .directoryPattern(directoryPattern)
                    .product(product)
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
            ExecutorService[] services = {};
            for (ExecutorService service : services) {
                service.shutdown();
                if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
                    service.shutdownNow();
                }
            }
            // write the last position
            //setLastPos(current_dir,file_loc);
            //logger.info("---------LastPos {} -----------",current_dir+file_loc);
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
        return String.format("Taildir source:byteOffsetHeader: %s", byteOffsetHeader);
    }


    public synchronized void configure(Context context) {

        spoolDirectory = context.getString(SPOOL_DIRECTORY);
        Preconditions.checkState(spoolDirectory != null, "Configuration must specify a spooling directory");

        status_fn = context.getString(STATUS_FN);
        Preconditions.checkNotNull(status_fn);

        product = context.getString(PRODUCT_NAME);

        status_file = new File(status_fn);
        directoryPattern = context.getString(DIRECTORY_PATTERN,DEFAULT_DIRECTORY_PATTERN);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);

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
                file_loc = getLastPos(tailDirectory);
                //logger.info("--------File location {}------ ",file_loc);
                if (file_loc.equals("")){
                    //如果位置文件是空的，则从头开始读
                    existingInodes.clear();
                    existingInodes.addAll(reader.updateTailFiles(tailDirectory,""));
                }else{
                    //如果位置文件中记录的偏移量信息，则从记录的地方开始读
                    String file_name = file_loc.split(COMMA)[0];
                    //logger.info("--------start directory {}------ ",tailDirectory);
                    existingInodes.clear();
                    existingInodes.addAll(reader.updateTailFiles(tailDirectory,file_name));
                }
                //logger.info("--------existingInodes {}------ ",existingInodes.toString());
                //20170901需要判断一个文件是否已经读完然后再开始下一个文件的读取
                for (String fliePath : existingInodes) {
                    //logger.info("--------0.fliePath {}------ ",fliePath);
                    TailFile tf = reader.getTailFiles().get(fliePath);
                    //如果文件日期大于记录文件中的日志,则读取文件内容
                    current_dir = tf.getPath().substring(0, tf.getPath().lastIndexOf(File.separator));
                    String fileName = tf.getPath().substring(tf.getPath().lastIndexOf(File.separator));
                    //如果记录文件等于日志文件，则继续判断大小
                    if (tf.getPath().compareToIgnoreCase((tailDirectory+file_loc.split(",")[0])) == 0) {
                        //logger.info("--------Equal File {}------ ",tf.getPath());
                        tf.setLine_pos(Long.parseLong(file_loc.split(",")[1]));
                        tf.setPos(Long.parseLong(file_loc.split(",")[2]));
                        //logger.info("--------Read position {}------ ",tf.getPos());
                        try {
                            tailFileProcess(tf,true);
                            file_loc = fileName + COMMA + tf.getLine_pos()+COMMA+tf.getRaf().getFilePointer();
                            setLastPos(current_dir,file_loc);
                        } catch (Throwable t){
                            //记录该文件夹下读到的最后一个文件的位置
                            file_loc = fileName + COMMA + tf.getLine_pos()+COMMA+tf.getRaf().getFilePointer();
                            setLastPos(current_dir,file_loc);
                            logger.error("-------tailFileProcess error------ ");
                        }
                    } else {//如果文件名大于记录文件中的名称
                        /*读取文件生成events*/
                        //logger.info("--------Bigger File {}------ ",tf.getPath());
                        tf.setLine_pos(0L);
                        tf.setPos(0L);
                        try {
                            tailFileProcess(tf,true);
                            file_loc = fileName + COMMA + tf.getLine_pos()+COMMA+tf.getRaf().getFilePointer();
                            setLastPos(current_dir,file_loc);
                        } catch (Throwable t){
                            //记录该文件夹下读到的最后一个文件的位置
                            file_loc = fileName + COMMA + tf.getLine_pos()+COMMA+tf.getRaf().getFilePointer();
                            setLastPos(current_dir,file_loc);
                        }
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
            List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
            //20170831如果上一个文件没有读到记录点位置，返回的events会是null，然后结束该文件读取，这里要添加判断
            //判断该文件是否结束,通过读取的大小和文件总大小进行判断
            //logger.info("********* current {} ********total {}*******",tf.getRaf().getFilePointer(),tf.getFile_size());
            if (events.isEmpty() && tf.getRaf().getFilePointer()>=tf.getFile_size()) {
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
            //判断该文件是否结束,通过读取的大小和文件总大小进行判断
            if (events.size() < batchSize && tf.getRaf().getFilePointer()>=tf.getFile_size() ) {
                break;
            }
        }
    }

    //20170807写入信息到状态文件
    private boolean setLastPos(String dir, String v) {

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

    private String getLastPos(String dir) {
        //读取文件
        try {
            status_reader = new FileReader(status_file);
            char[] chars = new char[(int) status_file.length()];
            status_reader.read(chars);
            status_reader.close();

            String[] dir_info = new String(chars).split("\n");
            for (int i = 0; i < dir_info.length; i++) {
                String[] pos_arr = dir_info[i].trim().split(COMMA);
                if (pos_arr.length == 4) {
                    last_pos_map.put(pos_arr[0], pos_arr[1] + COMMA
                            + pos_arr[2] + COMMA + pos_arr[3]);
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
