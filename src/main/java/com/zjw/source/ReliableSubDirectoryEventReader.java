package com.zjw.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.zjw.source.SpoolDirectorySourceConfigurationConstants.ConsumeOrder;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.serialization.*;
import org.apache.flume.tools.PlatformDetect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by zjw on 2017/6/19.
 */
public class ReliableSubDirectoryEventReader implements ReliableEventReader {

    private static final Logger logger = LoggerFactory
            .getLogger(ReliableSubDirectoryEventReader.class);

    static final String metaFileName = ".flumespool-main.meta";//被处理文件元数据的存储目录，默认".flumespool"

    private final File spoolDirectory;//监控目录，不能为空，没有默认值
    private final String completedSuffix;//是文件读取完毕后给完成文件添加的标记后缀，默认是".COMPLETED"；
    private final String deserializerType;//将文件中的数据序列化成event的方式，默认是“LINE”---org.apache.flume.serialization.LineDeserializer
    private final Context deserializerContext;//这个主要用在Deserializer中设置编码方式outputCharset和文件每行最大长度maxLineLength。
    private final Pattern ignorePattern;//忽略符合条件的文件名
    private final File metaFile;
    private final boolean annotateFileName;
    private final boolean annotateBaseName;
    private final String fileNameHeader;//是否在event的Header中添加文件名，boolean类型
    private final String baseNameHeader;
    private final String deletePolicy;//这是是否删除读取完毕的文件，默认是"never"，就是不删除，目前只支持"never"和“IMMEDIATE”；
    private final Charset inputCharset;//编码方式，默认是"UTF-8"；
    private final DecodeErrorPolicy decodeErrorPolicy;
    private final ConsumeOrder consumeOrder;
    private final boolean recursiveDirectorySearch;
    //目录规则,例如：/logs/game_name/zid/sid/par_dt,根据目录规则，解析日志文件路径的对应目录
    private final String directoryPattern;

    private Optional<FileInfo> currentFile = Optional.absent();
    /** Always contains the last file from which lines have been read. **/
    private Optional<FileInfo> lastFileRead = Optional.absent();
    private boolean committed = true;

    /** Instance var to Cache directory listing **/
    private Iterator<File> candidateFileIter = null;
    private int listFilesCount = 0;

    /**
     Filter to exclude files/directories either hidden, finished, or names matching the ignore pattern
     */
    final FileFilter filter = new FileFilter() {
        public boolean accept(File candidate) {
            if ( candidate.isDirectory() ) {
                String directoryName = candidate.getName();
                if ( (! recursiveDirectorySearch) ||
                        (directoryName.startsWith(".")) ||
                        ignorePattern.matcher(directoryName).matches()) {
                    return false;
                }
                return true;
            }
            else{
                String fileName = candidate.getName();
                if ((fileName.endsWith(completedSuffix)) ||
                        (fileName.startsWith(".")) ||
                        ignorePattern.matcher(fileName).matches()) {
                    return false;
                }
            }
            return true;
        }
    };

    /**
     * Recursively gather candidate files
     * @param directory the directory to gather files from
     * @return list of files within the passed in directory
     */
    private List<File> getCandidateFiles(File directory){
        List<File> candidateFiles = new ArrayList<File>();
        if (directory==null || ! directory.isDirectory()){
            return candidateFiles;
        }

        for(File file : directory.listFiles(filter)){
            if (file.isDirectory()) {
                candidateFiles.addAll(getCandidateFiles(file));
            }
            else {
                candidateFiles.add(file);
            }
        }

        return candidateFiles;
    }


    /**
     * Create a ReliableSpoolingFileEventReader to watch the given directory.
     */
    private ReliableSubDirectoryEventReader(File spoolDirectory,
                                                     String completedSuffix, String ignorePattern, String trackerDirPath,
                                                     boolean annotateFileName, String fileNameHeader,
                                                     boolean annotateBaseName, String baseNameHeader,
                                                     String deserializerType, Context deserializerContext,
                                                     String deletePolicy, String inputCharset,
                                                     DecodeErrorPolicy decodeErrorPolicy,
                                                     ConsumeOrder consumeOrder,
                                                     boolean recursiveDirectorySearch,String directoryPattern) throws IOException {

        // Sanity checks
        Preconditions.checkNotNull(spoolDirectory);
        Preconditions.checkNotNull(completedSuffix);
        Preconditions.checkNotNull(ignorePattern);
        Preconditions.checkNotNull(trackerDirPath);
        Preconditions.checkNotNull(deserializerType);
        Preconditions.checkNotNull(deserializerContext);
        Preconditions.checkNotNull(deletePolicy);
        Preconditions.checkNotNull(inputCharset);

        // validate delete policy
        if (!deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name()) &&
                !deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
            throw new IllegalArgumentException("Delete policies other than " +
                    "NEVER and IMMEDIATE are not yet supported");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Initializing {} with directory={}, metaDir={}, " +
                            "deserializer={}",
                    new Object[] { ReliableSubDirectoryEventReader.class.getSimpleName(),
                            spoolDirectory, trackerDirPath, deserializerType });
        }

        // Verify directory exists and is readable/writable
        Preconditions.checkState(spoolDirectory.exists(),
                "Directory does not exist: " + spoolDirectory.getAbsolutePath());
        Preconditions.checkState(spoolDirectory.isDirectory(),
                "Path is not a directory: " + spoolDirectory.getAbsolutePath());

        // Do a canary test to make sure we have access to spooling directory
        try {
            File canary = File.createTempFile("flume-spooldir-perm-check-", ".canary",
                    spoolDirectory);
            Files.write("testing flume file permissions\n", canary, Charsets.UTF_8);
            List<String> lines = Files.readLines(canary, Charsets.UTF_8);
            Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", canary);
            if (!canary.delete()) {
                throw new IOException("Unable to delete canary file " + canary);
            }
            logger.debug("Successfully created and deleted canary file: {}", canary);
        } catch (IOException e) {
            throw new FlumeException("Unable to read and modify files" +
                    " in the spooling directory: " + spoolDirectory, e);
        }

        this.spoolDirectory = spoolDirectory;
        this.completedSuffix = completedSuffix;
        this.deserializerType = deserializerType;
        this.deserializerContext = deserializerContext;
        this.annotateFileName = annotateFileName;
        this.fileNameHeader = fileNameHeader;
        this.annotateBaseName = annotateBaseName;
        this.baseNameHeader = baseNameHeader;
        this.ignorePattern = Pattern.compile(ignorePattern);
        this.deletePolicy = deletePolicy;
        this.inputCharset = Charset.forName(inputCharset);
        this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);
        this.consumeOrder = Preconditions.checkNotNull(consumeOrder);
        this.recursiveDirectorySearch = recursiveDirectorySearch;
        this.directoryPattern = directoryPattern;


        File trackerDirectory = new File(trackerDirPath);

        // if relative path, treat as relative to spool directory
        if (!trackerDirectory.isAbsolute()) {
            trackerDirectory = new File(spoolDirectory, trackerDirPath);
        }

        // ensure that meta directory exists
        if (!trackerDirectory.exists()) {
            if (!trackerDirectory.mkdir()) {
                throw new IOException("Unable to mkdir nonexistent meta directory " +
                        trackerDirectory);
            }
        }

        // ensure that the meta directory is a directory
        if (!trackerDirectory.isDirectory()) {
            throw new IOException("Specified meta directory is not a directory" +
                    trackerDirectory);
        }

        this.metaFile = new File(trackerDirectory, metaFileName);
        if(metaFile.exists() && metaFile.length() == 0) {
            deleteMetaFile();
        }
    }

    @VisibleForTesting
    int getListFilesCount() {
        return listFilesCount;
    }

    /** Return the filename which generated the data from the last successful
     * {@link #readEvents(int)} call. Returns null if called before any file
     * contents are read. */
    public String getLastFileRead() {
        if (!lastFileRead.isPresent()) {
            return null;
        }
        return lastFileRead.get().getFile().getAbsolutePath();
    }

    // public interface
    public Event readEvent() throws IOException {
        List<Event> events = readEvents(1);
        if (!events.isEmpty()) {
            return events.get(0);
        } else {
            return null;
        }
    }

    public List<Event> readEvents(int numEvents) throws IOException {
        //如果没有提交
        if (!committed) {
            if (!currentFile.isPresent()) {//为空,如果Optional包含非null的引用（引用存在），返回true
                throw new IllegalStateException("File should not roll when " +
                        "commit is outstanding.");
            }
            logger.info("Last read was never committed - resetting mark position.");
            currentFile.get().getDeserializer().reset();
            //如果已经提交，则获取下一个文件
        } else {//已经committed成功，初始committed=True，获取下一个文件
            // Check if new files have arrived since last call
            if (!currentFile.isPresent()) {//为空，获取下一个文件，初次调用
                currentFile = getNextFile();
            }
            // Return empty list if no new files
            if (!currentFile.isPresent()) {//为空，已经没有可读的文件了
                return Collections.emptyList();
            }
            //其它的说明是currentFile目前还在读
        }

        /*
        * 获取配置文件中的字段名和日志组成event body
        *
        * */
        String filepath = currentFile.get().getFile().getPath();
        //获取文件目录的上一层目录
        String uppath = filepath.substring(0, filepath.lastIndexOf("/"));
        //获取上一层目录中的配置文件
        File schemafile = new File(uppath);
        String[] filelists = schemafile.list();
        String schema = readLineFile(uppath+'/'+filelists[0]);

        SubLineDeserializer des = currentFile.get().getDeserializer();
        List<Event> events = des.readEvents(numEvents,schema);//添加event的body,调用序列化方法

        /* It's possible that the last read took us just up to a file boundary.
        * If so, try to roll to the next file, if there is one.
        * Loop until events is not empty or there is no next file in case of 0 byte files */
        while (events.isEmpty()) {
            logger.info("Last read took us just up to a file boundary. Rolling to the next file, if there is one.");
            retireCurrentFile();//改名字
            currentFile = getNextFile();//换下一个文件
            if (!currentFile.isPresent()) {
                return Collections.emptyList();
            }
            events = currentFile.get().getDeserializer().readEvents(numEvents,schema);//继续读,添加event的body
        }

        if (annotateFileName) {
            String filename = currentFile.get().getFile().getAbsolutePath();
            //获取文件目录

            //拆分路径和规则，配成map放到header中
            String[] parts = filename.split("/");
            String[] partPattern = directoryPattern.split("/");

            HashMap map = new HashMap();
            for(int i=1;i<parts.length;i++){
                map.put(partPattern[i],parts[i]);
            }
            map.put("filename",filename);

            for (Event event : events) {
                event.getHeaders().put(fileNameHeader, map.toString());
            }
        }

        if (annotateBaseName) {
            String basename = currentFile.get().getFile().getName();
            for (Event event : events) {
                event.getHeaders().put(baseNameHeader, basename);
            }
        }

        committed = false;
        lastFileRead = currentFile;
        return events;
    }

    public void close() throws IOException {
        if (currentFile.isPresent()) {
            currentFile.get().getDeserializer().close();
            currentFile = Optional.absent();
        }
    }

    /** Commit the last lines which were read. */
    public void commit() throws IOException {
        if (!committed && currentFile.isPresent()) {
            currentFile.get().getDeserializer().mark();
            committed = true;
        }
    }

    public String readLineFile(String filename){
        try {
            FileInputStream in = new FileInputStream(filename);
            InputStreamReader inReader = new InputStreamReader(in, "UTF-8");
            BufferedReader bufReader = new BufferedReader(inReader);
            String line = bufReader.readLine();
            bufReader.close();
            inReader.close();
            in.close();
            return line;
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("读取" + filename + "出错！");
            return null;
        }
    }

    /**
     * Closes currentFile and attempt to rename it.
     *
     * If these operations fail in a way that may cause duplicate log entries,
     * an error is logged but no exceptions are thrown. If these operations fail
     * in a way that indicates potential misuse of the spooling directory, a
     * FlumeException will be thrown.
     * @throws FlumeException if files do not conform to spooling assumptions
     */
    private void retireCurrentFile() throws IOException {
        Preconditions.checkState(currentFile.isPresent());

        File fileToRoll = new File(currentFile.get().getFile().getAbsolutePath());

        currentFile.get().getDeserializer().close();

        // Verify that spooling assumptions hold
        if (fileToRoll.lastModified() != currentFile.get().getLastModified()) {
            String message = "File has been modified since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        }
        if (fileToRoll.length() != currentFile.get().getLength()) {
            String message = "File has changed size since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        }

        if (deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name())) {
            rollCurrentFile(fileToRoll);
        } else if (deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
            deleteCurrentFile(fileToRoll);
        } else {
            // TODO: implement delay in the future
            throw new IllegalArgumentException("Unsupported delete policy: " +
                    deletePolicy);
        }
    }

    /**
     * Rename the given spooled file
     * @param fileToRoll
     * @throws IOException
     */
    private void rollCurrentFile(File fileToRoll) throws IOException {

        File dest = new File(fileToRoll.getPath() + completedSuffix);
        logger.info("Preparing to move file {} to {}", fileToRoll, dest);

        // Before renaming, check whether destination file name exists
        if (dest.exists() && PlatformDetect.isWindows()) {
      /*
       * If we are here, it means the completed file already exists. In almost
       * every case this means the user is violating an assumption of Flume
       * (that log files are placed in the spooling directory with unique
       * names). However, there is a corner case on Windows systems where the
       * file was already rolled but the rename was not atomic. If that seems
       * likely, we let it pass with only a warning.
       */
            if (Files.equal(currentFile.get().getFile(), dest)) {
                logger.warn("Completed file " + dest +
                        " already exists, but files match, so continuing.");
                boolean deleted = fileToRoll.delete();
                if (!deleted) {
                    logger.error("Unable to delete file " + fileToRoll.getAbsolutePath() +
                            ". It will likely be ingested another time.");
                }
            } else {
                String message = "File name has been re-used with different" +
                        " files. Spooling assumptions violated for " + dest;
                throw new IllegalStateException(message);
            }

            // Dest file exists and not on windows
        } else if (dest.exists()) {
            String message = "File name has been re-used with different" +
                    " files. Spooling assumptions violated for " + dest;
            throw new IllegalStateException(message);

            // Destination file does not already exist. We are good to go!
        } else {
            boolean renamed = fileToRoll.renameTo(dest);
            if (renamed) {
                logger.debug("Successfully rolled file {} to {}", fileToRoll, dest);

                // now we no longer need the meta file
                deleteMetaFile();
            } else {
        /* If we are here then the file cannot be renamed for a reason other
         * than that the destination file exists (actually, that remains
         * possible w/ small probability due to TOC-TOU conditions).*/
                String message = "Unable to move " + fileToRoll + " to " + dest +
                        ". This will likely cause duplicate events. Please verify that " +
                        "flume has sufficient permissions to perform these operations.";
                throw new FlumeException(message);
            }
        }
    }

    /**
     * Delete the given spooled file
     * @param fileToDelete
     * @throws IOException
     */
    private void deleteCurrentFile(File fileToDelete) throws IOException {
        logger.info("Preparing to delete file {}", fileToDelete);
        if (!fileToDelete.exists()) {
            logger.warn("Unable to delete nonexistent file: {}", fileToDelete);
            return;
        }
        if (!fileToDelete.delete()) {
            throw new IOException("Unable to delete spool file: " + fileToDelete);
        }
        // now we no longer need the meta file
        deleteMetaFile();
    }

    /**
     * Returns the next file to be consumed from the chosen directory.
     * If the directory is empty or the chosen file is not readable,
     * this will return an absent option.
     * If the {@link #consumeOrder} variable is {@link ConsumeOrder#OLDEST}
     * then returns the oldest file. If the {@link #consumeOrder} variable
     * is {@link ConsumeOrder#YOUNGEST} then returns the youngest file.
     * If two or more files are equally old/young, then the file name with
     * lower lexicographical value is returned.
     * If the {@link #consumeOrder} variable is {@link ConsumeOrder#RANDOM}
     * then cache the directory listing to amortize retreival cost, and return
     * any arbitary file from the directory.
     */
    private Optional<FileInfo> getNextFile() {
        List<File> candidateFiles = Collections.emptyList();

        if (consumeOrder != ConsumeOrder.RANDOM ||
                candidateFileIter == null ||
                !candidateFileIter.hasNext()) {
      /* Filter to exclude finished or hidden files */
            FileFilter filter = new FileFilter() {
                public boolean accept(File candidate) {
                    String fileName = candidate.getName();
                    if ((candidate.isDirectory()) ||
                            (fileName.endsWith(completedSuffix)) ||
                            (fileName.startsWith(".")) ||
                            ignorePattern.matcher(fileName).matches()) {
                        return false;
                    }
                    return true;
                }
            };
//      candidateFiles = Arrays.asList(spoolDirectory.listFiles(filter));
            candidateFiles = getCandidateFiles(spoolDirectory);

            listFilesCount++;
            candidateFileIter = candidateFiles.iterator();
        }

        if (!candidateFileIter.hasNext()) { // No matching file in spooling directory.
            return Optional.absent();
        }

        File selectedFile = candidateFileIter.next();
        if (consumeOrder == ConsumeOrder.RANDOM) { // Selected file is random.
            return openFile(selectedFile);
        } else if (consumeOrder == ConsumeOrder.YOUNGEST) {
            for (File candidateFile: candidateFiles) {
                long compare = selectedFile.lastModified() -
                        candidateFile.lastModified();
                if (compare == 0) { // ts is same pick smallest lexicographically.
                    selectedFile = smallerLexicographical(selectedFile, candidateFile);
                } else if (compare < 0) { // candidate is younger (cand-ts > selec-ts)
                    selectedFile = candidateFile;
                }
            }
        } else { // default order is OLDEST
            for (File candidateFile: candidateFiles) {
                long compare = selectedFile.lastModified() -
                        candidateFile.lastModified();
                if (compare == 0) { // ts is same pick smallest lexicographically.
                    selectedFile = smallerLexicographical(selectedFile, candidateFile);
                } else if (compare > 0) { // candidate is older (cand-ts < selec-ts).
                    selectedFile = candidateFile;
                }
            }
        }

        return openFile(selectedFile);
    }

    private File smallerLexicographical(File f1, File f2) {
        if (f1.getName().compareTo(f2.getName()) < 0) {
            return f1;
        }
        return f2;
    }
    /**
     * Opens a file for consuming
     * @param file
     * @return {@link # FileInfo} for the file to consume or absent option if the
     * file does not exists or readable.
     */
    private Optional<FileInfo> openFile(File file) {
        try {
            // roll the meta file, if needed
            String nextPath = file.getPath();
            // 这里判断是否需要刷新元文件记录每个正在读取的文件信息，最重要的是文件名称，以及当前已经读取到的位置的数据
            PositionTracker tracker =
                    DurablePositionTracker.getInstance(metaFile, nextPath);
            // 如果tracker中的target与file不是同一个文件，那么说明，是一个新文件
            // 然后重建tracker
            if (!tracker.getTarget().equals(nextPath)) {
                tracker.close();
                deleteMetaFile();
                tracker = DurablePositionTracker.getInstance(metaFile, nextPath);
            }

            // sanity check
            // 在此检查构造的tracker是否正确
            Preconditions.checkState(tracker.getTarget().equals(nextPath),
                    "Tracker target %s does not equal expected filename %s",
                    tracker.getTarget(), nextPath);

            // 构造输入流
            // tracker传进去的作用是：当使用deserializer读取文件行时，会从上次记录的位置开始读取
            // 所以我们在每读取一行时，需要记录当前行结束时，文件已经读取到的位置，以字节数表示
            ResettableInputStream in =
                    new ResettableFileInputStream(file, tracker,
                            ResettableFileInputStream.DEFAULT_BUF_SIZE, inputCharset,
                            decodeErrorPolicy);
            SubLineDeserializer deserializer = new SubLineDeserializer(deserializerContext, in);

            return Optional.of(new FileInfo(file, deserializer));
        } catch (FileNotFoundException e) {
            // File could have been deleted in the interim
            logger.warn("Could not find file: " + file, e);
            return Optional.absent();
        } catch (IOException e) {
            logger.error("Exception opening file: " + file, e);
            return Optional.absent();
        }
    }

    private void deleteMetaFile() throws IOException {
        if (metaFile.exists() && !metaFile.delete()) {
            throw new IOException("Unable to delete old meta file " + metaFile);
        }
    }

    /** An immutable class with information about a file being processed. */
    private static class FileInfo {
        private final File file;
        private final long length;
        private final long lastModified;
        private final SubLineDeserializer deserializer;

        public FileInfo(File file, SubLineDeserializer deserializer) {
            this.file = file;
            this.length = file.length();
            this.lastModified = file.lastModified();
            this.deserializer = deserializer;
        }

        public long getLength() { return length; }
        public long getLastModified() { return lastModified; }
        public SubLineDeserializer getDeserializer() { return deserializer; }
        public File getFile() { return file; }
    }

    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    static enum DeletePolicy {
        NEVER,
        IMMEDIATE,
        DELAY
    }

    /**
     * Special builder class for ReliableSpoolingFileEventReader
     */
    public static class Builder {
        private File spoolDirectory;
        private String completedSuffix =
                SpoolDirectorySourceConfigurationConstants.SPOOLED_FILE_SUFFIX;
        private String ignorePattern =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_IGNORE_PAT;
        private String trackerDirPath =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR;
        private Boolean annotateFileName =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_FILE_HEADER;
        private String fileNameHeader =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
        private Boolean annotateBaseName =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER;
        private String baseNameHeader =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER_KEY;
        private String deserializerType =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_DESERIALIZER;
        private Context deserializerContext = new Context();
        private String deletePolicy =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_DELETE_POLICY;
        private String inputCharset =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
        private DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy.valueOf(
                SpoolDirectorySourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY
                        .toUpperCase(Locale.ENGLISH));
        private ConsumeOrder consumeOrder =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_CONSUME_ORDER;

        private boolean recursiveDirectorySearch = false;
        private String directoryPattern =
                SpoolDirectorySourceConfigurationConstants.DEFAULT_DIRECTORY_PATTERN;

        public Builder recursiveDirectorySearch(boolean recursiveDirectorySearch) {
            this.recursiveDirectorySearch = recursiveDirectorySearch;
            return this;
        }

        public Builder spoolDirectory(File directory) {
            this.spoolDirectory = directory;
            return this;
        }

        public Builder completedSuffix(String completedSuffix) {
            this.completedSuffix = completedSuffix;
            return this;
        }

        public Builder ignorePattern(String ignorePattern) {
            this.ignorePattern = ignorePattern;
            return this;
        }

        public Builder trackerDirPath(String trackerDirPath) {
            this.trackerDirPath = trackerDirPath;
            return this;
        }

        public Builder annotateFileName(Boolean annotateFileName) {
            this.annotateFileName = annotateFileName;
            return this;
        }

        public Builder fileNameHeader(String fileNameHeader) {
            this.fileNameHeader = fileNameHeader;
            return this;
        }

        public Builder annotateBaseName(Boolean annotateBaseName) {
            this.annotateBaseName = annotateBaseName;
            return this;
        }

        public Builder baseNameHeader(String baseNameHeader) {
            this.baseNameHeader = baseNameHeader;
            return this;
        }

        public Builder deserializerType(String deserializerType) {
            this.deserializerType = deserializerType;
            return this;
        }

        public Builder deserializerContext(Context deserializerContext) {
            this.deserializerContext = deserializerContext;
            return this;
        }

        public Builder deletePolicy(String deletePolicy) {
            this.deletePolicy = deletePolicy;
            return this;
        }

        public Builder inputCharset(String inputCharset) {
            this.inputCharset = inputCharset;
            return this;
        }

        public Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
            this.decodeErrorPolicy = decodeErrorPolicy;
            return this;
        }

        public Builder consumeOrder(ConsumeOrder consumeOrder) {
            this.consumeOrder = consumeOrder;
            return this;
        }

        public Builder directoryPattern(String directoryPattern) {
            this.directoryPattern = directoryPattern;
            return this;
        }

        public ReliableSubDirectoryEventReader build() throws IOException {
            return new ReliableSubDirectoryEventReader(spoolDirectory, completedSuffix,
                    ignorePattern, trackerDirPath, annotateFileName, fileNameHeader,
                    annotateBaseName, baseNameHeader, deserializerType,
                    deserializerContext, deletePolicy, inputCharset, decodeErrorPolicy,
                    consumeOrder, recursiveDirectorySearch,directoryPattern);
        }
    }

}
