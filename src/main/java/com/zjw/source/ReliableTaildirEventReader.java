/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.zjw.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.stream.JsonReader;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableTaildirEventReader implements ReliableEventReader {
  private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReader.class);
  public static final String OS_NAME = System.getProperty("os.name").toLowerCase();

  private final File spoolDirectory;

  private TailFile currentFile = null;
  private Map<Long, TailFile> tailFiles = Maps.newHashMap();
  private long updateTime;
  private boolean addByteOffset;
  private boolean committed = true;

  //目录规则,例如：/logs/game_name/zid/sid/par_dt,根据目录规则，解析日志文件路径的对应目录
  private final String directoryPattern;

  /**
   * Create a ReliableTaildirEventReader to watch the given directory.
   */
  private ReliableTaildirEventReader(File spoolDirectory, String positionFilePath,
      boolean skipToEnd, boolean addByteOffset,String directoryPattern) throws IOException {
    // Sanity checks
    Preconditions.checkNotNull(spoolDirectory);
    Preconditions.checkNotNull(directoryPattern);
    Preconditions.checkNotNull(positionFilePath);

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}, metaDir={}",
          new Object[] { ReliableTaildirEventReader.class.getSimpleName(), spoolDirectory });
    }

    this.addByteOffset = addByteOffset;
    this.spoolDirectory = spoolDirectory;
    this.directoryPattern = directoryPattern;
    updateTailFiles(skipToEnd);

    logger.info("Updating position from position file: " + positionFilePath);
    loadPositionFile(positionFilePath);
  }

  /**
   * Load a position file which has the last read position of each file.
   * If the position file exists, update tailFiles mapping.
   */
  public void loadPositionFile(String filePath) {
    Long inode, pos;
    String path;
    FileReader fr = null;
    JsonReader jr = null;
    try {
      fr = new FileReader(filePath);
      jr = new JsonReader(fr);
      jr.beginArray();
      while (jr.hasNext()) {
        inode = null;
        pos = null;
        path = null;
        jr.beginObject();
        while (jr.hasNext()) {
          switch (jr.nextName()) {
          case "inode":
            inode = jr.nextLong();
            break;
          case "pos":
            pos = jr.nextLong();
            break;
          case "file":
            path = jr.nextString();
            break;
          }
        }
        jr.endObject();

        for (Object v : Arrays.asList(inode, pos, path)) {
          Preconditions.checkNotNull(v, "Detected missing value in position file. "
              + "inode: " + inode + ", pos: " + pos + ", path: " + path);
        }
        TailFile tf = tailFiles.get(inode);
        if (tf != null && tf.updatePos(path, inode, pos)) {
          tailFiles.put(inode, tf);
        } else {
          logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos);
        }
      }
      jr.endArray();
    } catch (FileNotFoundException e) {
      logger.info("File not found: " + filePath + ", not updating position");
    } catch (IOException e) {
      logger.error("Failed loading positionFile: " + filePath, e);
    } finally {
      try {
        if (fr != null) fr.close();
        if (jr != null) jr.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
      }
    }
  }

  public Map<Long, TailFile> getTailFiles() {
    return tailFiles;
  }

  public void setCurrentFile(TailFile currentFile) {
    this.currentFile = currentFile;
  }


  public Event readEvent() throws IOException {
    List<Event> events = readEvents(1);
    if (events.isEmpty()) {
      return null;
    }
    return events.get(0);
  }

  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    return readEvents(numEvents, false);
  }

  @VisibleForTesting
  public List<Event> readEvents(TailFile tf, int numEvents) throws IOException {
    setCurrentFile(tf);
    return readEvents(numEvents, true);
  }

  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL)
      throws IOException {
    /*
     * 获取配置文件中的字段名和日志组成event body
     *
     * */
    String filepath = currentFile.getPath();
    //获取文件目录的上一层目录
    String separator = File.separator;
    String currentpath = filepath.substring(0, filepath.lastIndexOf(separator));
    String uppath = currentpath.substring(0, currentpath.lastIndexOf(separator));
    //获取上一层目录中的配置文件
    File schemafile = new File(uppath);
    String[] filelists = schemafile.list();
    String schema = "";
    //schema文件名字和表名字对应
    for(String filename : filelists){
      if(filename.matches("(.*)schema")){
          String logname = filepath.substring(filepath.lastIndexOf(separator)+1,filepath.length());
          Pattern pattern = Pattern.compile("(\\D*)_\\d+.*");
          Matcher matcher = pattern.matcher(logname);
          String tablename = "";
          if(matcher.find()){
              tablename = matcher.group(1);
          }else{
              logger.debug("Lost {} schema file,please check it again!",tablename);
          }

          String schemaname = tablename+".schema";
          String schemapath = uppath+separator+schemaname;
          File sch = new File(schemapath);
          if (sch.exists()){
            schema = readLineFile(schemapath);
            //logger.info("******** schema {} ********",schema);
          }else {
            logger.error("schema {} does not exists,please check again!",schemaname);
          }
      }
    }
    if (!committed) {
      if (currentFile == null) {
        throw new IllegalStateException("current file does not exist. " + currentFile.getPath());
      }
      logger.info("Last read was never committed - resetting position");
      long lastPos = currentFile.getPos();
      currentFile.getRaf().seek(lastPos);
    }
    List<Event> events = currentFile.readEvents(schema,numEvents, backoffWithoutNL, addByteOffset);
    if (events.isEmpty()) {
      return events;
    }

    String filename = currentFile.getPath();

    //拆分路径和规则，配成map放到header中
    //header中的内容不能再嵌套map，否则无法解析
    String[] parts = filename.replaceAll("\\\\","/").split("/");
    String[] partPattern = directoryPattern.replaceAll("\\\\","/").split("/");
    //为每个event添加header
    for (Event event : events) {
      for(int i=1;i<partPattern.length;i++){
        //获取日志文件中的日期信息往event header 中添加timestamp
        if(partPattern[i].endsWith("logname")){
          Pattern pattern = Pattern.compile("\\D*_(\\d+)_(\\d+).log");
          Matcher matcher = pattern.matcher(parts[i]);
          String timestamp = "";
          if(matcher.find()){
            String date = matcher.group(1);
            String hour = matcher.group(2);
            String datehour = date+hour;
            SimpleDateFormat format =  new SimpleDateFormat("yyyyMMddHH");
            try {
              Date intdate = format.parse(datehour);
              timestamp = String.valueOf(intdate.getTime());
            } catch (ParseException e) {
              e.printStackTrace();
            }
          }
          event.getHeaders().put("timestamp",timestamp);
        }
        event.getHeaders().put(partPattern[i], parts[i]);
      }
    }
    committed = false;
    return events;
  }

  @Override
  public void close() throws IOException {
    for (TailFile tf : tailFiles.values()) {
      if (tf.getRaf() != null) tf.getRaf().close();
    }
  }

  /** Commit the last lines which were read. */
  @Override
  public void commit() throws IOException {
    if (!committed && currentFile != null) {
      long pos = currentFile.getRaf().getFilePointer();
      currentFile.setPos(pos);
      currentFile.setLastUpdated(updateTime);
      committed = true;
    }
  }

  /**
   * Update tailFiles mapping if a new file is created or appends are detected
   * to the existing file.
   */
  public List<Long> updateTailFiles(boolean skipToEnd) throws IOException {

    updateTime = System.currentTimeMillis();
    List<Long> updatedInodes = Lists.newArrayList();
    //获取带监控的目录

      for (File f : getMatchFiles( spoolDirectory)) {
        long inode = getInode(f);
        TailFile tf = tailFiles.get(inode);
        if (tf == null || !tf.getPath().equals(f.getAbsolutePath())) {
          long startPos = skipToEnd ? f.length() : 0;
          tf = openFile(f, inode, startPos);
        } else {
          boolean updated = tf.getLastUpdated() < f.lastModified();
          if (updated) {
            if (tf.getRaf() == null) {
              tf = openFile(f, inode, tf.getPos());
            }
            if (f.length() < tf.getPos()) {
              logger.info("Pos " + tf.getPos() + " is larger than file size! "
                      + "Restarting from pos 0, file: " + tf.getPath() + ", inode: " + inode);
              tf.updatePos(tf.getPath(), inode, 0);
            }
          }
          tf.setNeedTail(updated);
        }
        tailFiles.put(inode, tf);
        updatedInodes.add(inode);
      }
    return updatedInodes;
  }

  public List<Long> updateTailFiles() throws IOException {
    return updateTailFiles(false);
  }

  /*获取所有满足的文件*/
  private List<File> getMatchFiles(File parentDir) {
    //读取文件夹下和子文件夹下的所有满足的xxxx.log文件
    List<File> candidateFiles = new ArrayList<File>();
    if (parentDir==null || ! parentDir.isDirectory()){
      return candidateFiles;
    }

    for(File file : parentDir.listFiles()){
      if (file.isDirectory()) {
        candidateFiles.addAll(getMatchFiles(file));
      }
      else {
        if (file.getName().toString().matches("(.*)log")){
          candidateFiles.add(file);
        }
      }
    }
    ArrayList<File> result = Lists.newArrayList(candidateFiles);
    Collections.sort(result, new TailFile.CompareByLastModifiedTime());
    //logger.info("**********result********** {}",result);
    return result;
  }

  /*获取所有满足的文件夹*/
  private List<String> getMatchDirectories(File parentDir) {
    //读取文件夹下和子文件夹下的所有满足的xxxx.log文件
    Set<String> candidateDirectories = new HashSet<String>();

    if (parentDir==null || ! parentDir.isDirectory()){
      return null;
    }

    for(File file : parentDir.listFiles()){
      if (file.isDirectory()) {
        candidateDirectories.addAll(getMatchDirectories(file));
      }
      else {
        if (file.getName().toString().matches("(.*)log")){
          candidateDirectories.add(file.getParent());
        }
      }
    }
    ArrayList<String> result = Lists.newArrayList(candidateDirectories);
    Collections.sort(result);
    logger.info("**********result********** {}",result);
    List<String> candidateDirectoriesList = new ArrayList<String>(candidateDirectories);
    return candidateDirectoriesList;
  }

  private long getInode(File file) throws IOException {
    long inode;
    if (OS_NAME.contains("windows")) {
      inode = Long.parseLong(WinFileUtil.getFileId(file.toPath().toString()));
    } else {
      inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
    }
    return inode;
  }

  private TailFile openFile(File file, long inode, long pos) {
    try {
      logger.info("Opening file: " + file + ", inode: " + inode + ", pos: " + pos);
      return new TailFile(file, inode, pos);
    } catch (IOException e) {
      throw new FlumeException("Failed opening file: " + file, e);
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
   * Special builder class for ReliableTaildirEventReader
   */
  public static class Builder {

    private File spoolDirectory;
    private String positionFilePath;
    private boolean skipToEnd;
    private boolean addByteOffset;
    private String directoryPattern;

    public Builder spoolDirectory(File spoolDirectory) {
      this.spoolDirectory = spoolDirectory;
      return this;
    }

    public Builder positionFilePath(String positionFilePath) {
      this.positionFilePath = positionFilePath;
      return this;
    }

    public Builder skipToEnd(boolean skipToEnd) {
      this.skipToEnd = skipToEnd;
      return this;
    }

    public Builder addByteOffset(boolean addByteOffset) {
      this.addByteOffset = addByteOffset;
      return this;
    }

    public Builder directoryPattern(String directoryPattern) {
      this.directoryPattern = directoryPattern;
      return this;
    }

    public ReliableTaildirEventReader build() throws IOException {
      return new ReliableTaildirEventReader(spoolDirectory, positionFilePath, skipToEnd, addByteOffset,directoryPattern);
    }
  }

}
