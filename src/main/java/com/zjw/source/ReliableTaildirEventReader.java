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
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableTaildirEventReader implements ReliableEventReader {
  private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReader.class);

  private TailFile currentFile = null;
  private Map<String, TailFile> tailFiles = Maps.newHashMap();
  private long updateTime;
  private boolean addByteOffset;
  private boolean committed = true;

  //目录规则,例如：/logs/game_name/zid/sid/par_dt,根据目录规则，解析日志文件路径的对应目录
  private final String directoryPattern;
  private final String product;

  /**
   * Create a ReliableTaildirEventReader to watch the given directory.
   */
  private ReliableTaildirEventReader(File spoolDirectory,
      boolean addByteOffset,String directoryPattern,String product) throws IOException {
    // Sanity checks
    Preconditions.checkNotNull(spoolDirectory);
    Preconditions.checkNotNull(directoryPattern);

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}, metaDir={}",
          new Object[] { ReliableTaildirEventReader.class.getSimpleName(), spoolDirectory });
    }

    this.addByteOffset = addByteOffset;
    this.directoryPattern = directoryPattern;
    this.product = product;

  }

  public Map<String, TailFile> getTailFiles() {
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
    List<Event> events = null;
    String filepath = currentFile.getPath();
    //获取文件目录的上一层目录
    String separator = File.separator;
    String currentpath = filepath.substring(0, filepath.lastIndexOf(separator));
    String uppath = currentpath.substring(0, currentpath.lastIndexOf(separator));
    //获取上一层目录中的配置文件
    File schemafile = new File(uppath);
    String[] filelists = schemafile.list();
    String schema = "";
      if (!committed) {
          if (currentFile == null) {
              throw new IllegalStateException("current file does not exist. " + currentFile.getPath());
          }
          logger.info("Last read was never committed - resetting position");
          long lastPos = currentFile.getPos();
          currentFile.getRaf().seek(lastPos);
      }
    //schema文件名字和表名字对应
    for(String filename : filelists){
      if(filename.matches("(.*)schema")){
          //logger.info("======= Filename {} =======",filename);
          String logname = filepath.substring(filepath.lastIndexOf(separator)+1,filepath.length());
          Pattern pattern = Pattern.compile("(\\D*)_\\d+.*");
          Matcher matcher = pattern.matcher(logname);
          String tablename = "";
          if(matcher.find()){
              tablename = matcher.group(1);
          }else{
              logger.error("Lost {} schema file,please check it again!",tablename);
          }
          String schemaname = tablename+".schema";
          String schemapath = uppath+separator+schemaname;
          File sch = new File(schemapath);
          if (filename.equalsIgnoreCase(schemaname)){
            schema = readLineFile(schemapath);
            events = currentFile.readEvents(schema,numEvents, backoffWithoutNL, addByteOffset);
            //logger.info("--------4.tf {}------ ",currentFile.getPath());
          }
          if (!sch.exists()){
              logger.error("schema {} does not exists,please check again!",schemaname);
          }
      }
    }
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
        String log_type = event.getHeaders().get("log_type");
        event.getHeaders().put("product", product);
        event.getHeaders().put("topic", product+"_"+log_type);
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
  public List<String> updateTailFiles(String tailDirectory,String last_file_name) throws IOException {

    updateTime = System.currentTimeMillis();
    List<String> updatedInodes = Lists.newArrayList();
    //获取待监控的目录
      for (File f : getMatchFiles(new File(tailDirectory),last_file_name)) {
        TailFile tf = tailFiles.get(f.getAbsolutePath());
          if (tf == null || !tf.getPath().equals(f.getAbsolutePath())) {
              long startPos = 0;
              tf = openFile(f, startPos,0);
          } else {
              boolean updated = tf.getLastUpdated() < f.lastModified();
              if (updated) {
                  if (tf.getRaf() == null) {
                      tf = openFile(f, tf.getPos(),tf.getLine_pos());
                  }
              }
              tf.setNeedTail(updated);
          }
          tailFiles.put(f.getAbsolutePath(), tf);
          updatedInodes.add(f.getAbsolutePath());
      }
      return updatedInodes;
  }

  /*获取所有满足的文件*/
  private List<File> getMatchFiles(File parentDir,String last_file_name) {
    //读取文件夹下和子文件夹下的所有满足的xxxx.log文件
    List<File> candidateFiles = new ArrayList<File>();
    if (parentDir==null || ! parentDir.isDirectory()){
      return candidateFiles;
    }
    for(File file : parentDir.listFiles()){
      if (file.isDirectory()) {
        candidateFiles.addAll(getMatchFiles(file,last_file_name));
      }
      else {
        if (file.getName().toString().matches("(.*)log")){
            //和记录文件中的文件比较，如果字符串比较，大于最后记录的文件则添加到满足的文件
            if(last_file_name.equals("")){
                candidateFiles.add(file);
            }else{
                if(file.getName().toString().compareToIgnoreCase(last_file_name.substring(1))>=0){
                    //logger.info("++++++ CompareFile {} ++++++",file.getName().toString());
                    //logger.info("****** CompareToFile {} ******",last_file_name.substring(1));
                    candidateFiles.add(file);
                }
            }

        }
      }
    }
    ArrayList<File> result = Lists.newArrayList(candidateFiles);
    Collections.sort(result, new TailFile.CompareByLastModifiedTime());
    //Collections.reverse(result);
    //logger.info("**********result********** {}",result);
    return result;
  }

    private TailFile openFile(File file, long pos,long line_pos) {
        try {
            logger.info("Opening file: " + file  + ", pos: " + pos);
            return new TailFile(file, pos,line_pos);
        } catch (IOException e) {
            throw new FlumeException("Failed opening file: " + file, e);
        }
    }

  /*获取所有满足的文件夹*/
  public List<String> getMatchDirectories(File parentDir) {
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
        if (file.getName().toString().matches("(.*)log") && (!candidateDirectories.contains(file.getParent()))){
          //判断是否有schema文件，有的话就加入待扫描的数组中
          String filePath = file.getParent();
          File schemaFile = new File(filePath+".schema");
          //logger.info("********** schemaFile {} **********",schemaFile);
          if(schemaFile.exists()){
            candidateDirectories.add(file.getParent());
          }
          else{
            logger.warn("********** schemaFile {} does not exists!**********",schemaFile);
          }
        }
      }
    }
    ArrayList<String> result = Lists.newArrayList(candidateDirectories);
    Collections.sort(result);
    //logger.info("**********result********** {}",result);
    List<String> candidateDirectoriesList = new ArrayList<String>(candidateDirectories);
    return candidateDirectoriesList;
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
    private boolean addByteOffset;
    private String directoryPattern;
    private String product;

    public Builder spoolDirectory(File spoolDirectory) {
      this.spoolDirectory = spoolDirectory;
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
    public Builder product(String product) {
      this.product = product;
      return this;
    }

    public ReliableTaildirEventReader build() throws IOException {
      return new ReliableTaildirEventReader(spoolDirectory, addByteOffset,directoryPattern,product);
    }
  }

}
