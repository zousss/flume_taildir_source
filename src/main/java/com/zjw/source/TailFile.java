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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.zjw.source.TailSubDirectorySourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY;

public class TailFile {
  private static final Logger logger = LoggerFactory.getLogger(TailFile.class);

  private static final String LINE_SEP = "\n";
  private static final String LINE_SEP_WIN = "\r\n";

  private RandomAccessFile raf;
  private final String path;
  private long pos;
  private long lastUpdated;
  private boolean needTail;
  private Long line_pos;
  private Long curr_pos = 1L;
  private Long file_size = 0L;

  public TailFile(File file, long pos,long line_pos)
      throws IOException {
    this.raf = new RandomAccessFile(file, "r");
    if (pos > 0) raf.seek(pos);
    this.path = file.getAbsolutePath();
    this.pos = pos;
    this.line_pos = line_pos;
    this.lastUpdated = 0L;
    this.needTail = true;
    this.file_size = file.length();
  }

  public RandomAccessFile getRaf() { return raf; }
  public Long getFile_size() { return file_size; }
  public String getPath() { return path; }
  public long getPos() { return pos; }
  public long getLine_pos() { return line_pos; }
  public long getCurr_pos() { return curr_pos; }
  public long getLastUpdated() { return lastUpdated; }
  public void setPos(long pos) throws IOException { this.pos = pos;raf.seek(pos); }
  public void setLine_pos(Long line_pos) { this.line_pos = line_pos; }
  public void setLastUpdated(long lastUpdated) { this.lastUpdated = lastUpdated; }
  public void setNeedTail(boolean needTail) { this.needTail = needTail; }

  //20170808增加line_pos，记录读取的行数
  public List<Event> readEvents(String schema,int numEvents, boolean backoffWithoutNL,
      boolean addByteOffset) throws IOException {
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent(schema,backoffWithoutNL, addByteOffset);
      if (event == null) {
              break;
      }
      events.add(event);
      //logger.info("--------Curr_pos {}------line_pos {}",curr_pos,line_pos);
      //20170808如果当前行小于记录文件中的行数，则不读文件；如果大于，则开始读取
      if (curr_pos > line_pos){
        line_pos = curr_pos;
        //setLine_pos(line_pos);
      }
        curr_pos = curr_pos + 1;
    }
    return events;
  }
  /*获取文件指针，seek到相应的位置，开始读取数据*/
  private Event readEvent(String schema,boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
    Long posTmp = raf.getFilePointer();
    String line = readLine();
    if (line == null) {
      return null;
    }

    if (backoffWithoutNL && !line.endsWith(LINE_SEP)) {
      logger.info("Backing off in file without newline: "
          + path + ", pos: " + raf.getFilePointer());
      raf.seek(posTmp);
      return null;
    }
    String lineSep = LINE_SEP;
    if(line.endsWith(LINE_SEP_WIN)) {
      lineSep = LINE_SEP_WIN;
    }

    String newLine = StringUtils.removeEnd(line, lineSep);
    ArrayList jsonmap = new ArrayList();
    //20170802修改split("")为split("",-1)
    String[] recordname = schema.split(",",-1);
    String[] recordvalue = newLine.split(",",-1);
    //20170802schema中的字段数，不能超过日志文件中的记录数
    //20180803schema中定义需要跳过的字段，用SKIP标记
    if(recordname.length <= recordvalue.length){
      for ( int i = 0;i<recordname.length;i++){
        if(recordname[i].equals("SKIP")){
          continue;
        }else{
          if(recordname[i].split(" ")[1].matches("string")){
            jsonmap.add("\""+recordname[i].split(" ")[0]+"\""+":"+"\""+recordvalue[i]+"\"");
          }else{
            jsonmap.add("\""+recordname[i].split(" ")[0]+"\""+":"+recordvalue[i]);
          }
        }
      }
      Event event = EventBuilder.withBody(jsonmap.toString().replace("[\"","{\"").replaceAll("]$","}"), Charsets.UTF_8);
      if (addByteOffset == true) {
        event.getHeaders().put(BYTE_OFFSET_HEADER_KEY,posTmp.toString());
      }
      return event;
    }else{
      logger.error("Schema {} length is bigger then log record {} length,Please check again!",schema,newLine);
      return null;
    }
  }

  /*按行读取文件*/
  private String readLine() throws IOException {
    ByteArrayDataOutput out = ByteStreams.newDataOutput(300);
    int i = 0;
    int c;
    while ((c = raf.read()) != -1) {
      i++;
      out.write((byte) c);
      if (c == LINE_SEP.charAt(0)) {
        break;
      }
    }
    if (i == 0) {
      return null;
    }
    return new String(out.toByteArray(), Charsets.UTF_8);
  }

  public void close() {
    try {
      raf.close();
      raf = null;
      long now = System.currentTimeMillis();
      setLastUpdated(now);
    } catch (IOException e) {
      logger.error("Failed closing file: " + path , e);
    }
  }

  public static class CompareByLastModifiedTime implements Comparator<File> {
    public int compare(File f1, File f2) {
      return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
    }
  }
}
