功能说明
1.	能够监控多级目录，定义监控的其实目录，其子文件夹都会被监控，如果满足日志文件以.log结尾，并且日志文件的上级目录需要有该日志文件对应表的描述文件，描述文件名称为：表名.schema， 其中schema文件中的内容为该表的字段名和字段类型；例如：
2.	能够实现文件边读边写（写入后需要关闭文件句柄，Python写入时内容在缓存中，关闭句柄后才会写入文件）每行要求有换行符
3.	Event header中携带的信息包括，日志文件每一级目录所表示的含义，filepattern:{目录含义：目录名称，……}，
例如：
日志文件路径：E:\DBLog\tab_login\tab_login_20170712_10.log
配置中的模式信息：sd1.sources.r1.directoryPattern = disk\\logdir\\log_type\\logname
添加到header中的信息：disk=E:;logdir = DBLog;log_type = tab_login;logname = tab_login_20170712_10.log
4.	Event body中携带的信息为，日志描述文件中的字段名和日志文件中的值进行对应，最终event body为json格式，body:{字段名：值，……..}
日志描述文件中的字段间用英文逗号分隔；对于中间需要跳过的字段用SKIP表示；对于最后要省略的字段可以直接跳过，例如：
5.	能够实现断开重写，写入信息记录在status_fn所定义的文件中，内容：E:\DBLog\tab_race_end,\tab_race_end_20170725_12.log,10000，最深一级文件夹占一行，表示当前文件夹下读到的最后一个文件和读到的行数。
例如：
 
配置文件：

* 定义agent sd1的组件名称，sources,sink,channels
> sd1.sources = r1
> 
> sd1.sinks = k1
> 
> sd1.channels = c1

* 定义sources属性
>##### 定义source类
> sd1.sources.r1.type = com.zjw.source.TailSubDirectorySource
>##### 定义记录文件信息的文件
> sd1.sources.r1.status_fn = E:\\DBLog\\status_fn.txt
>##### 定义监控的起始目录（例如：日志文件存放位置为：/home/zjw/Documents/log1/flume, /home/zjw/Documents/log2/flume等，则定义的起始目录为/home/zjw/Documents）
> sd1.sources.r1.spoolDirectory = E:\\DBLog
>##### 定义每一级目录的含义，需要一直说明到文件的位置（例如：/home/zjw/Documents/log1/flume/example.log，该路径的每一级目录的含义都需说明）
> sd1.sources.r1.directoryPattern = disk\\logdir\\log_type\\logname
具体配置参考：taidir.conf
自定义组件存放位置：
	在安装目录中新建文件夹plugins/TailSubDirectorySource/lib,放入自定义组件jar包
	 
windows执行命令：
>进入flume安装文件夹下执行：
>
* 对于单个自定义组件命令：
.\bin\flume-ng agent -n sd1 -conf conf -conf-file conf\taildir.conf -C D:\apache-flume-1.6.0\plugins\TailSubDirectorySource\lib\flume_spool_dir.jar -property "flume.root.logger=INFO,LOGFILE,console"
* 对于多个自定义组件命令：
.\bin\flume-ng agent -n dfdjcn_mysql -conf conf -conf-file conf\dfdjcn-mysql.conf -C "D:\Tool\apache-flume-1.6.0\plugins\TailSubDirectorySource\lib\flume_spool_dir.jar;D:\Tool\apache-flume-1.6.0\plugins\MysqlSinkExt\lib\MySQLSink.jar" -property "flume.root.logger=INFO,LOGFILE,console"
