#flume_spool_dir
##功能说明
>1.	能够监控多级目录，定义监控的其实目录，其子文件夹都会被监控，而且每个表都需要有对应的日志文件
>2.	能够实现文件边读边写（写入后需要关闭文件句柄，Python写入时内容在缓存中，关闭句柄后才会写入文件）每行要求有换行符
>3.	Event header中携带的信息包括，日志文件每一级目录所表示的含义，filepattern:{目录含义：目录名称，……}
>4.	Event body中携带的信息为，日志描述文件中的字段名和日志文件中的值进行对应，body:{字段名：值，……..}
>5.	能够实现断开重写，写入信息记录在positionFile所定义的文件中[{"inode":278920,"pos":99,"file":"/home/zjw/Documents/tab_create_role/tab_create_role_20170617_13.log"},……]
##命名规则
>日志文件：表名_日期_小时.log，文件中的字段以英文逗号分隔，每行都需要以换行结尾。
>描述文件：表名.schema，内容为表的字段，文件中的字段以英文逗号分隔，需要以换行结尾,每张表必须有描述文件，并且描述文件中字段数量和日志文件中的字段数量需要保持一致。
##配置文件：
* 定义agent sd1的组件名称，sources,sink,channels
* sd1.sources = r1
* sd1.sinks = k1
* sd1.channels = c1
###定义sources属性
* 定义source类
> sd1.sources.r1.type = com.zjw.source.TailSubDirectorySource
* 定义记录文件信息的文件
> sd1.sources.r1.positionFile = /home/zjw/Documents/log/flume/taildir_position.json
* 定义监控的起始目录（例如：日志文件存放位置为：/home/zjw/Documents/log1/flume, /home/zjw/Documents/log2/flume等，则定义的起始目录为/home/zjw/Documents）
> sd1.sources.r1.spoolDirectory = /home/zjw/Documents
* 定义每一级目录的含义，需要一直说明到文件的位置（例如：/home/zjw/Documents/log1/flume/example.log，该路径的每一级目录的含义都需说明）
> sd1.sources.r1.directoryPattern = /home/user/path3/log/soft/filename
