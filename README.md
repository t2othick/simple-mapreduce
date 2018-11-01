# simple-mapreduce

为了给团队介绍 mapreduce 写的简化版框架

相对于完整版的分布式框架，还需要完善：
1. 可以跨进程调用的 RPC 框架 - thrift、grpc 等
2. 能够注册、管理和发现 worker，帮助记录临时文件位置的设施 - Zookeeper
3. 能够跨机器访问的文件系统 NFS - GFS、HDFS、Moose-FS 等
4. worker 的执行超时管理，以及 worker 执行失败后的重试
5. master 的执行进度保存，以及失败后重新恢复
