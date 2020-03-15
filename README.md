# gDFS

简单的分布式文件系统，有以下实现功能：
- 文件的上传、下载、追加和删除
- 异步写入
- 文件的分片和冗余
- 使用心跳机制处理故障
详细的说明可以查看**云计算.pdf**

## 项目结构

gDFS 下有三个子模块，分别是

- proto（定义所有的 RPC 调用）
- nameNode
- dataNode

在在 name node 和 data node 之间使用 RPC 进行通信（gRPC），NameNode 向 DataNode 传输**逻辑分区**，在 DataNode 完成存储之后，向 NameNode 汇报相应块在自己的记录中所处的**物理块号**。

## Protocol Buffer

### master

在第一轮迭代，master 作为 client，调用 slave 的服务。master 要负责对用户给出的文件进行边读边分片，然后调用 slave 的函数来存储。当 slave 返回成功之后，master 应当保存相应的逻辑文件分块到物理文件块到映射表。在处理读请求时，master 查看映射关系，到相应的 slave 那里去读相应的块。master 有以下的 RPC 方法
```protobuf
service Master{
    rpc register(RegisterRequest) returns (RegisterResponse) {}
    rpc reportDataWriteStatus(WriteReportRequest) returns (WriteReportReply) {}
    rpc reportDataDeleteStatus(DeleteReportRequest) returns (DeleteReportReply) {}
    rpc heartBeat(stream HeartBeatInfo) returns (NullReply) {}
    rpc readTable(TableName) returns (TableContent) {}
    rpc updateTable(Table) return (Status) {}
}
```

### slave

在第一轮迭代，slave 作为 server，接受 master 的调用。初步确定有以下三个方法，具体实现参看 proto 模块 

```protobuf
service Slave{
    rpc addNewDataNode (PeerInfo) returns (DataNodeReply) {}
    rpc writeToBlock (WriteRequest) returns (WriteReply) {}
    rpc readBlockByID (ReadBlockRequest) returns (ReadBlockReply) {}
    rpc deleteBlockByID (DeleteBlockRequest) returns (DeleteBlockReply) {}
}

```

## 迭代目标

### 第一轮

1. 单层目录
2. 上传下载
3. 普遍的文件
4. 无 Client
5. master 节点直接挨个写

### 第二轮

1. 修改（可能只有 **append** ）删除
2. master 节点只需要等待第一个 slave 返回，而 slave 节点内部负责进行相应的备份

### 第三轮 

1. 心跳
2. 异常处理
