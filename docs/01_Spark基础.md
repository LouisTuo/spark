# spark基础
## 1.Spark工作原理
+ 分布式
+ 主要基于内存（少数继承于磁盘）
+ 迭代式计算
## 2.RDD
RDD（Resilient Distributed Dataset）叫做`弹性`分布式数据集，是Spark中最基本的数据抽象
它代表一个不可变、**可分区**、里面的元素可并行计算的**集合**
+ 一个RDD，在逻辑上，抽象的代表着一个HDFS文件
但是实际上是分区的，不同分区散落在Spark集群中的不同节点上
+ 弹性特点：自动进行内存和磁盘之前的权衡和计算
+ 容错性：当节点发生故障时，能立即从数据源重新获取数据

## 3.Spark开发
+ Spark核心编程：离线批处理/延迟性的交互性数据处理
  + 第一，定义初始的RDD，也就是说定义第一个RDD从哪里读取数据，HDFS，Linux本地文件，程序中的集合
  + 第二，定义对RDD的计算操作，这个在Spark里称为算子，map,reduce,flatMap,groupByKey,
  + 第三，循环往复
  + 第四，获取最终数据，保存数据
+ SQL查询
+ 

## 4.Spark共享变量
Spark中有2中共享变量
+ Broacdcast(广播变量)，sparkContext.broacdcast()方法
+ Accumulator(累加变量)，多个节点对同一个变量累加操作



