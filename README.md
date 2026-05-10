# 智慧出行数据集成处理系统

------

## 一、项目背景

随着人们对出行的需求日益增加，出行的安全问题，出行的便捷问题等问题日益突出，特别是安全出行是我们每个人都迫切需要的，为了增加出行的便捷，提高出行的安全，对我们乘车的细节以及发生点我们迫切的需要及时知道，为此特地通过大数据的手段来处理我们海量的出行数据，做到乘车轨迹的的细节回放，虚拟车站的设置，订单以及用户乘车行为、司机行为统计等功能，实现用户的出行统计，用于保障乘车安全，制定用户的“杀熟”策略等。

------

## 二、模块介绍和技术选型

1. 实时接入：海口订单 + 成都 GPS（Structured Streaming）

2. 批处理：虚拟车站选取（空间计算 + 行政区匹配）

3. 实时同步：MySQL → Kafka → HBase（Maxwell）

4. 批处理：订单 + 用户行为联表统计（自定义 HBase 数据源）


------

## 三、项目环境

**2核4GB × 3台（共12GB）**
**centos9**

## 📦 集群组件版本清单

| 组件          | 版本   | 安装路径                              | 备注                     |
| :------------ | :----- | :------------------------------------ | :----------------------- |
| **Hadoop**    | 3.3.6  | `/opt/module/hadoop-3.3.6`            | HDFS + YARN              |
| **Spark**     | 3.5.8  | `/opt/module/spark-3.5.8-bin-hadoop3` | 含 Spark SQL / Streaming |
| **HBase**     | 2.5.8  | `/opt/module/hbase-2.5.8`             | 含 Hadoop3 适配包        |
| **Kafka**     | 3.6.2  | `/opt/module/kafka_2.12-3.6.2`        | Scala 2.12 版本          |
| **Flume**     | 1.11.0 | `/opt/module/flume-1.11.0`            | 日志采集                 |
| **Maxwell**   | 1.43.2 | `/opt/module/maxwell-1.43.2`          | CDC 工具                 |
| **ZooKeeper** | 3.6.3  | `/opt/module/zookeeper-3.6.3`         | 分布式协调               |
| **Hive**      | 4.0.0  | `/opt/module/hive-4.0.0`              | 数据仓库（备选）         |
| **Tez**       | 0.10.3 | `/opt/module/tez-0.10.3`              | Hive 引擎（备选）        |

------

## 🔧 JDK 版本（多版本共存）

| 用途                                           | JDK 版本      | 路径                                                    |
| :--------------------------------------------- | :------------ | :------------------------------------------------------ |
| **主要 JDK**（Hadoop / HBase / Spark / Kafka） | **1.8.0_202** | `/opt/module/jdk1.8.0_202`                              |
| **Maxwell 专用**（兼容性需要）                 | **11.0.20.1** | `/usr/lib/jvm/java-11-openjdk-11.0.20.1.1-2.el9.x86_64` |

> ✅ Maxwell 单独使用 Java 11，不影响其他组件

------

## 🌐 集群节点与服务分布（3 节点）

| 服务 / 节点              | hadoop2427 | hadoop2428 | hadoop2429 |
| :----------------------- | :--------- | :--------- | :--------- |
| **HDFS NameNode**        | ❌          | ❌          | ✅          |
| **HDFS DataNode**        | ✅          | ✅          | ✅          |
| **YARN ResourceManager** | ❌          | ✅          | ❌          |
| **YARN NodeManager**     | ✅          | ✅          | ✅          |
| **HBase Master**         | ❌          | ❌          | ✅          |
| **HBase RegionServer**   | ✅          | ✅          | ✅          |
| **Kafka Broker**         | ✅          | ✅          | ✅          |
| **ZooKeeper**            | ✅          | ✅          | ✅          |
| **Spark (Client)**       | ✅          | ✅          | ✅          |
| **Flume**                | ✅          | ❌          | ❌          |
| **Maxwell**              | ✅          | ❌          | ❌          |
| **MySQL**                | ✅          | ❌          | ❌          |

----------

## 四、项目架构

**轨迹监控**

<img width="1029" height="341" alt="image-20260510191026367" src="https://github.com/user-attachments/assets/6a8eaef1-05ab-4397-8a45-0ac99c092548" />


**虚拟车站**

<img width="718" height="876" alt="image-20260510190904804" src="https://github.com/user-attachments/assets/ee52260a-0d8d-416f-8961-757164b63dc8" />




**Maxwell采集binlog日志**

<img width="1018" height="291" alt="image-20260510191238339" src="https://github.com/user-attachments/assets/2264f73a-c22e-4906-b7db-96ebc1a1f128" />




**订单/司机/用户统计**

（略）
