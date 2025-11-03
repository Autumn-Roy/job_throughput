任务吞吐量测评工具

概述

本工具用于测试超算集群的任务吞吐效率。它能够自动化生成不同时长的作业脚本、批量提交任务作业、实时监控作业状态、收集任务状态数据并生成报告。

功能特点

• 自动化脚本生成：多种不同时长、不同节点要求的作业，模拟真实情况

• 精准测量：准确记录作业运行的状态详情

• 全面监控：实时监控队列的作业状态，自动提交作业脚本运行

• 专业分析报告：作业运行情况明细、系统吞吐率、系统资源利用率等指标

• 场景化覆盖：支持灵活配置，根据不同测试集群调整配置文件即可


系统要求

• Python 3.6+

• 必需Python包：

  • `pandas`

  • `matplotlib`

  • `tabulate`

  • `sqlite3`


安装步骤

1. 克隆仓库或下载源代码

使用方法

1. 准备配置文件(文本格式)，示例如下：

```json
{
   "total_test_hours": 4.5,
   "total_nodes": 64,
   "queue_name": "kshcnormal",
   "jobs": [
     {
       "nodes": 10,
       "count": 10,
       "durations": [
         {"minutes": 30, "ratio": 0.3},
         {"minutes": 60, "ratio": 0.5},
         {"minutes": 120, "ratio": 0.2}
       ]
     },
     {
       "nodes": 4,
       "count": 30,
       "durations": [
         {"minutes": 30, "ratio": 0.3},
         {"minutes": 60, "ratio": 0.5},
         {"minutes": 120, "ratio": 0.2}
       ]
     },
     ...
   ]
 }
```

2. 运行基准测试：

```bash
python job_throughput.py config.json
```

配置参数说明

| 参数名 | 类型 | 描述 | 默认值 |
|--------|------|------|--------|
| total_test_hours | float | 测试总时长（小时） | 4.5 |
| total_nodes | integer | 系统总节点数 | 64 |
| queue_name | string | 队列名称 | kshcnormal |
| jobs | array | 作业配置数组 | 无(必填) |
| jobs[].nodes | integer | 每个作业使用的节点数 | 无(必填) |
| jobs[].count | integer | 该类型作业的数量 | 无(必填) |
| jobs[].durations | array | 作业时长分布配置| 无(必填) |
| jobs[].durations[].minutes | integer | 作业运行时长（分钟） | 无(必填) |
| jobs[].durations[].ratio | float | 该时长作业的比例（0-1之间） | 无(必填) |

输出结果

测试完成后将生成：
1. 数据库文件：job_throughput.db，包含所有作业的详细信息（JobID、节点数、时长、提交时间、脚本文件、完成状态）
2. PDF报告文件：job_throughput_report.pdf，包含作业运行情况明细表、测评指标计算结果等
3. SLURM脚本模板文件：xxx.slurm，对应不同时长的作业执行脚本
4. 作业提交过程记录：包含每个作业提交成功/失败状态
5. 最终统计信息：控制台输出，包含任务吞吐量、系统吞吐率、系统资源利用率

注意事项

1. 确保运行工具的用户能够正常使用测试队列提交任务，并且可使用的节点总数大于等于配置文件中的数量
2. 运行测试的时间较长时，请注意保持连接畅通，发生程序中断后，需要手动取消之前提交的所有任务并重新运行
3. 配置文件中的作业节点数、运行时长请逐一匹配
4. 建议至少在3个不同时间段执行测试程序以获得准确结果

技术支持

如有任何问题，请联系开发者或提交issue。