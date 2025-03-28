# Mysql亿级数据的导入导出


## 脚本方案
按 id > 上次已处理的 id order by id limit 1000 的方式，从前到后批次查询、处理、批次写入目标表。批次大小先设置在1000条左右，这个数值在大多数场景下表现良好，既不会让单次查询太重，也能保证足够的处理效率。

单个批次可以加上事务包装，确保每批数据的处理要么全部成功，要么全部失败回滚。这样可以避免出现部分数据处理成功而部分失败的情况，保证数据一致性。尤其是在进行聚合计算后插入新表的场景，事务能确保源表读取和目标表写入的原子性。

> 每个批次处理完成后加入适当的sleep时间，比如0.5秒或1秒，这样可以有效控制每秒的事务处理量(TPS)。防止数据库负载过高，避免影响其他业务。

为了便于监控运行状态，每处理10万条记录写一次详细日志。日志中包含当前处理进度、处理速率、已耗时间等信息。无论处理成功、失败还是发生异常，都应记录对应的日志信息，这样出现问题时能快速定位。良好的日志系统是长时间运行批处理任务的重要保障。


由于亿级数据处理通常无法一次完成，很可能需要分多次执行，所以把已处理的最大id记录在某个地方(专门的状态表中)。这样下次运行时就可以从这个id继续处理，避免重复工作或遗漏数据。主要是为了有效应对程序中断、服务器重启等突发情况。

## 中间件方案
部署NiFi和Doris, 适合需要对数据进行复杂转换和多维分析的场景。