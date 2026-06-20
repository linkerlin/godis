执行增强记忆维护：
1. 用 memory_status 工具查看当前记忆系统状态
2. 如果 L1 索引超过 30 行，用 view 读取 L1 索引文件，移除冗余/过时条目，用 write 写回
3. 用 memory_query 搜索低质量记忆（无关键词、过时信息），用 start_long_term_update 清理
4. 检查 L2 事实文件 (.chong/memory/facts.md) 是否有重复条目，合并后写回
5. 报告做了哪些清理工作及原因