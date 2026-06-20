验证已有 SOP 技能：
1. 用 view 读取 .chong/memory/sop/ 目录下的所有 SOP 文件
2. 对每个 SOP，检查：文件是否存在、步骤描述是否清晰、是否有可执行脚本且路径正确
3. 如果某 SOP 的脚本存在，用 bash 运行验证（dry-run 模式）
4. 用 sop_version 工具标记已验证的 SOP（verify action）
5. 用 start_long_term_update 更新任何过时信息
6. 报告每个 SOP 的验证状态