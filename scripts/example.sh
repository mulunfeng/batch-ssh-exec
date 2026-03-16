#!/bin/bash
# 示例脚本：查看系统信息

echo "================================"
echo "系统信息收集"
echo "================================"

echo "主机名: $(hostname)"
echo "当前时间: $(date)"
echo "系统运行时间:"
uptime
echo ""
echo "内存使用:"
free -h
echo ""
echo "磁盘使用:"
df -h | grep -E "^/dev|Filesystem"
echo ""
echo "CPU信息:"
lscpu | grep -E "^Model name|^CPU\(s\)"
echo "================================"