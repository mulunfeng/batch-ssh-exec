#!/usr/bin/env python3
"""
批量远程命令执行工具

使用方法:
    python batch_exec.py -c nodes.yaml -x "uptime"
    python batch_exec.py -c nodes.yaml -s ./script.sh
    python batch_exec.py -c nodes.yaml -x "df -h" --node web-server-1

交互式模式:
    python batch_exec.py -c nodes.yaml --interactive
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import re
import csv
import subprocess
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Any, Callable
import json
import hashlib
from datetime import datetime, timedelta
from collections import defaultdict

import paramiko
import yaml
from paramiko.ssh_exception import SSHException, AuthenticationException

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

try:
    from cryptography.fernet import Fernet
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from croniter import croniter
    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
    from rich.layout import Layout
    from rich.live import Live
    from rich.text import Text
    HAS_RICH = True
except ImportError:
    HAS_RICH = False


# ============== 日志配置 ==============
def setup_logging(verbose: bool = False) -> logging.Logger:
    """配置日志系统"""
    logger = logging.getLogger("batch_exec")
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# ============== 安全功能 ==============
def generate_key() -> bytes:
    """生成加密密钥"""
    if not HAS_CRYPTO:
        raise ImportError("需要安装 cryptography 库: pip install cryptography")
    return Fernet.generate_key()


def encrypt_password(password: str, key: bytes) -> str:
    """加密密码"""
    if not HAS_CRYPTO:
        raise ImportError("需要安装 cryptography 库")
    f = Fernet(key)
    encrypted = f.encrypt(password.encode())
    return encrypted.decode()


def decrypt_password(encrypted_password: str, key: bytes) -> str:
    """解密密码"""
    if not HAS_CRYPTO:
        raise ImportError("需要安装 cryptography 库")
    f = Fernet(key)
    decrypted = f.decrypt(encrypted_password.encode())
    return decrypted.decode()


def get_or_create_key(key_file: str = None) -> bytes:
    """获取或创建加密密钥"""
    if key_file is None:
        key_file = os.path.expanduser("~/.batch_exec_key")

    if os.path.exists(key_file):
        with open(key_file, 'rb') as f:
            return f.read()
    else:
        key = generate_key()
        with open(key_file, 'wb') as f:
            f.write(key)
        return key


def verify_host_fingerprint(client: paramiko.SSHClient, host: str, port: int,
                            fingerprint_file: str = None, auto_accept: bool = False,
                            logger: logging.Logger = None) -> bool:
    """验证主机密钥指纹"""
    logger = logger or logging.getLogger("batch_exec")

    if fingerprint_file is None:
        fingerprint_file = os.path.expanduser("~/.batch_exec_known_hosts")

    # 获取连接时收到的主机密钥
    transport = client.get_transport()
    if not transport:
        return False

    host_key = transport.get_remote_server_key()
    if not host_key:
        return False

    fingerprint = host_key.get_fingerprint().hex()
    key_type = host_key.get_name()

    # 检查是否已在已知主机列表中
    known_hosts = {}
    if os.path.exists(fingerprint_file):
        try:
            with open(fingerprint_file, 'r') as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 3:
                        known_hosts[f"{parts[0]}:{parts[1]}"] = parts[2]
        except Exception:
            pass

    host_key_id = f"{host}:{port}"

    if host_key_id in known_hosts:
        if known_hosts[host_key_id] == fingerprint:
            logger.debug(f"主机密钥验证成功: {host}:{port}")
            return True
        else:
            logger.error(f"主机密钥指纹不匹配! 可能存在安全风险")
            return False

    # 新主机，显示指纹并确认
    logger.info(f"新主机 {host}:{port}")
    logger.info(f"密钥类型: {key_type}")
    logger.info(f"指纹: {fingerprint}")

    if auto_accept:
        logger.info("自动接受新主机密钥")
    else:
        print("是否接受此主机密钥? (yes/no): ", end='')
        try:
            answer = input().strip().lower()
            if answer not in ('yes', 'y'):
                logger.warning("拒绝主机密钥")
                return False
        except EOFError:
            logger.warning("无法读取输入，拒绝主机密钥")
            return False

    # 保存到已知主机列表
    with open(fingerprint_file, 'a') as f:
        f.write(f"{host} {port} {fingerprint}\n")
    logger.info(f"主机密钥已保存到 {fingerprint_file}")

    return True


# ============== 数据类定义 ==============
@dataclass
class NodeConfig:
    """节点配置"""
    name: str
    host: str
    port: int
    username: str
    password: Optional[str] = None
    private_key: Optional[str] = None
    sudo_password: Optional[str] = None
    sudo_user: Optional[str] = None
    tags: list[str] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """执行结果"""
    node_name: str
    host: str
    success: bool
    stdout: str
    stderr: str
    exit_code: int
    duration: float
    error: Optional[str] = None


@dataclass
class Settings:
    """全局设置"""
    timeout: int = 30
    parallel: bool = True
    max_workers: int = 5
    retry_times: int = 3
    retry_delay: float = 1.0
    sudo_password: Optional[str] = None
    ssh_agent_forwarding: bool = False
    verify_host_key: bool = False


@dataclass
class TransferResult:
    """文件传输结果"""
    node_name: str
    host: str
    success: bool
    local_path: str
    remote_path: str
    bytes_transferred: int
    duration: float
    error: Optional[str] = None


@dataclass
class HealthStatus:
    """节点健康状态"""
    node_name: str
    host: str
    connected: bool
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    disk_usage: Optional[float] = None
    uptime: Optional[str] = None
    load_avg: Optional[str] = None
    error: Optional[str] = None


@dataclass
class ServiceStatus:
    """服务状态"""
    node_name: str
    host: str
    service_name: str
    is_running: bool
    is_enabled: bool
    active_since: Optional[str] = None
    error: Optional[str] = None


@dataclass
class ExecutionHistory:
    """执行历史记录"""
    timestamp: str
    command: Optional[str] = None
    script: Optional[str] = None
    transfer_type: Optional[str] = None
    monitor_type: Optional[str] = None
    node_count: int = 0
    success_count: int = 0
    fail_count: int = 0
    duration: float = 0.0
    nodes: list[str] = field(default_factory=list)


# ============== 新增数据类 ==============
@dataclass
class ScheduledTask:
    """定时任务"""
    name: str
    cron_expression: str
    config_file: str
    command: Optional[str] = None
    script: Optional[str] = None
    nodes_filter: Optional[str] = None
    tags_filter: Optional[str] = None
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    enabled: bool = True


@dataclass
class WorkflowTask:
    """工作流任务"""
    name: str
    command: Optional[str] = None
    script: Optional[str] = None
    depends_on: list[str] = field(default_factory=list)
    nodes: list[str] = field(default_factory=list)
    on_failure: Optional[str] = None  # continue, stop, retry
    retry_count: int = 0


@dataclass
class MetricsData:
    """指标数据"""
    node_name: str
    host: str
    timestamp: str
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    disk_usage: Optional[float] = None
    network_in: Optional[float] = None
    network_out: Optional[float] = None
    load_avg: Optional[str] = None


@dataclass
class AuditLog:
    """审计日志"""
    timestamp: str
    user: str
    action: str
    target_nodes: list[str]
    result: str  # success, failed, blocked
    command: Optional[str] = None
    details: Optional[str] = None


@dataclass
class NodeGroup:
    """节点分组"""
    name: str
    description: Optional[str] = None
    nodes: list[str] = field(default_factory=list)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class PluginInfo:
    """插件信息"""
    name: str
    path: str
    version: str
    enabled: bool = True
    hooks: list[str] = field(default_factory=list)


@dataclass
class ConfigDiff:
    """配置差异"""
    node_name: str
    file_path: str
    local_content: Optional[str] = None
    remote_content: Optional[str] = None
    diff_lines: list[str] = field(default_factory=list)
    match: bool = False


# ============== 调度与自动化核心类 ==============
class CronScheduler:
    """Cron定时任务调度器"""

    def __init__(self, cron_expression: str, logger: Optional[logging.Logger] = None):
        if not HAS_CRONITER:
            raise ImportError("需要安装 croniter 库: pip install croniter")
        self.cron_expression = cron_expression
        self.cron = croniter(cron_expression)
        self.logger = logger or logging.getLogger("batch_exec")
        self._running = False

    def get_next_run_time(self) -> datetime:
        """获取下次执行时间"""
        return self.cron.get_next(datetime)

    def start(self, task_func: Callable, *args, once: bool = False, **kwargs):
        """启动调度器

        Args:
            task_func: 要执行的任务函数
            once: 是否只执行一次后退出
        """
        self._running = True
        self.logger.info(f"定时任务启动: cron表达式 '{self.cron_expression}'")

        while self._running:
            next_time = self.get_next_run_time()
            now = datetime.now()
            sleep_duration = (next_time - now).total_seconds()

            if sleep_duration > 0:
                self.logger.info(f"下次执行时间: {next_time.strftime('%Y-%m-%d %H:%M:%S')}, 等待 {sleep_duration:.0f} 秒")
                time.sleep(sleep_duration)

            if self._running:
                self.logger.info(f"开始执行定时任务: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
                try:
                    task_func(*args, **kwargs)
                    self.logger.info("定时任务执行完成")
                except Exception as e:
                    self.logger.error(f"定时任务执行失败: {e}")

                if once:
                    self.logger.info("单次执行模式，退出调度器")
                    break

    def stop(self):
        """停止调度器"""
        self._running = False
        self.logger.info("定时任务已停止")


class WorkflowExecutor:
    """工作流任务编排执行器"""

    def __init__(self, workflow_def: str | dict, nodes: list[NodeConfig],
                 settings: Settings, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("batch_exec")
        self.nodes = nodes
        self.settings = settings
        self.tasks: list[WorkflowTask] = []
        self.completed_tasks: set[str] = set()
        self.failed_tasks: set[str] = set()
        self.task_results: dict[str, list[ExecutionResult]] = {}

        # 解析workflow定义
        self._parse_workflow(workflow_def)

    def _parse_workflow(self, workflow_def: str | dict):
        """解析workflow定义"""
        if isinstance(workflow_def, str):
            # 字符串格式: "task1->task2->task3"
            if workflow_def.endswith('.yaml') or workflow_def.endswith('.yml'):
                # YAML文件路径
                workflow_path = os.path.expanduser(workflow_def)
                if not os.path.exists(workflow_path):
                    raise FileNotFoundError(f"Workflow文件不存在: {workflow_path}")
                with open(workflow_path, 'r', encoding='utf-8') as f:
                    workflow_data = yaml.load(f, Loader=SafeLoader)
                tasks_data = workflow_data.get('workflow', {}).get('tasks', [])
            else:
                # 简单链式格式
                task_names = [t.strip() for t in workflow_def.split('->')]
                tasks_data = []
                for i, name in enumerate(task_names):
                    task = {'name': name}
                    if i > 0:
                        task['depends_on'] = [task_names[i-1]]
                    tasks_data.append(task)
        else:
            # 字典格式
            tasks_data = workflow_def.get('tasks', [])

        # 构建WorkflowTask对象
        for task_data in tasks_data:
            task = WorkflowTask(
                name=task_data.get('name', ''),
                command=task_data.get('command'),
                script=task_data.get('script'),
                depends_on=task_data.get('depends_on', []),
                nodes=task_data.get('nodes', []),
                on_failure=task_data.get('on_failure', 'stop'),
                retry_count=task_data.get('retry_count', 0)
            )
            self.tasks.append(task)

        self.logger.info(f"已解析 {len(self.tasks)} 个工作流任务")

    def _can_execute_task(self, task: WorkflowTask) -> bool:
        """检查任务是否可以执行（依赖是否满足）"""
        for dep in task.depends_on:
            if dep not in self.completed_tasks:
                return False
        return True

    def _get_task_nodes(self, task: WorkflowTask) -> list[NodeConfig]:
        """获取任务的目标节点"""
        if task.nodes:
            # 按节点名称过滤
            node_names = set(task.nodes)
            return [n for n in self.nodes if n.name in node_names]
        return self.nodes

    def execute_task(self, task: WorkflowTask, retry_count: int = 0) -> bool:
        """执行单个任务"""
        task_nodes = self._get_task_nodes(task)

        self.logger.info(f"执行任务 '{task.name}' 在 {len(task_nodes)} 个节点上")

        results = []
        for node in task_nodes:
            result = execute_on_node(
                node, task.command, task.script,
                self.settings.timeout, False,
                self.settings.retry_times, self.settings.retry_delay,
                self.logger
            )
            results.append(result)
            print_result(result, False, self.logger)

        self.task_results[task.name] = results
        success = all(r.success for r in results)

        if success:
            self.completed_tasks.add(task.name)
            self.logger.info(f"任务 '{task.name}' 完成")
            return True
        else:
            self.failed_tasks.add(task.name)
            self.logger.error(f"任务 '{task.name}' 失败")

            # 失败处理策略
            if task.on_failure == 'retry' and retry_count < task.retry_count:
                self.logger.info(f"任务 '{task.name}' 重试 ({retry_count + 1}/{task.retry_count})")
                return self.execute_task(task, retry_count + 1)

            return False

    def run(self) -> bool:
        """执行整个工作流"""
        self.logger.info("=" * 60)
        self.logger.info("开始执行工作流")
        self.logger.info("=" * 60)

        # 构建任务执行顺序
        remaining_tasks = list(self.tasks)

        while remaining_tasks:
            # 找出可以执行的任务
            executable_tasks = [t for t in remaining_tasks if self._can_execute_task(t)]

            if not executable_tasks:
                # 检查是否有任务失败导致阻塞
                blocked_tasks = [t for t in remaining_tasks]
                self.logger.error(f"工作流阻塞: 任务 {[t.name for t in blocked_tasks]} 无法执行")
                return False

            # 执行可执行的任务
            for task in executable_tasks:
                success = self.execute_task(task)
                remaining_tasks.remove(task)

                if not success and task.on_failure == 'stop':
                    self.logger.error(f"任务 '{task.name}' 失败，终止工作流")
                    return False

        self.logger.info("=" * 60)
        self.logger.info(f"工作流完成: {len(self.completed_tasks)} 成功, {len(self.failed_tasks)} 失败")
        self.logger.info("=" * 60)

        return len(self.failed_tasks) == 0

    def visualize(self) -> str:
        """可视化任务依赖图"""
        lines = ["工作流任务依赖图:", ""]
        for task in self.tasks:
            deps = ", ".join(task.depends_on) if task.depends_on else "无依赖"
            lines.append(f"  {task.name} -> 依赖: {deps}")
            if task.command:
                lines.append(f"    命令: {task.command}")
            if task.script:
                lines.append(f"    脚本: {task.script}")
        return "\n".join(lines)


class PatrolRunner:
    """定期巡检执行器"""

    def __init__(self, nodes: list[NodeConfig], settings: Settings,
                 interval: int = 3600, checks: Optional[list[str]] = None,
                 report_path: Optional[str] = None, alert_config: Optional[dict] = None,
                 logger: Optional[logging.Logger] = None):
        self.nodes = nodes
        self.settings = settings
        self.interval = interval
        self.checks = checks or ['cpu', 'memory', 'disk', 'uptime']
        self.report_path = report_path or '/tmp/patrol_report.json'
        self.alert_config = alert_config
        self.logger = logger or logging.getLogger("batch_exec")
        self._running = False
        self.patrol_history: list[dict] = []

    def _run_checks_on_node(self, node: NodeConfig) -> dict:
        """在节点上运行巡检"""
        result = {
            'node_name': node.name,
            'host': node.host,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'checks': {},
            'status': 'healthy'
        }

        try:
            pool = ConnectionPool(max_connections=5, logger=self.logger)
            wrapper = SSHClientWrapper(node, self.settings.timeout, pool, False, self.logger)
            wrapper.connect_with_retry(self.settings.retry_times, self.settings.retry_delay)

            for check in self.checks:
                check_result = {'passed': True, 'value': None, 'threshold': None}

                try:
                    if check == 'cpu':
                        exit_code, stdout, _ = wrapper.execute("top -bn1 | grep 'Cpu(s)' | awk '{print $2}' | cut -d'%' -f1", 10)
                        if exit_code == 0 and stdout.strip():
                            check_result['value'] = float(stdout.strip())
                            check_result['threshold'] = 80
                            check_result['passed'] = check_result['value'] < check_result['threshold']

                    elif check == 'memory':
                        exit_code, stdout, _ = wrapper.execute("free | grep Mem | awk '{print $3/$2 * 100.0}'", 10)
                        if exit_code == 0 and stdout.strip():
                            check_result['value'] = float(stdout.strip())
                            check_result['threshold'] = 85
                            check_result['passed'] = check_result['value'] < check_result['threshold']

                    elif check == 'disk':
                        exit_code, stdout, _ = wrapper.execute("df -h / | tail -1 | awk '{print $5}' | cut -d'%' -f1", 10)
                        if exit_code == 0 and stdout.strip():
                            check_result['value'] = float(stdout.strip())
                            check_result['threshold'] = 90
                            check_result['passed'] = check_result['value'] < check_result['threshold']

                    elif check == 'uptime':
                        exit_code, stdout, _ = wrapper.execute("uptime -p", 10)
                        check_result['value'] = stdout.strip() if exit_code == 0 else 'N/A'

                    elif check.startswith('process:'):
                        process_name = check.split(':')[1]
                        exit_code, stdout, _ = wrapper.execute(f"pgrep -x {process_name}")
                        check_result['passed'] = exit_code == 0
                        check_result['value'] = '存在' if exit_code == 0 else '不存在'

                    elif check.startswith('service:'):
                        service_name = check.split(':')[1]
                        status = wrapper.service_status(service_name)
                        check_result['passed'] = status.is_running
                        check_result['value'] = '运行' if status.is_running else '停止'

                except Exception as e:
                    check_result['passed'] = False
                    check_result['error'] = str(e)

                result['checks'][check] = check_result
                if not check_result['passed']:
                    result['status'] = 'unhealthy'

            wrapper.close()

        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)

        return result

    def run_patrol(self) -> dict:
        """执行一次巡检"""
        self.logger.info("=" * 60)
        self.logger.info(f"开始巡检 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        self.logger.info("=" * 60)

        patrol_result = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'nodes': [],
            'summary': {'healthy': 0, 'unhealthy': 0, 'error': 0}
        }

        progress = ProgressBar(len(self.nodes), "巡检进度")

        for node in self.nodes:
            result = self._run_checks_on_node(node)
            patrol_result['nodes'].append(result)
            patrol_result['summary'][result['status']] += 1

            status_icon = "✓" if result['status'] == 'healthy' else "✗"
            self.logger.info(f"[{status_icon}] {result['node_name']} ({result['host']}) - {result['status']}")

            if result['status'] != 'healthy':
                for check_name, check_data in result['checks'].items():
                    if not check_data['passed']:
                        self.logger.warning(f"  - {check_name}: {check_data.get('value', 'N/A')}")

            progress.update(1)

        progress.close()

        # 保存报告
        self.patrol_history.append(patrol_result)
        self._save_report()

        # 发送告警
        if patrol_result['summary']['unhealthy'] > 0 and self.alert_config:
            self._send_alert(patrol_result)

        self.logger.info("=" * 60)
        self.logger.info(f"巡检完成: 健康 {patrol_result['summary']['healthy']}, "
                        f"异常 {patrol_result['summary']['unhealthy']}, "
                        f"错误 {patrol_result['summary']['error']}")
        self.logger.info("=" * 60)

        return patrol_result

    def start(self, once: bool = False):
        """启动定期巡检"""
        self._running = True

        while self._running:
            self.run_patrol()

            if once:
                break

            self.logger.info(f"下次巡检时间: {self.interval} 秒后")
            time.sleep(self.interval)

    def stop(self):
        """停止巡检"""
        self._running = False

    def _save_report(self):
        """保存巡检报告"""
        try:
            with open(self.report_path, 'w', encoding='utf-8') as f:
                json.dump(self.patrol_history, f, ensure_ascii=False, indent=2)
            self.logger.info(f"巡检报告已保存: {self.report_path}")
        except Exception as e:
            self.logger.error(f"保存报告失败: {e}")

    def _send_alert(self, patrol_result: dict):
        """发送异常告警"""
        unhealthy_nodes = [n for n in patrol_result['nodes'] if n['status'] != 'healthy']

        message = f"""【巡检告警】
时间: {patrol_result['timestamp']}
异常节点数: {len(unhealthy_nodes)}

异常节点:
"""
        for node in unhealthy_nodes[:10]:  # 最多显示10个
            message += f"- {node['node_name']} ({node['host']})\n"

        if self.alert_config:
            alert_type = self.alert_config.get('type', 'dingtalk')
            target = self.alert_config.get('target', '')

            if alert_type == 'dingtalk':
                send_dingtalk_alert(target, message)
            elif alert_type == 'wechat':
                send_wechat_alert(target, message)


class RetryManager:
    """失败节点重试管理器"""

    def __init__(self, max_retry: int = 5, retry_delay: float = 60.0,
                 backoff: bool = False, log_file: Optional[str] = None,
                 logger: Optional[logging.Logger] = None):
        self.max_retry = max_retry
        self.retry_delay = retry_delay
        self.backoff = backoff
        self.log_file = log_file or '/tmp/retry_status.json'
        self.logger = logger or logging.getLogger("batch_exec")
        self.retry_status: dict[str, dict] = {}

    def _load_status(self):
        """加载重试状态"""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    self.retry_status = json.load(f)
            except Exception:
                self.retry_status = {}

    def _save_status(self):
        """保存重试状态"""
        try:
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(self.retry_status, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存重试状态失败: {e}")

    def _calculate_delay(self, retry_count: int) -> float:
        """计算重试延迟"""
        if self.backoff:
            # 指数退避
            return self.retry_delay * (2 ** retry_count)
        return self.retry_delay

    def add_failed_node(self, node: NodeConfig, result: ExecutionResult):
        """添加失败节点"""
        self.retry_status[node.name] = {
            'host': node.host,
            'last_error': result.error or result.stderr,
            'retry_count': 0,
            'last_attempt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'pending'
        }
        self._save_status()

    def execute_retries(self, nodes: list[NodeConfig], command: Optional[str],
                        script: Optional[str], settings: Settings) -> list[ExecutionResult]:
        """执行失败节点重试"""
        self._load_status()

        results = []
        nodes_to_retry = [n for n in nodes if n.name in self.retry_status]

        if not nodes_to_retry:
            self.logger.info("没有需要重试的节点")
            return results

        self.logger.info("=" * 60)
        self.logger.info(f"开始重试 {len(nodes_to_retry)} 个失败节点")
        self.logger.info("=" * 60)

        for node in nodes_to_retry:
            status = self.retry_status[node.name]

            while status['retry_count'] < self.max_retry:
                status['retry_count'] += 1
                delay = self._calculate_delay(status['retry_count'] - 1)

                self.logger.info(f"重试节点 '{node.name}' ({status['retry_count']}/{self.max_retry}), "
                                f"等待 {delay:.0f} 秒")

                time.sleep(delay)

                result = execute_on_node(
                    node, command, script,
                    settings.timeout, False,
                    settings.retry_times, settings.retry_delay,
                    self.logger
                )

                results.append(result)
                status['last_attempt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                if result.success:
                    self.logger.info(f"节点 '{node.name}' 重试成功")
                    status['status'] = 'success'
                    del self.retry_status[node.name]
                    break
                else:
                    self.logger.warning(f"节点 '{node.name}' 重试失败: {result.error}")
                    status['last_error'] = result.error or result.stderr

            if status['retry_count'] >= self.max_retry:
                self.logger.error(f"节点 '{node.name}' 达到最大重试次数，放弃重试")
                status['status'] = 'failed'

        self._save_status()

        # 统计结果
        success_count = sum(1 for r in results if r.success)
        failed_count = len(results) - success_count

        self.logger.info("=" * 60)
        self.logger.info(f"重试完成: 成功 {success_count}, 失败 {failed_count}")
        self.logger.info("=" * 60)

        return results

    def clear_status(self):
        """清除重试状态"""
        self.retry_status = {}
        self._save_status()
        self.logger.info("重试状态已清除")


# ============== 多节点协同执行类 ==============
class MasterSlaveExecutor:
    """主从模式执行器 - 主节点先执行成功后再执行从节点"""

    def __init__(self, master_node: NodeConfig, slave_nodes: list[NodeConfig],
                 settings: Settings, logger: Optional[logging.Logger] = None):
        self.master_node = master_node
        self.slave_nodes = slave_nodes
        self.settings = settings
        self.logger = logger or logging.getLogger("batch_exec")
        self.results: list[ExecutionResult] = []

    def execute(self, command: Optional[str], script: Optional[str]) -> bool:
        """执行主从模式"""
        self.logger.info("=" * 60)
        self.logger.info("主从模式执行")
        self.logger.info(f"主节点: {self.master_node.name}")
        self.logger.info(f"从节点: {[n.name for n in self.slave_nodes]}")
        self.logger.info("=" * 60)

        # 1. 先执行主节点
        self.logger.info(f"[主节点] 执行: {self.master_node.name}")
        master_result = execute_on_node(
            self.master_node, command, script,
            self.settings.timeout, False,
            self.settings.retry_times, self.settings.retry_delay,
            self.logger
        )
        self.results.append(master_result)
        print_result(master_result, False, self.logger)

        if not master_result.success:
            self.logger.error(f"主节点 {self.master_node.name} 执行失败，终止从节点执行")
            return False

        self.logger.info(f"主节点执行成功，开始执行 {len(self.slave_nodes)} 个从节点")

        # 2. 主节点成功后，执行从节点
        for slave_node in self.slave_nodes:
            self.logger.info(f"[从节点] 执行: {slave_node.name}")
            slave_result = execute_on_node(
                slave_node, command, script,
                self.settings.timeout, False,
                self.settings.retry_times, self.settings.retry_delay,
                self.logger
            )
            self.results.append(slave_result)
            print_result(slave_result, False, self.logger)

        # 统计结果
        success_count = sum(1 for r in self.results if r.success)
        failed_count = len(self.results) - success_count

        self.logger.info("=" * 60)
        self.logger.info(f"主从执行完成: 成功 {success_count}, 失败 {failed_count}")
        self.logger.info("=" * 60)

        return failed_count == 0


class BatchExecutor:
    """分批次执行器 - 按批次逐组执行节点"""

    def __init__(self, nodes: list[NodeConfig], batch_size: int = 5,
                 batch_delay: float = 10.0, settings: Settings = None,
                 logger: Optional[logging.Logger] = None):
        self.nodes = nodes
        self.batch_size = batch_size
        self.batch_delay = batch_delay
        self.settings = settings or Settings()
        self.logger = logger or logging.getLogger("batch_exec")
        self.results: list[ExecutionResult] = []

    def _split_batches(self) -> list[list[NodeConfig]]:
        """将节点分成批次"""
        batches = []
        for i in range(0, len(self.nodes), self.batch_size):
            batches.append(self.nodes[i:i + self.batch_size])
        return batches

    def execute(self, command: Optional[str], script: Optional[str]) -> bool:
        """分批次执行"""
        batches = self._split_batches()
        total_batches = len(batches)

        self.logger.info("=" * 60)
        self.logger.info("分批次执行模式")
        self.logger.info(f"总节点数: {len(self.nodes)}")
        self.logger.info(f"批次大小: {self.batch_size}")
        self.logger.info(f"总批次数: {total_batches}")
        self.logger.info(f"批次间隔: {self.batch_delay} 秒")
        self.logger.info("=" * 60)

        for batch_idx, batch_nodes in enumerate(batches):
            self.logger.info(f"\n--- 执行批次 {batch_idx + 1}/{total_batches} ---")
            self.logger.info(f"批次节点: {[n.name for n in batch_nodes]}")

            batch_start = time.time()

            # 并行执行当前批次
            with ThreadPoolExecutor(max_workers=len(batch_nodes)) as executor:
                futures = {
                    executor.submit(
                        execute_on_node, node, command, script,
                        self.settings.timeout, False,
                        self.settings.retry_times, self.settings.retry_delay,
                        self.logger
                    ): node for node in batch_nodes
                }

                for future in as_completed(futures):
                    result = future.result()
                    self.results.append(result)
                    print_result(result, False, self.logger)

            batch_duration = time.time() - batch_start
            batch_success = sum(1 for r in self.results[-len(batch_nodes):] if r.success)
            self.logger.info(f"批次 {batch_idx + 1} 完成: {batch_success}/{len(batch_nodes)} 成功, 耗时 {batch_duration:.2f}s")

            # 批次间隔（最后一批不等待）
            if batch_idx < total_batches - 1:
                self.logger.info(f"等待 {self.batch_delay} 秒后执行下一批次...")
                time.sleep(self.batch_delay)

        # 统计结果
        success_count = sum(1 for r in self.results if r.success)
        failed_count = len(self.results) - success_count

        self.logger.info("=" * 60)
        self.logger.info(f"分批次执行完成: 成功 {success_count}, 失败 {failed_count}")
        self.logger.info("=" * 60)

        return failed_count == 0


class LoopExecutor:
    """轮询执行器 - 循环执行直到满足条件"""

    def __init__(self, nodes: list[NodeConfig], until_condition: str,
                 max_loops: int = 100, loop_interval: float = 5.0,
                 settings: Settings = None,
                 logger: Optional[logging.Logger] = None):
        self.nodes = nodes
        self.until_condition = until_condition
        self.max_loops = max_loops
        self.loop_interval = loop_interval
        self.settings = settings or Settings()
        self.logger = logger or logging.getLogger("batch_exec")
        self.results: list[ExecutionResult] = []
        self.loop_count = 0

    def _check_condition(self, results: list[ExecutionResult]) -> bool:
        """检查终止条件是否满足"""
        # 解析条件表达式
        # 支持格式: success_count == N, success_count >= N, fail_count <= N, all_success 等
        condition = self.until_condition.strip()

        if condition == "all_success":
            return all(r.success for r in results)

        if condition == "any_success":
            return any(r.success for r in results)

        if condition == "all_failed":
            return all(not r.success for r in results)

        # 解析数值条件
        import re
        match = re.match(r'(success_count|fail_count|total_count)\s*(==|>=|<=|>|<)\s*(\d+)', condition)
        if match:
            metric = match.group(1)
            operator = match.group(2)
            threshold = int(match.group(3))

            if metric == 'success_count':
                value = sum(1 for r in results if r.success)
            elif metric == 'fail_count':
                value = sum(1 for r in results if not r.success)
            elif metric == 'total_count':
                value = len(results)
            else:
                return False

            if operator == '==':
                return value == threshold
            elif operator == '>=':
                return value >= threshold
            elif operator == '<=':
                return value <= threshold
            elif operator == '>':
                return value > threshold
            elif operator == '<':
                return value < threshold

        return False

    def execute(self, command: Optional[str], script: Optional[str]) -> bool:
        """轮询执行"""
        self.logger.info("=" * 60)
        self.logger.info("轮询执行模式")
        self.logger.info(f"目标节点: {[n.name for n in self.nodes]}")
        self.logger.info(f"终止条件: {self.until_condition}")
        self.logger.info(f"最大循环次数: {self.max_loops}")
        self.logger.info(f"轮询间隔: {self.loop_interval} 秒")
        self.logger.info("=" * 60)

        while self.loop_count < self.max_loops:
            self.loop_count += 1
            self.logger.info(f"\n--- 轮询第 {self.loop_count} 次 ---")

            loop_results = []
            loop_start = time.time()

            for node in self.nodes:
                result = execute_on_node(
                    node, command, script,
                    self.settings.timeout, False,
                    self.settings.retry_times, self.settings.retry_delay,
                    self.logger
                )
                loop_results.append(result)
                self.results.append(result)
                print_result(result, False, self.logger)

            loop_duration = time.time() - loop_start
            loop_success = sum(1 for r in loop_results if r.success)

            self.logger.info(f"轮询 {self.loop_count} 完成: {loop_success}/{len(self.nodes)} 成功, 耗时 {loop_duration:.2f}s")

            # 检查终止条件
            if self._check_condition(self.results):
                self.logger.info(f"终止条件 '{self.until_condition}' 已满足，停止轮询")
                break

            # 轮询间隔
            self.logger.info(f"等待 {self.loop_interval} 秒后继续轮询...")
            time.sleep(self.loop_interval)

        # 统计结果
        success_count = sum(1 for r in self.results if r.success)
        failed_count = len(self.results) - success_count

        self.logger.info("=" * 60)
        self.logger.info(f"轮询执行完成: 总轮询次数 {self.loop_count}, 成功 {success_count}, 失败 {failed_count}")
        self.logger.info("=" * 60)

        return success_count > 0


class FallbackExecutor:
    """故障转移执行器 - 主节点失败自动切换备用节点"""

    def __init__(self, primary_node: NodeConfig, fallback_nodes: list[NodeConfig],
                 settings: Settings, logger: Optional[logging.Logger] = None):
        self.primary_node = primary_node
        self.fallback_nodes = fallback_nodes
        self.settings = settings
        self.logger = logger or logging.getLogger("batch_exec")
        self.results: list[ExecutionResult] = []
        self.success_node: Optional[NodeConfig] = None

    def execute(self, command: Optional[str], script: Optional[str]) -> bool:
        """故障转移执行"""
        self.logger.info("=" * 60)
        self.logger.info("故障转移执行模式")
        self.logger.info(f"主节点: {self.primary_node.name}")
        self.logger.info(f"备用节点: {[n.name for n in self.fallback_nodes]}")
        self.logger.info("=" * 60)

        # 尝试主节点
        self.logger.info(f"[主节点] 尝试执行: {self.primary_node.name}")
        primary_result = execute_on_node(
            self.primary_node, command, script,
            self.settings.timeout, False,
            self.settings.retry_times, self.settings.retry_delay,
            self.logger
        )
        self.results.append(primary_result)
        print_result(primary_result, False, self.logger)

        if primary_result.success:
            self.success_node = self.primary_node
            self.logger.info(f"主节点 {self.primary_node.name} 执行成功")
            self.logger.info("=" * 60)
            self.logger.info("故障转移执行完成: 主节点成功")
            self.logger.info("=" * 60)
            return True

        self.logger.warning(f"主节点 {self.primary_node.name} 执行失败，尝试备用节点")

        # 尝试备用节点
        for idx, fallback_node in enumerate(self.fallback_nodes):
            self.logger.info(f"[备用节点 {idx + 1}] 尝试执行: {fallback_node.name}")
            fallback_result = execute_on_node(
                fallback_node, command, script,
                self.settings.timeout, False,
                self.settings.retry_times, self.settings.retry_delay,
                self.logger
            )
            self.results.append(fallback_result)
            print_result(fallback_result, False, self.logger)

            if fallback_result.success:
                self.success_node = fallback_node
                self.logger.info(f"备用节点 {fallback_node.name} 执行成功")
                self.logger.info("=" * 60)
                self.logger.info(f"故障转移执行完成: 备用节点 {fallback_node.name} 成功")
                self.logger.info("=" * 60)
                return True

            self.logger.warning(f"备用节点 {fallback_node.name} 执行失败")

        # 所有节点都失败
        self.logger.error("所有节点（主节点和备用节点）都执行失败")
        self.logger.info("=" * 60)
        self.logger.info("故障转移执行完成: 全部失败")
        self.logger.info("=" * 60)
        return False

    def get_success_node(self) -> Optional[NodeConfig]:
        """获取成功执行的节点"""
        return self.success_node


# ============== 增强监控与采集类 ==============
@dataclass
class MetricsSample:
    """指标采样数据"""
    timestamp: str
    node_name: str
    host: str
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    disk_usage: Optional[float] = None
    network_in: Optional[float] = None
    network_out: Optional[float] = None
    load_avg: Optional[str] = None
    process_count: Optional[int] = None


@dataclass
class BaselineData:
    """基准数据"""
    node_name: str
    host: str
    timestamp: str
    cpu_avg: Optional[float] = None
    cpu_max: Optional[float] = None
    memory_avg: Optional[float] = None
    memory_max: Optional[float] = None
    disk_avg: Optional[float] = None
    disk_max: Optional[float] = None
    load_avg_avg: Optional[float] = None
    samples_count: int = 0


@dataclass
class AnomalyReport:
    """异常报告"""
    timestamp: str
    node_name: str
    host: str
    metric_name: str
    current_value: float
    baseline_value: float
    deviation_percent: float
    threshold: float
    is_anomaly: bool
    severity: str  # low, medium, high


class MetricsCollector:
    """实时指标采集器"""

    def __init__(self, nodes: list[NodeConfig], metrics: list[str],
                 duration: int = 60, interval: int = 5,
                 settings: Settings = None,
                 logger: Optional[logging.Logger] = None):
        self.nodes = nodes
        self.metrics = metrics or ['cpu', 'memory', 'disk', 'net']
        self.duration = duration
        self.interval = interval
        self.settings = settings or Settings()
        self.logger = logger or logging.getLogger("batch_exec")
        self.samples: list[MetricsSample] = []
        self._running = False

    def _collect_single_node(self, node: NodeConfig) -> MetricsSample:
        """采集单个节点的指标"""
        sample = MetricsSample(
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            node_name=node.name,
            host=node.host
        )

        try:
            pool = ConnectionPool(max_connections=5, logger=self.logger)
            wrapper = SSHClientWrapper(node, self.settings.timeout, pool, False, self.logger)
            wrapper.connect_with_retry(self.settings.retry_times, self.settings.retry_delay)

            # 采集CPU
            if 'cpu' in self.metrics:
                exit_code, stdout, _ = wrapper.execute("top -bn1 | grep 'Cpu(s)' | awk '{print $2}' | cut -d'%' -f1", 10)
                if exit_code == 0 and stdout.strip():
                    sample.cpu_usage = float(stdout.strip())

            # 采集内存
            if 'memory' in self.metrics:
                exit_code, stdout, _ = wrapper.execute("free | grep Mem | awk '{print $3/$2 * 100.0}'", 10)
                if exit_code == 0 and stdout.strip():
                    sample.memory_usage = float(stdout.strip())

            # 采集磁盘
            if 'disk' in self.metrics:
                exit_code, stdout, _ = wrapper.execute("df -h / | tail -1 | awk '{print $5}' | cut -d'%' -f1", 10)
                if exit_code == 0 and stdout.strip():
                    sample.disk_usage = float(stdout.strip())

            # 采集网络
            if 'net' in self.metrics:
                # 网络入流量
                exit_code, stdout, _ = wrapper.execute("cat /proc/net/dev | grep -E 'eth0|ens' | awk '{print $2}'", 10)
                if exit_code == 0 and stdout.strip():
                    sample.network_in = float(stdout.strip()) / 1024  # KB
                # 网络出流量
                exit_code, stdout, _ = wrapper.execute("cat /proc/net/dev | grep -E 'eth0|ens' | awk '{print $10}'", 10)
                if exit_code == 0 and stdout.strip():
                    sample.network_out = float(stdout.strip()) / 1024  # KB

            # 采集负载
            if 'load' in self.metrics:
                exit_code, stdout, _ = wrapper.execute("cat /proc/loadavg | awk '{print $1}'", 10)
                if exit_code == 0 and stdout.strip():
                    sample.load_avg = stdout.strip()

            # 采集进程数
            if 'process' in self.metrics:
                exit_code, stdout, _ = wrapper.execute("ps aux | wc -l", 10)
                if exit_code == 0 and stdout.strip():
                    sample.process_count = int(stdout.strip())

            wrapper.close()

        except Exception as e:
            self.logger.error(f"节点 {node.name} 指标采集失败: {e}")

        return sample

    def collect(self) -> list[MetricsSample]:
        """执行指标采集"""
        self._running = True
        start_time = time.time()
        iterations = 0
        max_iterations = self.duration // self.interval

        self.logger.info("=" * 60)
        self.logger.info("开始实时指标采集")
        self.logger.info(f"目标节点: {[n.name for n in self.nodes]}")
        self.logger.info(f"采集指标: {self.metrics}")
        self.logger.info(f"采集时长: {self.duration} 秒")
        self.logger.info(f"采集间隔: {self.interval} 秒")
        self.logger.info(f"预计采样: {max_iterations} 次")
        self.logger.info("=" * 60)

        while self._running and iterations < max_iterations:
            iteration_start = time.time()
            self.logger.info(f"\n--- 采集第 {iterations + 1} 次 ---")

            # 并行采集所有节点
            with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
                futures = {executor.submit(self._collect_single_node, node): node for node in self.nodes}
                for future in as_completed(futures):
                    sample = future.result()
                    self.samples.append(sample)
                    self.logger.info(f"  {sample.node_name}: CPU={sample.cpu_usage:.1f}%, "
                                    f"MEM={sample.memory_usage:.1f}%, "
                                    f"DISK={sample.disk_usage:.1f}%")

            iterations += 1

            # 计算下次采集时间
            elapsed = time.time() - iteration_start
            sleep_time = max(0, self.interval - elapsed)
            if sleep_time > 0 and iterations < max_iterations:
                time.sleep(sleep_time)

        self.logger.info("=" * 60)
        self.logger.info(f"指标采集完成: 共采集 {len(self.samples)} 个样本")
        self.logger.info("=" * 60)

        return self.samples

    def stop(self):
        """停止采集"""
        self._running = False

    def get_aggregated_data(self) -> dict[str, list[MetricsSample]]:
        """按节点聚合数据"""
        aggregated = defaultdict(list)
        for sample in self.samples:
            aggregated[sample.node_name].append(sample)
        return dict(aggregated)

    def save_to_json(self, output_path: str):
        """保存采集数据到JSON"""
        data = {
            "metadata": {
                "nodes": [n.name for n in self.nodes],
                "metrics": self.metrics,
                "duration": self.duration,
                "interval": self.interval,
                "samples_count": len(self.samples),
                "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            "samples": [
                {
                    "timestamp": s.timestamp,
                    "node_name": s.node_name,
                    "host": s.host,
                    "cpu_usage": s.cpu_usage,
                    "memory_usage": s.memory_usage,
                    "disk_usage": s.disk_usage,
                    "network_in": s.network_in,
                    "network_out": s.network_out,
                    "load_avg": s.load_avg,
                    "process_count": s.process_count
                }
                for s in self.samples
            ]
        }
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self.logger.info(f"采集数据已保存: {output_path}")


class BaselineComparator:
    """性能基准对比器"""

    def __init__(self, baseline_file: str, current_data: list[MetricsSample],
                 logger: Optional[logging.Logger] = None):
        self.baseline_file = baseline_file
        self.current_data = current_data
        self.logger = logger or logging.getLogger("batch_exec")
        self.baseline_data: dict[str, BaselineData] = {}
        self.comparison_results: list[dict] = []

    def load_baseline(self) -> bool:
        """加载基准数据"""
        if not os.path.exists(self.baseline_file):
            self.logger.error(f"基准文件不存在: {self.baseline_file}")
            return False

        try:
            with open(self.baseline_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # 从保存的聚合数据构建基准
            if 'baseline' in data:
                for item in data['baseline']:
                    baseline = BaselineData(
                        node_name=item['node_name'],
                        host=item['host'],
                        timestamp=item['timestamp'],
                        cpu_avg=item.get('cpu_avg'),
                        cpu_max=item.get('cpu_max'),
                        memory_avg=item.get('memory_avg'),
                        memory_max=item.get('memory_max'),
                        disk_avg=item.get('disk_avg'),
                        disk_max=item.get('disk_max'),
                        samples_count=item.get('samples_count', 0)
                    )
                    self.baseline_data[baseline.node_name] = baseline

            self.logger.info(f"已加载基准数据: {len(self.baseline_data)} 个节点")
            return True

        except Exception as e:
            self.logger.error(f"加载基准数据失败: {e}")
            return False

    def calculate_current_baseline(self) -> dict[str, BaselineData]:
        """从当前数据计算基准值"""
        aggregated = defaultdict(list)
        for sample in self.current_data:
            aggregated[sample.node_name].append(sample)

        current_baseline = {}
        for node_name, samples in aggregated.items():
            cpu_values = [s.cpu_usage for s in samples if s.cpu_usage]
            mem_values = [s.memory_usage for s in samples if s.memory_usage]
            disk_values = [s.disk_usage for s in samples if s.disk_usage]

            baseline = BaselineData(
                node_name=node_name,
                host=samples[0].host if samples else '',
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                cpu_avg=sum(cpu_values) / len(cpu_values) if cpu_values else None,
                cpu_max=max(cpu_values) if cpu_values else None,
                memory_avg=sum(mem_values) / len(mem_values) if mem_values else None,
                memory_max=max(mem_values) if mem_values else None,
                disk_avg=sum(disk_values) / len(disk_values) if disk_values else None,
                disk_max=max(disk_values) if disk_values else None,
                samples_count=len(samples)
            )
            current_baseline[node_name] = baseline

        return current_baseline

    def compare(self) -> list[dict]:
        """对比当前数据与基准"""
        if not self.baseline_data:
            if not self.load_baseline():
                return []

        current_baseline = self.calculate_current_baseline()

        self.logger.info("=" * 60)
        self.logger.info("性能基准对比")
        self.logger.info("=" * 60)

        for node_name, current in current_baseline.items():
            if node_name not in self.baseline_data:
                self.logger.warning(f"节点 {node_name} 无基准数据")
                continue

            baseline = self.baseline_data[node_name]
            result = {
                'node_name': node_name,
                'host': current.host,
                'comparison_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # CPU对比
            if current.cpu_avg and baseline.cpu_avg:
                cpu_diff = current.cpu_avg - baseline.cpu_avg
                cpu_percent = (cpu_diff / baseline.cpu_avg) * 100 if baseline.cpu_avg > 0 else 0
                result['cpu'] = {
                    'current_avg': current.cpu_avg,
                    'baseline_avg': baseline.cpu_avg,
                    'difference': cpu_diff,
                    'change_percent': cpu_percent,
                    'status': 'degraded' if cpu_percent > 20 else 'improved' if cpu_percent < -10 else 'normal'
                }

            # 内存对比
            if current.memory_avg and baseline.memory_avg:
                mem_diff = current.memory_avg - baseline.memory_avg
                mem_percent = (mem_diff / baseline.memory_avg) * 100 if baseline.memory_avg > 0 else 0
                result['memory'] = {
                    'current_avg': current.memory_avg,
                    'baseline_avg': baseline.memory_avg,
                    'difference': mem_diff,
                    'change_percent': mem_percent,
                    'status': 'degraded' if mem_percent > 20 else 'improved' if mem_percent < -10 else 'normal'
                }

            # 磁盘对比
            if current.disk_avg and baseline.disk_avg:
                disk_diff = current.disk_avg - baseline.disk_avg
                disk_percent = (disk_diff / baseline.disk_avg) * 100 if baseline.disk_avg > 0 else 0
                result['disk'] = {
                    'current_avg': current.disk_avg,
                    'baseline_avg': baseline.disk_avg,
                    'difference': disk_diff,
                    'change_percent': disk_percent,
                    'status': 'degraded' if disk_percent > 10 else 'normal'
                }

            self.comparison_results.append(result)

            # 输出对比结果
            self.logger.info(f"节点: {node_name}")
            if 'cpu' in result:
                self.logger.info(f"  CPU: 当前 {result['cpu']['current_avg']:.1f}% vs "
                                f"基准 {result['cpu']['baseline_avg']:.1f}% "
                                f"(变化 {result['cpu']['change_percent']:.1f}%) [{result['cpu']['status']}]")
            if 'memory' in result:
                self.logger.info(f"  内存: 当前 {result['memory']['current_avg']:.1f}% vs "
                                f"基准 {result['memory']['baseline_avg']:.1f}% "
                                f"(变化 {result['memory']['change_percent']:.1f}%) [{result['memory']['status']}]")
            if 'disk' in result:
                self.logger.info(f"  磁盘: 当前 {result['disk']['current_avg']:.1f}% vs "
                                f"基准 {result['disk']['baseline_avg']:.1f}% "
                                f"(变化 {result['disk']['change_percent']:.1f}%) [{result['disk']['status']}]")

        self.logger.info("=" * 60)
        return self.comparison_results

    def save_comparison(self, output_path: str):
        """保存对比结果"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.comparison_results, f, ensure_ascii=False, indent=2)
        self.logger.info(f"对比结果已保存: {output_path}")

    def save_as_baseline(self, output_path: str):
        """将当前数据保存为新基准"""
        current_baseline = self.calculate_current_baseline()
        data = {
            'baseline': [
                {
                    'node_name': b.node_name,
                    'host': b.host,
                    'timestamp': b.timestamp,
                    'cpu_avg': b.cpu_avg,
                    'cpu_max': b.cpu_max,
                    'memory_avg': b.memory_avg,
                    'memory_max': b.memory_max,
                    'disk_avg': b.disk_avg,
                    'disk_max': b.disk_max,
                    'samples_count': b.samples_count
                }
                for b in current_baseline.values()
            ]
        }
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self.logger.info(f"新基准已保存: {output_path}")


class PrometheusExporter:
    """Prometheus格式指标输出器"""

    def __init__(self, samples: list[MetricsSample],
                 logger: Optional[logging.Logger] = None):
        self.samples = samples
        self.logger = logger or logging.getLogger("batch_exec")

    def export(self, output_path: str) -> str:
        """导出Prometheus格式指标"""
        lines = []
        lines.append("# HELP batch_exec_cpu_usage CPU usage percentage")
        lines.append("# TYPE batch_exec_cpu_usage gauge")

        for sample in self.samples:
            if sample.cpu_usage:
                lines.append(f"batch_exec_cpu_usage{{node=\"{sample.node_name}\",host=\"{sample.host}\"}} {sample.cpu_usage:.2f}")

        lines.append("")
        lines.append("# HELP batch_exec_memory_usage Memory usage percentage")
        lines.append("# TYPE batch_exec_memory_usage gauge")

        for sample in self.samples:
            if sample.memory_usage:
                lines.append(f"batch_exec_memory_usage{{node=\"{sample.node_name}\",host=\"{sample.host}\"}} {sample.memory_usage:.2f}")

        lines.append("")
        lines.append("# HELP batch_exec_disk_usage Disk usage percentage")
        lines.append("# TYPE batch_exec_disk_usage gauge")

        for sample in self.samples:
            if sample.disk_usage:
                lines.append(f"batch_exec_disk_usage{{node=\"{sample.node_name}\",host=\"{sample.host}\"}} {sample.disk_usage:.2f}")

        lines.append("")
        lines.append("# HELP batch_exec_network_in Network incoming traffic (KB)")
        lines.append("# TYPE batch_exec_network_in gauge")

        for sample in self.samples:
            if sample.network_in:
                lines.append(f"batch_exec_network_in{{node=\"{sample.node_name}\",host=\"{sample.host}\"}} {sample.network_in:.2f}")

        lines.append("")
        lines.append("# HELP batch_exec_network_out Network outgoing traffic (KB)")
        lines.append("# TYPE batch_exec_network_out gauge")

        for sample in self.samples:
            if sample.network_out:
                lines.append(f"batch_exec_network_out{{node=\"{sample.node_name}\",host=\"{sample.host}\"}} {sample.network_out:.2f}")

        lines.append("")
        lines.append(f"# Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        output = "\n".join(lines)

        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(output)
            self.logger.info(f"Prometheus指标已导出: {output_path}")

        return output


class AnomalyDetector:
    """自动异常检测器"""

    def __init__(self, baseline_file: str, threshold: float = 20.0,
                 samples: list[MetricsSample] = None,
                 logger: Optional[logging.Logger] = None):
        self.baseline_file = baseline_file
        self.threshold = threshold
        self.samples = samples or []
        self.logger = logger or logging.getLogger("batch_exec")
        self.baseline_data: dict[str, BaselineData] = {}
        self.anomalies: list[AnomalyReport] = []

    def load_baseline(self) -> bool:
        """加载基准数据"""
        if not os.path.exists(self.baseline_file):
            self.logger.warning(f"基准文件不存在: {self.baseline_file}")
            return False

        try:
            with open(self.baseline_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if 'baseline' in data:
                for item in data['baseline']:
                    baseline = BaselineData(
                        node_name=item['node_name'],
                        host=item['host'],
                        timestamp=item['timestamp'],
                        cpu_avg=item.get('cpu_avg'),
                        cpu_max=item.get('cpu_max'),
                        memory_avg=item.get('memory_avg'),
                        memory_max=item.get('memory_max'),
                        disk_avg=item.get('disk_avg'),
                        disk_max=item.get('disk_max'),
                        samples_count=item.get('samples_count', 0)
                    )
                    self.baseline_data[baseline.node_name] = baseline

            self.logger.info(f"已加载基准数据: {len(self.baseline_data)} 个节点")
            return True

        except Exception as e:
            self.logger.error(f"加载基准数据失败: {e}")
            return False

    def detect(self) -> list[AnomalyReport]:
        """检测异常"""
        if not self.baseline_data:
            if not self.load_baseline():
                # 如果没有基准，使用首次采集数据作为基准
                self.logger.info("无基准数据，将首次采集数据作为基准")

        self.logger.info("=" * 60)
        self.logger.info(f"自动异常检测 (阈值: {self.threshold}%)")
        self.logger.info("=" * 60)

        # 按节点聚合当前数据
        aggregated = defaultdict(list)
        for sample in self.samples:
            aggregated[sample.node_name].append(sample)

        # 如果没有基准数据，创建基准
        if not self.baseline_data:
            for node_name, samples in aggregated.items():
                cpu_values = [s.cpu_usage for s in samples if s.cpu_usage]
                mem_values = [s.memory_usage for s in samples if s.memory_usage]
                disk_values = [s.disk_usage for s in samples if s.disk_usage]

                baseline = BaselineData(
                    node_name=node_name,
                    host=samples[0].host if samples else '',
                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    cpu_avg=sum(cpu_values) / len(cpu_values) if cpu_values else None,
                    memory_avg=sum(mem_values) / len(mem_values) if mem_values else None,
                    disk_avg=sum(disk_values) / len(disk_values) if disk_values else None,
                    samples_count=len(samples)
                )
                self.baseline_data[node_name] = baseline

        anomaly_count = 0

        for node_name, samples in aggregated.items():
            baseline = self.baseline_data.get(node_name)

            # 计算当前平均值
            cpu_values = [s.cpu_usage for s in samples if s.cpu_usage]
            mem_values = [s.memory_usage for s in samples if s.memory_usage]
            disk_values = [s.disk_usage for s in samples if s.disk_usage]

            current_cpu_avg = sum(cpu_values) / len(cpu_values) if cpu_values else None
            current_mem_avg = sum(mem_values) / len(mem_values) if mem_values else None
            current_disk_avg = sum(disk_values) / len(disk_values) if disk_values else None

            # 检测CPU异常
            if current_cpu_avg and baseline and baseline.cpu_avg:
                deviation = abs(current_cpu_avg - baseline.cpu_avg)
                deviation_percent = (deviation / baseline.cpu_avg) * 100 if baseline.cpu_avg > 0 else 0

                if deviation_percent > self.threshold:
                    severity = 'high' if deviation_percent > 50 else 'medium' if deviation_percent > 30 else 'low'
                    anomaly = AnomalyReport(
                        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        node_name=node_name,
                        host=samples[0].host,
                        metric_name='cpu_usage',
                        current_value=current_cpu_avg,
                        baseline_value=baseline.cpu_avg,
                        deviation_percent=deviation_percent,
                        threshold=self.threshold,
                        is_anomaly=True,
                        severity=severity
                    )
                    self.anomalies.append(anomaly)
                    anomaly_count += 1
                    self.logger.warning(f"[异常] {node_name} CPU: 当前 {current_cpu_avg:.1f}% "
                                       f"基准 {baseline.cpu_avg:.1f}% 偏离 {deviation_percent:.1f}% "
                                       f"严重度: {severity}")

            # 检测内存异常
            if current_mem_avg and baseline and baseline.memory_avg:
                deviation = abs(current_mem_avg - baseline.memory_avg)
                deviation_percent = (deviation / baseline.memory_avg) * 100 if baseline.memory_avg > 0 else 0

                if deviation_percent > self.threshold:
                    severity = 'high' if deviation_percent > 50 else 'medium' if deviation_percent > 30 else 'low'
                    anomaly = AnomalyReport(
                        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        node_name=node_name,
                        host=samples[0].host,
                        metric_name='memory_usage',
                        current_value=current_mem_avg,
                        baseline_value=baseline.memory_avg,
                        deviation_percent=deviation_percent,
                        threshold=self.threshold,
                        is_anomaly=True,
                        severity=severity
                    )
                    self.anomalies.append(anomaly)
                    anomaly_count += 1
                    self.logger.warning(f"[异常] {node_name} 内存: 当前 {current_mem_avg:.1f}% "
                                       f"基准 {baseline.memory_avg:.1f}% 偏离 {deviation_percent:.1f}% "
                                       f"严重度: {severity}")

            # 检测磁盘异常
            if current_disk_avg and baseline and baseline.disk_avg:
                deviation = abs(current_disk_avg - baseline.disk_avg)
                deviation_percent = (deviation / baseline.disk_avg) * 100 if baseline.disk_avg > 0 else 0

                if deviation_percent > self.threshold / 2:  # 磁盘阈值减半
                    severity = 'high' if deviation_percent > 25 else 'medium' if deviation_percent > 15 else 'low'
                    anomaly = AnomalyReport(
                        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        node_name=node_name,
                        host=samples[0].host,
                        metric_name='disk_usage',
                        current_value=current_disk_avg,
                        baseline_value=baseline.disk_avg,
                        deviation_percent=deviation_percent,
                        threshold=self.threshold / 2,
                        is_anomaly=True,
                        severity=severity
                    )
                    self.anomalies.append(anomaly)
                    anomaly_count += 1
                    self.logger.warning(f"[异常] {node_name} 磁盘: 当前 {current_disk_avg:.1f}% "
                                       f"基准 {baseline.disk_avg:.1f}% 偏离 {deviation_percent:.1f}% "
                                       f"严重度: {severity}")

        self.logger.info("=" * 60)
        self.logger.info(f"异常检测完成: 发现 {anomaly_count} 个异常")
        self.logger.info("=" * 60)

        return self.anomalies

    def save_report(self, output_path: str):
        """保存异常报告"""
        data = {
            "detection_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "threshold": self.threshold,
            "anomaly_count": len(self.anomalies),
            "anomalies": [
                {
                    "timestamp": a.timestamp,
                    "node_name": a.node_name,
                    "host": a.host,
                    "metric_name": a.metric_name,
                    "current_value": a.current_value,
                    "baseline_value": a.baseline_value,
                    "deviation_percent": a.deviation_percent,
                    "threshold": a.threshold,
                    "is_anomaly": a.is_anomaly,
                    "severity": a.severity
                }
                for a in self.anomalies
            ]
        }
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self.logger.info(f"异常报告已保存: {output_path}")

    def has_critical_anomaly(self) -> bool:
        """是否存在严重异常"""
        return any(a.severity == 'high' for a in self.anomalies)


# ============== 数据分析与报表类 ==============
@dataclass
class LogEntry:
    """日志条目"""
    timestamp: str
    level: str
    message: str
    node_name: Optional[str] = None
    host: Optional[str] = None
    command: Optional[str] = None
    duration: Optional[float] = None
    success: Optional[bool] = None
    error: Optional[str] = None


@dataclass
class StatisticsResult:
    """统计分析结果"""
    period: str
    total_executions: int
    success_count: int
    fail_count: int
    success_rate: float
    avg_duration: float
    max_duration: float
    min_duration: float
    nodes_count: int
    top_commands: list[tuple[str, int]]
    top_failed_nodes: list[tuple[str, int]]


@dataclass
class PerformanceTrend:
    """性能趋势数据"""
    date: str
    avg_cpu: Optional[float] = None
    avg_memory: Optional[float] = None
    avg_disk: Optional[float] = None
    avg_duration: Optional[float] = None
    execution_count: int = 0
    success_rate: float = 0.0


@dataclass
class PredictionResult:
    """预测结果"""
    timestamp: str
    node_name: str
    metric_name: str
    predicted_value: float
    confidence: float
    risk_level: str  # low, medium, high
    predicted_anomaly: bool
    recommendation: Optional[str] = None


class LogAnalyzer:
    """执行日志解析器"""

    def __init__(self, log_path: str, pattern: Optional[str] = None,
                 logger: Optional[logging.Logger] = None):
        self.log_path = log_path
        self.pattern = pattern or r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - (\w+) - (.+)'
        self.logger = logger or logging.getLogger("batch_exec")
        self.entries: list[LogEntry] = []
        self.analysis_results: dict = {}

    def parse(self) -> list[LogEntry]:
        """解析日志文件"""
        self.logger.info("=" * 60)
        self.logger.info(f"解析日志文件: {self.log_path}")
        self.logger.info("=" * 60)

        if not os.path.exists(self.log_path):
            self.logger.error(f"日志文件不存在: {self.log_path}")
            return []

        try:
            with open(self.log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                try:
                    # 尝试匹配标准格式
                    match = re.match(self.pattern, line)
                    if match:
                        entry = LogEntry(
                            timestamp=match.group(1),
                            level=match.group(2),
                            message=match.group(3)
                        )

                        # 解析额外信息
                        if '节点' in line or 'node' in line.lower():
                            node_match = re.search(r'节点[:\s]+(\S+)', line)
                            if node_match:
                                entry.node_name = node_match.group(1)

                        if '成功' in line or '✓' in line:
                            entry.success = True
                        elif '失败' in line or '✗' in line:
                            entry.success = False

                        if '耗时' in line or 'duration' in line.lower():
                            duration_match = re.search(r'(\d+\.?\d*)s', line)
                            if duration_match:
                                entry.duration = float(duration_match.group(1))

                        self.entries.append(entry)

                except Exception as e:
                    self.logger.debug(f"解析行失败: {line[:50]}, 错误: {e}")

            self.logger.info(f"已解析 {len(self.entries)} 条日志")
            return self.entries

        except Exception as e:
            self.logger.error(f"读取日志失败: {e}")
            return []

    def analyze(self) -> dict:
        """分析日志数据"""
        if not self.entries:
            self.logger.warning("无日志数据可分析")
            return {}

        self.analysis_results = {
            'total_entries': len(self.entries),
            'levels': defaultdict(int),
            'success_count': 0,
            'fail_count': 0,
            'nodes': defaultdict(int),
            'errors': [],
            'warnings': [],
            'avg_duration': 0.0,
            'durations': []
        }

        for entry in self.entries:
            self.analysis_results['levels'][entry.level] += 1

            if entry.success:
                self.analysis_results['success_count'] += 1
            elif entry.success is False:
                self.analysis_results['fail_count'] += 1

            if entry.node_name:
                self.analysis_results['nodes'][entry.node_name] += 1

            if entry.level == 'ERROR':
                self.analysis_results['errors'].append(entry.message)
            elif entry.level == 'WARNING':
                self.analysis_results['warnings'].append(entry.message)

            if entry.duration:
                self.analysis_results['durations'].append(entry.duration)

        if self.analysis_results['durations']:
            self.analysis_results['avg_duration'] = sum(self.analysis_results['durations']) / len(self.analysis_results['durations'])

        # 输出分析结果
        self.logger.info("\n日志分析结果:")
        self.logger.info(f"  总条目: {self.analysis_results['total_entries']}")
        self.logger.info(f"  日志级别分布: {dict(self.analysis_results['levels'])}")
        self.logger.info(f"  成功/失败: {self.analysis_results['success_count']}/{self.analysis_results['fail_count']}")
        self.logger.info(f"  平均耗时: {self.analysis_results['avg_duration']:.2f}s")
        self.logger.info(f"  节点分布: {dict(self.analysis_results['nodes'])}")
        self.logger.info(f"  错误数: {len(self.analysis_results['errors'])}")
        self.logger.info(f"  警告数: {len(self.analysis_results['warnings'])}")

        return self.analysis_results

    def save_report(self, output_path: str):
        """保存分析报告"""
        report = {
            'log_path': self.log_path,
            'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'summary': {
                'total_entries': self.analysis_results.get('total_entries', 0),
                'levels': dict(self.analysis_results.get('levels', {})),
                'success_count': self.analysis_results.get('success_count', 0),
                'fail_count': self.analysis_results.get('fail_count', 0),
                'avg_duration': self.analysis_results.get('avg_duration', 0),
                'nodes': dict(self.analysis_results.get('nodes', {})),
                'error_count': len(self.analysis_results.get('errors', [])),
                'warning_count': len(self.analysis_results.get('warnings', []))
            },
            'top_errors': self.analysis_results.get('errors', [])[:20],
            'top_warnings': self.analysis_results.get('warnings', [])[:20]
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        self.logger.info(f"日志分析报告已保存: {output_path}")


class StatisticsAnalyzer:
    """统计分析器"""

    def __init__(self, history_files: list[str], logger: Optional[logging.Logger] = None):
        self.history_files = history_files
        self.logger = logger or logging.getLogger("batch_exec")
        self.all_data: list[dict] = []

    def load_data(self) -> bool:
        """加载历史数据"""
        self.logger.info("=" * 60)
        self.logger.info("加载统计数据")
        self.logger.info("=" * 60)

        for file_path in self.history_files:
            if not os.path.exists(file_path):
                self.logger.warning(f"文件不存在: {file_path}")
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.all_data.extend(data)
                    elif isinstance(data, dict):
                        self.all_data.append(data)
                self.logger.info(f"已加载: {file_path}")
            except Exception as e:
                self.logger.error(f"加载失败 {file_path}: {e}")

        self.logger.info(f"总数据条数: {len(self.all_data)}")
        return len(self.all_data) > 0

    def analyze_by_day(self, days: int = 7) -> list[StatisticsResult]:
        """按日期统计分析"""
        results = []

        # 按日期分组
        by_date = defaultdict(list)
        for item in self.all_data:
            timestamp = item.get('timestamp', '')
            if timestamp:
                date = timestamp.split()[0] if ' ' in timestamp else timestamp[:10]
                by_date[date].append(item)

        # 计算每天的统计
        sorted_dates = sorted(by_date.keys(), reverse=True)[:days]

        for date in sorted_dates:
            items = by_date[date]
            success_count = sum(1 for i in items if i.get('success_count', 0) >= 0)
            fail_count = sum(1 for i in items if i.get('fail_count', 0) > 0)
            durations = [i.get('duration', 0) for i in items if i.get('duration')]

            # 统计命令频率
            commands = defaultdict(int)
            for i in items:
                cmd = i.get('command', 'N/A')
                if cmd:
                    commands[cmd[:50]] += 1

            # 统计失败节点
            failed_nodes = defaultdict(int)
            for i in items:
                for node in i.get('nodes', []):
                    failed_nodes[node] += 1

            result = StatisticsResult(
                period=date,
                total_executions=len(items),
                success_count=sum(i.get('success_count', 0) for i in items),
                fail_count=sum(i.get('fail_count', 0) for i in items),
                success_rate=sum(i.get('success_count', 0) for i in items) / max(1, sum(i.get('success_count', 0) + i.get('fail_count', 0) for i in items)) * 100,
                avg_duration=sum(durations) / len(durations) if durations else 0,
                max_duration=max(durations) if durations else 0,
                min_duration=min(durations) if durations else 0,
                nodes_count=sum(len(i.get('nodes', [])) for i in items),
                top_commands=sorted(commands.items(), key=lambda x: x[1], reverse=True)[:5],
                top_failed_nodes=sorted(failed_nodes.items(), key=lambda x: x[1], reverse=True)[:5]
            )
            results.append(result)

        return results

    def analyze_by_node(self) -> list[StatisticsResult]:
        """按节点统计分析"""
        results = []

        # 按节点分组
        by_node = defaultdict(list)
        for item in self.all_data:
            for node in item.get('nodes', []):
                by_node[node].append(item)

        for node_name, items in by_node.items():
            success_count = sum(1 for i in items if i.get('success_count', 0) > 0)
            fail_count = sum(1 for i in items if i.get('fail_count', 0) > 0)
            durations = [i.get('duration', 0) for i in items if i.get('duration')]

            result = StatisticsResult(
                period=node_name,
                total_executions=len(items),
                success_count=success_count,
                fail_count=fail_count,
                success_rate=success_count / max(1, success_count + fail_count) * 100,
                avg_duration=sum(durations) / len(durations) if durations else 0,
                max_duration=max(durations) if durations else 0,
                min_duration=min(durations) if durations else 0,
                nodes_count=1,
                top_commands=[],
                top_failed_nodes=[]
            )
            results.append(result)

        return results

    def generate_report(self, by_day: bool = True, by_node: bool = True,
                        days: int = 7) -> dict:
        """生成综合统计报告"""
        report = {
            'report_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_sources': self.history_files,
            'total_records': len(self.all_data)
        }

        if by_day:
            report['by_day'] = []
            for result in self.analyze_by_day(days):
                report['by_day'].append({
                    'date': result.period,
                    'total_executions': result.total_executions,
                    'success_count': result.success_count,
                    'fail_count': result.fail_count,
                    'success_rate': result.success_rate,
                    'avg_duration': result.avg_duration,
                    'max_duration': result.max_duration,
                    'min_duration': result.min_duration
                })

        if by_node:
            report['by_node'] = []
            for result in self.analyze_by_node():
                report['by_node'].append({
                    'node': result.period,
                    'total_executions': result.total_executions,
                    'success_count': result.success_count,
                    'fail_count': result.fail_count,
                    'success_rate': result.success_rate,
                    'avg_duration': result.avg_duration
                })

        return report

    def save_report(self, output_path: str, by_day: bool = True, by_node: bool = True):
        """保存统计报告"""
        report = self.generate_report(by_day, by_node)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        self.logger.info(f"统计报告已保存: {output_path}")

    def print_summary(self, by_day: bool = True, by_node: bool = True):
        """打印统计摘要"""
        self.logger.info("=" * 60)
        self.logger.info("统计分析摘要")
        self.logger.info("=" * 60)

        if by_day:
            self.logger.info("\n按日期统计:")
            for result in self.analyze_by_day(7):
                self.logger.info(f"  {result.period}: 执行 {result.total_executions} 次, "
                                f"成功率 {result.success_rate:.1f}%, "
                                f"平均耗时 {result.avg_duration:.2f}s")

        if by_node:
            self.logger.info("\n按节点统计:")
            for result in self.analyze_by_node()[:10]:
                self.logger.info(f"  {result.period}: 执行 {result.total_executions} 次, "
                                f"成功率 {result.success_rate:.1f}%")

        self.logger.info("=" * 60)


class PerformanceReporter:
    """性能报表生成器"""

    def __init__(self, metrics_files: list[str], logger: Optional[logging.Logger] = None):
        self.metrics_files = metrics_files
        self.logger = logger or logging.getLogger("batch_exec")
        self.trends: list[PerformanceTrend] = []
        self.metrics_data: list[dict] = []

    def load_metrics(self) -> bool:
        """加载指标数据"""
        for file_path in self.metrics_files:
            if not os.path.exists(file_path):
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'samples' in data:
                        self.metrics_data.extend(data['samples'])
                    elif isinstance(data, list):
                        self.metrics_data.extend(data)
            except Exception as e:
                self.logger.error(f"加载指标失败 {file_path}: {e}")

        self.logger.info(f"已加载 {len(self.metrics_data)} 条指标数据")
        return len(self.metrics_data) > 0

    def calculate_trends(self, range_days: int = 7) -> list[PerformanceTrend]:
        """计算性能趋势"""
        # 按日期分组
        by_date = defaultdict(list)
        for sample in self.metrics_data:
            timestamp = sample.get('timestamp', '')
            if timestamp:
                date = timestamp.split()[0] if ' ' in timestamp else timestamp[:10]
                by_date[date].append(sample)

        # 计算趋势
        sorted_dates = sorted(by_date.keys(), reverse=True)[:range_days]

        for date in sorted_dates:
            samples = by_date[date]
            cpu_values = [s.get('cpu_usage') for s in samples if s.get('cpu_usage')]
            mem_values = [s.get('memory_usage') for s in samples if s.get('memory_usage')]
            disk_values = [s.get('disk_usage') for s in samples if s.get('disk_usage')]

            trend = PerformanceTrend(
                date=date,
                avg_cpu=sum(cpu_values) / len(cpu_values) if cpu_values else None,
                avg_memory=sum(mem_values) / len(mem_values) if mem_values else None,
                avg_disk=sum(disk_values) / len(disk_values) if disk_values else None,
                execution_count=len(samples),
                success_rate=100.0  # 默认值
            )
            self.trends.append(trend)

        return self.trends

    def generate_html_report(self, output_path: str, title: str = "性能报表") -> str:
        """生成HTML性能报表"""
        if not self.trends:
            self.calculate_trends()

        # 构建图表数据
        dates = [t.date for t in self.trends]
        cpu_data = [t.avg_cpu or 0 for t in self.trends]
        mem_data = [t.avg_memory or 0 for t in self.trends]
        disk_data = [t.avg_disk or 0 for t in self.trends]

        html_content = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        h1 {{ color: #333; }}
        .container {{ background: white; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .chart-container {{ height: 400px; margin: 20px 0; }}
        .summary {{ display: flex; gap: 20px; }}
        .stat-card {{ background: #e3f2fd; padding: 15px; border-radius: 8px; flex: 1; }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: #1976d2; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: center; }}
        th {{ background: #1976d2; color: white; }}
        .warning {{ background: #fff3e0; }}
        .danger {{ background: #ffebee; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

    <div class="container summary">
        <div class="stat-card">
            <div>CPU平均值</div>
            <div class="stat-value">{sum(cpu_data)/len(cpu_data):.1f}%</div>
        </div>
        <div class="stat-card">
            <div>内存平均值</div>
            <div class="stat-value">{sum(mem_data)/len(mem_data):.1f}%</div>
        </div>
        <div class="stat-card">
            <div>磁盘平均值</div>
            <div class="stat-value">{sum(disk_data)/len(disk_data):.1f}%</div>
        </div>
        <div class="stat-card">
            <div>采集样本数</div>
            <div class="stat-value">{len(self.metrics_data)}</div>
        </div>
    </div>

    <div class="container">
        <h2>性能趋势图</h2>
        <div class="chart-container">
            <canvas id="trendChart"></canvas>
        </div>
    </div>

    <div class="container">
        <h2>详细数据表</h2>
        <table>
            <tr><th>日期</th><th>CPU%</th><th>内存%</th><th>磁盘%</th><th>样本数</th><th>状态</th></tr>
'''

        for t in self.trends:
            status = "正常"
            row_class = ""
            if t.avg_cpu and t.avg_cpu > 80:
                status = "警告"
                row_class = "warning"
            if t.avg_memory and t.avg_memory > 85:
                status = "危险"
                row_class = "danger"

            cpu_display = f"{t.avg_cpu:.1f}" if t.avg_cpu else "N/A"
            mem_display = f"{t.avg_memory:.1f}" if t.avg_memory else "N/A"
            disk_display = f"{t.avg_disk:.1f}" if t.avg_disk else "N/A"

            html_content += f'''<tr class="{row_class}">
                <td>{t.date}</td>
                <td>{cpu_display}</td>
                <td>{mem_display}</td>
                <td>{disk_display}</td>
                <td>{t.execution_count}</td>
                <td>{status}</td>
            </tr>'''

        html_content += f'''        </table>
    </div>

    <script>
        const ctx = document.getElementById('trendChart').getContext('2d');
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [
                    {{
                        label: 'CPU使用率',
                        data: {json.dumps(cpu_data)},
                        borderColor: '#e53935',
                        backgroundColor: 'rgba(229, 57, 53, 0.1)',
                        tension: 0.3
                    }},
                    {{
                        label: '内存使用率',
                        data: {json.dumps(mem_data)},
                        borderColor: '#1976d2',
                        backgroundColor: 'rgba(25, 118, 210, 0.1)',
                        tension: 0.3
                    }},
                    {{
                        label: '磁盘使用率',
                        data: {json.dumps(disk_data)},
                        borderColor: '#388e3c',
                        backgroundColor: 'rgba(56, 142, 60, 0.1)',
                        tension: 0.3
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{ beginAtZero: true, max: 100 }}
                }}
            }}
        }});
    </script>
</body>
</html>'''

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        self.logger.info(f"性能报表已生成: {output_path}")
        return output_path

    def generate_json_report(self, output_path: str) -> dict:
        """生成JSON性能报表"""
        if not self.trends:
            self.calculate_trends()

        report = {
            'report_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_sources': self.metrics_files,
            'total_samples': len(self.metrics_data),
            'summary': {
                'avg_cpu': sum(t.avg_cpu or 0 for t in self.trends) / len(self.trends),
                'avg_memory': sum(t.avg_memory or 0 for t in self.trends) / len(self.trends),
                'avg_disk': sum(t.avg_disk or 0 for t in self.trends) / len(self.trends),
                'max_cpu': max(t.avg_cpu or 0 for t in self.trends),
                'max_memory': max(t.avg_memory or 0 for t in self.trends),
                'max_disk': max(t.avg_disk or 0 for t in self.trends)
            },
            'trends': [
                {
                    'date': t.date,
                    'avg_cpu': t.avg_cpu,
                    'avg_memory': t.avg_memory,
                    'avg_disk': t.avg_disk,
                    'execution_count': t.execution_count
                }
                for t in self.trends
            ]
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        self.logger.info(f"JSON报表已保存: {output_path}")
        return report


class AnomalyPredictor:
    """AI异常预测器"""

    def __init__(self, history_data: list[dict], model_path: Optional[str] = None,
                 logger: Optional[logging.Logger] = None):
        self.history_data = history_data
        self.model_path = model_path
        self.logger = logger or logging.getLogger("batch_exec")
        self.predictions: list[PredictionResult] = []
        self.baseline_stats: dict = {}

    def calculate_baseline_stats(self):
        """计算基准统计"""
        if not self.history_data:
            return

        # 计算CPU统计
        cpu_values = [d.get('cpu_usage') or d.get('cpu_avg') for d in self.history_data
                     if d.get('cpu_usage') or d.get('cpu_avg')]
        if cpu_values:
            self.baseline_stats['cpu'] = {
                'mean': sum(cpu_values) / len(cpu_values),
                'std': self._std(cpu_values),
                'max': max(cpu_values),
                'min': min(cpu_values)
            }

        # 计算内存统计
        mem_values = [d.get('memory_usage') or d.get('memory_avg') for d in self.history_data
                     if d.get('memory_usage') or d.get('memory_avg')]
        if mem_values:
            self.baseline_stats['memory'] = {
                'mean': sum(mem_values) / len(mem_values),
                'std': self._std(mem_values),
                'max': max(mem_values),
                'min': min(mem_values)
            }

        # 计算磁盘统计
        disk_values = [d.get('disk_usage') or d.get('disk_avg') for d in self.history_data
                      if d.get('disk_usage') or d.get('disk_avg')]
        if disk_values:
            self.baseline_stats['disk'] = {
                'mean': sum(disk_values) / len(disk_values),
                'std': self._std(disk_values),
                'max': max(disk_values),
                'min': min(disk_values)
            }

        self.logger.info(f"基准统计: {self.baseline_stats}")

    def _std(self, values: list[float]) -> float:
        """计算标准差"""
        if len(values) < 2:
            return 0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5

    def predict(self, nodes: list[str] = None) -> list[PredictionResult]:
        """预测异常"""
        self.calculate_baseline_stats()

        self.logger.info("=" * 60)
        self.logger.info("AI异常预测")
        self.logger.info("=" * 60)

        if not self.baseline_stats:
            self.logger.warning("无足够数据进行预测")
            return []

        nodes = nodes or ['default']
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for metric_name, stats in self.baseline_stats.items():
            mean = stats['mean']
            std = stats['std']

            # 简单预测模型：基于历史趋势
            # 预测下一个值可能超出均值+2倍标准差
            predicted_value = mean + std  # 预测值略高于均值
            threshold_high = mean + 2 * std

            confidence = 1 - (std / mean) if mean > 0 else 0.5

            # 判断风险级别
            if predicted_value > threshold_high:
                risk_level = 'high'
                predicted_anomaly = True
                recommendation = f"{metric_name}可能超出正常范围，建议提前干预"
            elif predicted_value > mean + std:
                risk_level = 'medium'
                predicted_anomaly = True
                recommendation = f"{metric_name}有上升趋势，建议监控"
            else:
                risk_level = 'low'
                predicted_anomaly = False
                recommendation = None

            for node in nodes:
                prediction = PredictionResult(
                    timestamp=timestamp,
                    node_name=node,
                    metric_name=metric_name,
                    predicted_value=predicted_value,
                    confidence=confidence,
                    risk_level=risk_level,
                    predicted_anomaly=predicted_anomaly,
                    recommendation=recommendation
                )
                self.predictions.append(prediction)

                if predicted_anomaly:
                    self.logger.warning(f"[预测异常] {node} {metric_name}: "
                                       f"预测值 {predicted_value:.1f}, "
                                       f"基准均值 {mean:.1f}, "
                                       f"风险级别 {risk_level}, "
                                       f"置信度 {confidence:.1%}")
                    if recommendation:
                        self.logger.info(f"  建议: {recommendation}")

        self.logger.info("=" * 60)
        self.logger.info(f"预测完成: 发现 {sum(1 for p in self.predictions if p.predicted_anomaly)} 个潜在异常")
        self.logger.info("=" * 60)

        return self.predictions

    def save_predictions(self, output_path: str):
        """保存预测结果"""
        data = {
            'prediction_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'baseline_stats': self.baseline_stats,
            'predictions': [
                {
                    'timestamp': p.timestamp,
                    'node_name': p.node_name,
                    'metric_name': p.metric_name,
                    'predicted_value': p.predicted_value,
                    'confidence': p.confidence,
                    'risk_level': p.risk_level,
                    'predicted_anomaly': p.predicted_anomaly,
                    'recommendation': p.recommendation
                }
                for p in self.predictions
            ]
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self.logger.info(f"预测结果已保存: {output_path}")

    def has_high_risk_prediction(self) -> bool:
        """是否有高风险预测"""
        return any(p.risk_level == 'high' for p in self.predictions)


# ============== 用户体验增强 ==============
class TUIInterface:
    """终端图形界面 (TUI)"""

    def __init__(self, nodes: list[NodeConfig], settings: Settings,
                 logger: logging.Logger):
        if not HAS_RICH:
            raise ImportError("需要安装 rich 库: pip install rich")
        self.nodes = nodes
        self.settings = settings
        self.logger = logger
        self.console = Console()
        self.results: list[ExecutionResult] = []
        self.current_command: str = ""
        self.running = False
        self.layout = Layout()

    def setup_layout(self):
        """设置布局"""
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3)
        )
        self.layout["body"].split_row(
            Layout(name="nodes"),
            Layout(name="output")
        )

    def render_header(self) -> Panel:
        """渲染头部"""
        return Panel(
            Text(f"批量远程执行工具 - {self.current_command}", style="bold cyan"),
            style="bold blue"
        )

    def render_nodes_panel(self) -> Panel:
        """渲染节点面板"""
        table = Table(title="节点列表", show_header=True, header_style="bold")
        table.add_column("节点", style="cyan")
        table.add_column("主机", style="green")
        table.add_column("状态", style="yellow")
        table.add_column("耗时", style="magenta")

        for result in self.results:
            status = "[green]✓[/green]" if result.success else "[red]✗[/red]"
            duration = f"{result.duration:.2f}s" if result.duration else "N/A"
            table.add_row(
                result.node_name,
                result.host,
                status,
                duration
            )

        # 添加未执行的节点
        executed_names = {r.node_name for r in self.results}
        for node in self.nodes:
            if node.name not in executed_names:
                table.add_row(node.name, node.host, "[yellow]等待[/yellow]", "-")

        return Panel(table, title="节点状态")

    def render_output_panel(self) -> Panel:
        """渲染输出面板"""
        if not self.results:
            return Panel("等待执行...", title="输出")

        # 显示最后一个结果
        last_result = self.results[-1]
        content = ""
        if last_result.success:
            content = f"[green]成功[/green]\n{last_result.stdout[:500]}"
        else:
            content = f"[red]失败[/red]\n{last_result.stderr[:500]}"

        return Panel(content, title=f"输出 - {last_result.node_name}")

    def render_footer(self) -> Panel:
        """渲染底部"""
        success = sum(1 for r in self.results if r.success)
        fail = len(self.results) - success
        progress = len(self.results) / len(self.nodes) * 100 if self.nodes else 0

        return Panel(
            f"进度: {progress:.1f}% | 成功: {success} | 失败: {fail} | 总计: {len(self.nodes)}",
            style="bold"
        )

    def update_display(self):
        """更新显示"""
        self.layout["header"].update(self.render_header())
        self.layout["nodes"].update(self.render_nodes_panel())
        self.layout["output"].update(self.render_output_panel())
        self.layout["footer"].update(self.render_footer())

    def run_with_tui(self, command: str = None, script: str = None) -> list[ExecutionResult]:
        """带TUI界面的执行"""
        self.setup_layout()
        self.current_command = command or script or "unknown"
        self.results = []

        timeout = self.settings.timeout
        max_workers = self.settings.max_workers

        with Live(self.layout, console=self.console, refresh_per_second=4) as live:
            self.update_display()

            if self.settings.parallel and len(self.nodes) > 1:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            execute_on_node, node, command, script,
                            timeout, False, 3, 1.0, self.logger, None
                        ): node for node in self.nodes
                    }

                    for future in as_completed(futures):
                        result = future.result()
                        self.results.append(result)
                        self.update_display()
                        live.refresh()
            else:
                for node in self.nodes:
                    result = execute_on_node(
                        node, command, script,
                        timeout, False, 3, 1.0, self.logger, None
                    )
                    self.results.append(result)
                    self.update_display()
                    live.refresh()

        # 最终统计
        self.console.print()
        success_count = sum(1 for r in self.results if r.success)
        fail_count = len(self.results) - success_count

        summary_table = Table(title="执行结果汇总", show_header=True)
        summary_table.add_column("统计", style="cyan")
        summary_table.add_column("数量", style="green")
        summary_table.add_row("成功", str(success_count))
        summary_table.add_row("失败", str(fail_count))
        summary_table.add_row("总计", str(len(self.results)))

        self.console.print(summary_table)

        return self.results

    def interactive_menu(self):
        """交互式菜单"""
        self.console.print(Panel("批量远程执行工具 - TUI模式", style="bold cyan"))

        while True:
            self.console.print("\n[bold]可用选项:[/bold]")
            options = [
                "1. 执行命令",
                "2. 执行脚本",
                "3. 健康检查",
                "4. 查看节点列表",
                "5. 查看历史",
                "q. 退出"
            ]
            for opt in options:
                self.console.print(f"  {opt}")

            choice = self.console.input("[bold cyan]请选择操作: [/bold cyan]")

            if choice == 'q':
                self.console.print("[yellow]退出TUI[/yellow]")
                break
            elif choice == '1':
                cmd = self.console.input("[bold]输入命令: [/bold]")
                self.run_with_tui(command=cmd)
            elif choice == '2':
                script = self.console.input("[bold]输入脚本路径: [/bold]")
                if os.path.exists(script):
                    self.run_with_tui(script=script)
                else:
                    self.console.print("[red]脚本文件不存在[/red]")
            elif choice == '3':
                self.console.print("[bold]执行健康检查...[/bold]")
                # 简化的健康检查
                self.current_command = "健康检查"
                self.run_with_tui(command="uptime")
            elif choice == '4':
                table = Table(title="节点列表")
                table.add_column("名称", style="cyan")
                table.add_column("主机", style="green")
                table.add_column("端口", style="yellow")
                table.add_column("标签", style="magenta")
                for node in self.nodes:
                    tags = ", ".join(node.tags) if node.tags else "-"
                    table.add_row(node.name, node.host, str(node.port), tags)
                self.console.print(table)
            elif choice == '5':
                # 显示最近历史
                history_file = "/tmp/exec_history.json"
                if os.path.exists(history_file):
                    with open(history_file, 'r') as f:
                        history = json.load(f)
                    table = Table(title="执行历史")
                    table.add_column("时间", style="cyan")
                    table.add_column("命令", style="green")
                    table.add_column("结果", style="yellow")
                    for h in history[-5:]:
                        status = f"{h['success_count']}/{h['node_count']}"
                        table.add_row(h['timestamp'], h.get('command', 'N/A')[:30], status)
                    self.console.print(table)
                else:
                    self.console.print("[yellow]无历史记录[/yellow]")


class ProgressChart:
    """实时进度图表"""

    def __init__(self, total: int, logger: logging.Logger):
        if not HAS_RICH:
            raise ImportError("需要安装 rich 库: pip install rich")
        self.total = total
        self.current = 0
        self.logger = logger
        self.console = Console()
        self.success_count = 0
        self.fail_count = 0
        self.start_time = time.time()
        self.node_times: list[tuple[str, float, bool]] = []

    def update(self, node_name: str, duration: float, success: bool):
        """更新进度"""
        self.current += 1
        self.node_times.append((node_name, duration, success))
        if success:
            self.success_count += 1
        else:
            self.fail_count += 1

    def render_chart(self) -> Table:
        """渲染进度图表"""
        elapsed = time.time() - self.start_time
        avg_time = elapsed / self.current if self.current > 0 else 0

        # 进度条表
        table = Table(title=f"执行进度 ({self.current}/{self.total})", show_header=True)
        table.add_column("节点", style="cyan", width=20)
        table.add_column("耗时", style="magenta", width=10)
        table.add_column("状态", style="yellow", width=10)
        table.add_column("进度", style="green", width=20)

        # 添加节点状态
        for node_name, duration, success in self.node_times[-10:]:  # 显示最近10个
            status = "✓" if success else "✗"
            status_style = "green" if success else "red"
            bar_len = int(20 * self.current / self.total)
            bar = "█" * bar_len + "░" * (20 - bar_len)

            table.add_row(
                node_name,
                f"{duration:.2f}s",
                f"[{status_style}]{status}[/{status_style}]",
                bar
            )

        return table

    def render_stats(self) -> Panel:
        """渲染统计信息"""
        elapsed = time.time() - self.start_time
        progress_pct = self.current / self.total * 100 if self.total > 0 else 0
        avg_time = sum(t[1] for t in self.node_times) / len(self.node_times) if self.node_times else 0

        stats_text = f"""
进度: {progress_pct:.1f}% ({self.current}/{self.total})
成功: {self.success_count}  失败: {self.fail_count}
已用时间: {elapsed:.1f}s
平均耗时: {avg_time:.2f}s
预估剩余: {(self.total - self.current) * avg_time:.1f}s
"""

        return Panel(stats_text, title="执行统计", style="bold cyan")

    def display(self):
        """显示进度图表"""
        with Live(self.console, refresh_per_second=2) as live:
            while self.current < self.total:
                layout = Layout()
                layout.split_column(
                    Layout(self.render_chart()),
                    Layout(self.render_stats())
                )
                live.update(layout)
                time.sleep(0.5)

    def close(self):
        """结束显示"""
        self.console.print()
        final_table = Table(title="最终结果", show_header=True)
        final_table.add_column("统计", style="cyan")
        final_table.add_column("数值", style="green")

        elapsed = time.time() - self.start_time
        final_table.add_row("总耗时", f"{elapsed:.2f}s")
        final_table.add_row("成功数", str(self.success_count))
        final_table.add_row("失败数", str(self.fail_count))
        final_table.add_row("成功率", f"{self.success_count/self.total*100:.1f}%")

        self.console.print(final_table)


class SideBySideView:
    """结果对比视图"""

    def __init__(self, results: list[ExecutionResult], logger: logging.Logger):
        if not HAS_RICH:
            raise ImportError("需要安装 rich 库: pip install rich")
        self.results = results
        self.logger = logger
        self.console = Console()

    def render(self) -> Layout:
        """渲染对比视图"""
        layout = Layout()

        # 根据结果数量决定布局
        if len(self.results) <= 2:
            layout.split_row(*[Layout(name=f"node_{i}") for i in range(len(self.results))])
        else:
            # 分成两行
            layout.split_column(
                Layout(name="top"),
                Layout(name="bottom")
            )
            half = len(self.results) // 2
            layout["top"].split_row(*[Layout(name=f"node_{i}") for i in range(half)])
            layout["bottom"].split_row(*[Layout(name=f"node_{i}") for i in range(half, len(self.results))])

        # 为每个节点添加内容
        for i, result in enumerate(self.results):
            panel = self._create_node_panel(result)
            layout[f"node_{i}"].update(panel)

        return layout

    def _create_node_panel(self, result: ExecutionResult) -> Panel:
        """创建节点面板"""
        title = f"{result.node_name} ({result.host})"

        if result.success:
            content = f"[green]成功[/green] ({result.duration:.2f}s)\n\n"
            content += result.stdout[:200] if result.stdout else "无输出"
            style = "green"
        else:
            content = f"[red]失败[/red] ({result.duration:.2f}s)\n\n"
            content += result.stderr[:200] if result.stderr else result.error or "未知错误"
            style = "red"

        return Panel(content, title=title, style=style)

    def display(self):
        """显示对比视图"""
        self.console.print(Panel("结果对比视图", style="bold cyan"))
        self.console.print(self.render())

        # 显示差异摘要
        self._display_diff_summary()

    def _display_diff_summary(self):
        """显示差异摘要"""
        success_results = [r for r in self.results if r.success]

        if len(success_results) < 2:
            self.console.print("[yellow]没有足够的结果进行对比[/yellow]")
            return

        # 对比输出长度
        outputs = [(r.node_name, len(r.stdout)) for r in success_results]

        diff_table = Table(title="输出对比", show_header=True)
        diff_table.add_column("节点", style="cyan")
        diff_table.add_column("输出长度", style="green")
        diff_table.add_column("差异", style="yellow")

        base_len = outputs[0][1]
        for node_name, length in outputs:
            diff = length - base_len
            diff_str = f"+{diff}" if diff > 0 else str(diff) if diff < 0 else "0"
            diff_table.add_row(node_name, str(length), diff_str)

        self.console.print(diff_table)

        # 对比关键指标（如果有）
        self.console.print("\n[bold]关键差异:[/bold]")
        for i, r1 in enumerate(success_results):
            for j, r2 in enumerate(success_results[i+1:], i+1):
                if r1.stdout != r2.stdout:
                    self.console.print(f"  - {r1.node_name} vs {r2.node_name}: 输出不同")


class HistorySearcher:
    """命令历史搜索"""

    def __init__(self, history_file: str = "/tmp/exec_history.json",
                 logger: logging.Logger = None):
        self.history_file = history_file
        self.logger = logger or logging.getLogger("batch_exec")
        self.history: list[dict] = []

    def load_history(self) -> bool:
        """加载历史"""
        if not os.path.exists(self.history_file):
            self.logger.warning(f"历史文件不存在: {self.history_file}")
            return False

        try:
            with open(self.history_file, 'r', encoding='utf-8') as f:
                self.history = json.load(f)
            return True
        except Exception as e:
            self.logger.error(f"加载历史失败: {e}")
            return False

    def search(self, keyword: str, field: str = "command") -> list[dict]:
        """搜索历史"""
        if not self.history:
            if not self.load_history():
                return []

        results = []
        for entry in self.history:
            # 搜索指定字段
            if field in entry and keyword.lower() in str(entry[field]).lower():
                results.append(entry)

        return results

    def search_by_date(self, date_str: str) -> list[dict]:
        """按日期搜索"""
        if not self.history:
            if not self.load_history():
                return []

        results = []
        for entry in self.history:
            if 'timestamp' in entry:
                if date_str in entry['timestamp']:
                    results.append(entry)

        return results

    def search_by_status(self, success_only: bool = True) -> list[dict]:
        """按状态搜索"""
        if not self.history:
            if not self.load_history():
                return []

        results = []
        for entry in self.history:
            if success_only:
                if entry.get('success_count', 0) == entry.get('node_count', 0):
                    results.append(entry)
            else:
                if entry.get('fail_count', 0) > 0:
                    results.append(entry)

        return results

    def display_results(self, results: list[dict], use_rich: bool = True):
        """显示搜索结果"""
        if not results:
            print("未找到匹配的历史记录")
            return

        if use_rich and HAS_RICH:
            console = Console()
            table = Table(title=f"搜索结果 ({len(results)}条)", show_header=True)
            table.add_column("时间", style="cyan", width=20)
            table.add_column("命令/脚本", style="green", width=30)
            table.add_column("节点数", style="yellow", width=10)
            table.add_column("成功/失败", style="magenta", width=12)
            table.add_column("耗时", style="blue", width=10)

            for entry in results:
                cmd = entry.get('command') or entry.get('script') or 'N/A'
                cmd_display = cmd[:30] if len(cmd) > 30 else cmd
                result_str = f"{entry.get('success_count', 0)}/{entry.get('fail_count', 0)}"
                duration = f"{entry.get('duration', 0):.1f}s"

                table.add_row(
                    entry.get('timestamp', 'N/A'),
                    cmd_display,
                    str(entry.get('node_count', 0)),
                    result_str,
                    duration
                )

            console.print(table)
        else:
            print(f"\n找到 {len(results)} 条记录:")
            print("-" * 80)
            for entry in results:
                cmd = entry.get('command') or entry.get('script') or 'N/A'
                print(f"时间: {entry.get('timestamp', 'N/A')}")
                print(f"命令: {cmd}")
                print(f"结果: {entry.get('success_count', 0)}成功/{entry.get('fail_count', 0)}失败")
                print("-" * 40)

    def interactive_search(self):
        """交互式搜索"""
        if not self.load_history():
            print("无法加载历史文件")
            return

        if HAS_RICH:
            console = Console()
            console.print(Panel("命令历史搜索", style="bold cyan"))

            while True:
                console.print("\n[bold]搜索选项:[/bold]")
                console.print("  1. 搜索命令关键词")
                console.print("  2. 搜索日期")
                console.print("  3. 查看成功记录")
                console.print("  4. 查看失败记录")
                console.print("  5. 查看所有历史")
                console.print("  q. 退出")

                choice = console.input("[bold cyan]选择: [/bold cyan]")

                if choice == 'q':
                    break
                elif choice == '1':
                    keyword = console.input("[bold]关键词: [/bold]")
                    results = self.search(keyword, "command")
                    self.display_results(results)
                elif choice == '2':
                    date_str = console.input("[bold]日期 (如: 2026-04): [/bold]")
                    results = self.search_by_date(date_str)
                    self.display_results(results)
                elif choice == '3':
                    results = self.search_by_status(success_only=True)
                    self.display_results(results)
                elif choice == '4':
                    results = self.search_by_status(success_only=False)
                    self.display_results(results)
                elif choice == '5':
                    self.display_results(self.history[-20:])  # 最近20条
        else:
            # 无rich库的简单交互
            while True:
                print("\n搜索选项:")
                print("  1. 搜索命令关键词")
                print("  2. 搜索日期")
                print("  3. 查看最近历史")
                print("  q. 退出")

                choice = input("选择: ")

                if choice == 'q':
                    break
                elif choice == '1':
                    keyword = input("关键词: ")
                    results = self.search(keyword, "command")
                    self.display_results(results, use_rich=False)
                elif choice == '2':
                    date_str = input("日期 (如: 2026-04): ")
                    results = self.search_by_date(date_str)
                    self.display_results(results, use_rich=False)
                elif choice == '3':
                    self.display_results(self.history[-10:], use_rich=False)


@dataclass
class ConnectionPool:
    """SSH连接池"""
    pool: dict[str, paramiko.SSHClient] = field(default_factory=dict)
    max_connections: int = 10
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("batch_exec"))

    def get_connection(self, node: NodeConfig, timeout: int) -> Optional[paramiko.SSHClient]:
        """获取连接（复用或新建）"""
        key = f"{node.host}:{node.port}:{node.username}"

        if key in self.pool:
            client = self.pool[key]
            try:
                transport = client.get_transport()
                if transport and transport.is_active():
                    self.logger.debug(f"复用连接: {key}")
                    return client
            except Exception:
                pass

        # 新建连接
        if len(self.pool) >= self.max_connections:
            # 关闭最老的连接
            self._close_oldest()

        client = self._create_connection(node, timeout)
        if client:
            self.pool[key] = client
        return client

    def _create_connection(self, node: NodeConfig, timeout: int) -> Optional[paramiko.SSHClient]:
        """创建新的SSH连接"""
        try:
            client = paramiko.SSHClient()
            # 使用更安全的策略
            known_hosts = os.path.expanduser("~/.ssh/known_hosts")
            if os.path.exists(known_hosts):
                client.load_host_keys(known_hosts)
                client.set_missing_host_key_policy(paramiko.RejectPolicy())
            else:
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs: dict[str, Any] = {
                'hostname': node.host,
                'port': node.port,
                'username': node.username,
                'timeout': timeout,
                'allow_agent': False,
                'look_for_keys': False,
            }

            if node.private_key:
                key = paramiko.RSAKey.from_private_key_file(node.private_key)
                connect_kwargs['pkey'] = key
            elif node.password:
                connect_kwargs['password'] = node.password
            else:
                raise ValueError(f"节点 {node.name} 未配置密码或密钥")

            client.connect(**connect_kwargs)
            self.logger.debug(f"新建连接: {node.host}:{node.port}")
            return client

        except AuthenticationException as e:
            raise Exception(f"认证失败: {e}")
        except SSHException as e:
            raise Exception(f"SSH连接错误: {e}")
        except Exception as e:
            raise Exception(f"连接失败: {e}")

    def _close_oldest(self):
        """关闭最老的连接"""
        if self.pool:
            key = next(iter(self.pool))
            try:
                self.pool[key].close()
            except Exception:
                pass
            del self.pool[key]
            self.logger.debug(f"关闭连接: {key}")

    def close_all(self):
        """关闭所有连接"""
        for client in self.pool.values():
            try:
                client.close()
            except Exception:
                pass
        self.pool.clear()


# 全局连接池（用于 --use-pool 模式下跨节点复用连接）
_GLOBAL_POOL: Optional[ConnectionPool] = None


# ============== SSH客户端封装 ==============
class SSHClientWrapper:
    """SSH客户端封装（带重试机制）"""

    def __init__(
        self,
        node: NodeConfig,
        timeout: int = 30,
        pool: Optional[ConnectionPool] = None,
        use_pool: bool = True,
        logger: Optional[logging.Logger] = None,
        ssh_agent_forwarding: bool = False,
        verify_host_key: bool = False,
        auto_accept_host: bool = False
    ):
        self.node = node
        self.timeout = timeout
        self.use_pool = use_pool
        self.logger = logger or logging.getLogger("batch_exec")
        self.pool = pool
        self.client: Optional[paramiko.SSHClient] = None
        self.ssh_agent_forwarding = ssh_agent_forwarding
        self.verify_host_key = verify_host_key
        self.auto_accept_host = auto_accept_host

    def connect_with_retry(self, retry_times: int = 3, retry_delay: float = 1.0) -> bool:
        """带重试的连接"""
        last_error = None

        for attempt in range(retry_times):
            try:
                if self.use_pool and self.pool:
                    self.client = self.pool.get_connection(self.node, self.timeout)
                else:
                    self.client = self._create_connection()
                return True
            except Exception as e:
                last_error = e
                self.logger.warning(
                    f"连接 {self.node.name} 失败 (尝试 {attempt + 1}/{retry_times}): {e}"
                )
                if attempt < retry_times - 1:
                    time.sleep(retry_delay)

        raise last_error or Exception("连接失败")

    def _create_connection(self) -> paramiko.SSHClient:
        """创建新连接"""
        client = paramiko.SSHClient()
        known_hosts = os.path.expanduser("~/.ssh/known_hosts")
        if os.path.exists(known_hosts):
            client.load_host_keys(known_hosts)
            client.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs: dict[str, Any] = {
            'hostname': self.node.host,
            'port': self.node.port,
            'username': self.node.username,
            'timeout': self.timeout,
            'allow_agent': self.ssh_agent_forwarding,
            'look_for_keys': self.ssh_agent_forwarding,
        }

        if self.node.private_key:
            key = paramiko.RSAKey.from_private_key_file(self.node.private_key)
            connect_kwargs['pkey'] = key
        elif self.node.password:
            connect_kwargs['password'] = self.node.password
        elif not self.ssh_agent_forwarding:
            raise ValueError(f"节点 {self.node.name} 未配置密码或密钥")

        client.connect(**connect_kwargs)

        # 主机密钥验证
        if self.verify_host_key:
            if not verify_host_fingerprint(client, self.node.host, self.node.port,
                                          auto_accept=self.auto_accept_host, logger=self.logger):
                client.close()
                raise Exception(f"主机密钥验证失败: {self.node.host}")

        return client

    def execute(self, command: str, command_timeout: Optional[int] = None) -> tuple[int, str, str]:
        """执行命令，支持sudo"""
        if not self.client:
            raise Exception("未建立SSH连接")

        timeout = command_timeout or self.timeout

        # 如果配置了sudo密码，使用sudo执行
        if self.node.sudo_password:
            sudo_user = self.node.sudo_user or "root"
            escaped_command = command.replace("'", "'\"'\"'")
            full_command = f"sudo -S -u {sudo_user} bash -c '{escaped_command}'"
            stdin, stdout, stderr = self.client.exec_command(full_command, timeout=timeout)
            stdin.write(self.node.sudo_password + '\n')
            stdin.flush()
        else:
            stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)

        exit_code = stdout.channel.recv_exit_status()
        stdout_text = stdout.read().decode('utf-8', errors='replace')
        stderr_text = stderr.read().decode('utf-8', errors='replace')

        return exit_code, stdout_text, stderr_text

    def execute_script(self, script_path: str, command_timeout: Optional[int] = None) -> tuple[int, str, str]:
        """执行本地脚本文件"""
        if not self.client:
            raise Exception("未建立SSH连接")

        with open(script_path, 'r', encoding='utf-8') as f:
            script_content = f.read()

        remote_path = f"/tmp/batch_exec_{int(time.time())}.sh"
        sftp = self.client.open_sftp()
        try:
            with sftp.open(remote_path, 'w') as f:
                f.write(script_content)

            self.execute(f"chmod +x {remote_path}", command_timeout)
            exit_code, stdout, stderr = self.execute(f"bash {remote_path}", command_timeout)
            self.execute(f"rm -f {remote_path}", command_timeout)

            return exit_code, stdout, stderr
        except Exception as e:
            # 尝试清理临时文件
            try:
                self.execute(f"rm -f {remote_path}", 5)
            except Exception:
                pass
            raise e
        finally:
            sftp.close()

    def upload_file(self, local_path: str, remote_path: str) -> tuple[int, str, str]:
        """上传文件到远端"""
        if not self.client:
            raise Exception("未建立SSH连接")

        sftp = self.client.open_sftp()
        try:
            sftp.put(local_path, remote_path)
            return 0, f"上传成功: {local_path} -> {remote_path}", ""
        except Exception as e:
            return 1, "", str(e)
        finally:
            sftp.close()

    def download_file(self, remote_path: str, local_path: str) -> tuple[int, str, str]:
        """从远端下载文件"""
        if not self.client:
            raise Exception("未建立SSH连接")

        sftp = self.client.open_sftp()
        try:
            sftp.get(remote_path, local_path)
            return 0, f"下载成功: {remote_path} -> {local_path}", ""
        except Exception as e:
            return 1, "", str(e)
        finally:
            sftp.close()

    def sync_directory(self, local_dir: str, remote_dir: str) -> tuple[int, str, str]:
        """递归同步目录到远端"""
        if not self.client:
            raise Exception("未建立SSH连接")

        import stat

        sftp = self.client.open_sftp()
        try:
            # 确保远端目录存在
            try:
                sftp.stat(remote_dir)
            except FileNotFoundError:
                self.execute(f"mkdir -p {remote_dir}")

            # 递归上传
            uploaded_files = []
            local_dir_path = Path(local_dir)

            for local_file in local_dir_path.rglob('*'):
                if local_file.is_file():
                    relative_path = local_file.relative_to(local_dir_path)
                    remote_file_path = f"{remote_dir}/{relative_path}"

                    # 确保远端子目录存在
                    remote_subdir = str(relative_path.parent)
                    if remote_subdir != '.':
                        self.execute(f"mkdir -p {remote_dir}/{remote_subdir}")

                    sftp.put(str(local_file), remote_file_path)
                    uploaded_files.append(str(relative_path))

            return 0, f"同步成功: {len(uploaded_files)} 个文件", ""
        except Exception as e:
            return 1, "", str(e)
        finally:
            sftp.close()

    def close(self, keep_in_pool: bool = True):
        """关闭连接"""
        if self.client and self.pool and keep_in_pool:
            # 连接会保留在池中，由池统一管理
            pass
        elif self.client:
            self.client.close()
            self.client = None

    def health_check(self) -> HealthStatus:
        """获取节点健康状态"""
        if not self.client:
            raise Exception("未建立SSH连接")

        try:
            # 获取CPU使用率
            exit_code, stdout, _ = self.execute("top -bn1 | grep 'Cpu(s)' | awk '{print $2}' | cut -d'%' -f1", 10)
            cpu_usage = float(stdout.strip()) if exit_code == 0 and stdout.strip() else None

            # 获取内存使用率
            exit_code, stdout, _ = self.execute("free | grep Mem | awk '{print $3/$2 * 100.0}'", 10)
            memory_usage = float(stdout.strip()) if exit_code == 0 and stdout.strip() else None

            # 获取磁盘使用率
            exit_code, stdout, _ = self.execute("df -h / | tail -1 | awk '{print $5}' | cut -d'%' -f1", 10)
            disk_usage = float(stdout.strip()) if exit_code == 0 and stdout.strip() else None

            # 获取uptime
            exit_code, stdout, _ = self.execute("uptime -p", 10)
            uptime = stdout.strip() if exit_code == 0 else None

            # 获取负载
            exit_code, stdout, _ = self.execute("cat /proc/loadavg | awk '{print $1,$2,$3}'", 10)
            load_avg = stdout.strip() if exit_code == 0 else None

            return HealthStatus(
                node_name=self.node.name,
                host=self.node.host,
                connected=True,
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                disk_usage=disk_usage,
                uptime=uptime,
                load_avg=load_avg
            )
        except Exception as e:
            return HealthStatus(
                node_name=self.node.name,
                host=self.node.host,
                connected=False,
                error=str(e)
            )

    def tail_file(self, remote_path: str, lines: int = 20) -> tuple[int, str, str]:
        """查看远程文件尾部内容"""
        if not self.client:
            raise Exception("未建立SSH连接")

        return self.execute(f"tail -n {lines} {remote_path}", self.timeout)

    def service_status(self, service_name: str) -> ServiceStatus:
        """检查systemd服务状态"""
        if not self.client:
            raise Exception("未建立SSH连接")

        try:
            exit_code, stdout, stderr = self.execute(f"systemctl status {service_name}", 10)

            is_running = "active (running)" in stdout or exit_code == 0
            is_enabled = "enabled" in stdout

            # 获取启动时间
            active_since = None
            if "since" in stdout:
                import re
                match = re.search(r'since\s+([^;]+)', stdout)
                if match:
                    active_since = match.group(1).strip()

            return ServiceStatus(
                node_name=self.node.name,
                host=self.node.host,
                service_name=service_name,
                is_running=is_running,
                is_enabled=is_enabled,
                active_since=active_since,
                error=stderr if exit_code != 0 else None
            )
        except Exception as e:
            return ServiceStatus(
                node_name=self.node.name,
                host=self.node.host,
                service_name=service_name,
                is_running=False,
                is_enabled=False,
                error=str(e)
            )


# ============== 配置加载与校验 ==============
def validate_config(config: dict) -> list[str]:
    """验证配置文件，返回错误列表"""
    errors = []

    if 'nodes' not in config or not config['nodes']:
        errors.append("配置文件中缺少 'nodes' 或节点列表为空")

    for i, node in enumerate(config.get('nodes', [])):
        if 'host' not in node:
            errors.append(f"节点 {i+1}: 缺少 'host' 字段")
        if 'username' not in node:
            errors.append(f"节点 {i+1}: 缺少 'username' 字段")
        if not node.get('password') and not node.get('private_key') and not node.get('encrypted_password'):
            errors.append(f"节点 {i+1} ({node.get('host', 'N/A')}): 未配置密码、私钥或加密密码")

    return errors


def load_config(config_path: str, logger: logging.Logger) -> tuple[list[NodeConfig], Settings]:
    """加载配置文件"""
    config_path = os.path.expanduser(config_path)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=SafeLoader)

    # 验证配置
    errors = validate_config(config)
    if errors:
        raise ValueError("配置错误:\n  " + "\n  ".join(errors))

    settings_data = config.get('settings', {})
    settings = Settings(
        timeout=settings_data.get('timeout', 30),
        parallel=settings_data.get('parallel', True),
        max_workers=settings_data.get('max_workers', 5),
        retry_times=settings_data.get('retry_times', 3),
        retry_delay=settings_data.get('retry_delay', 1.0),
        sudo_password=settings_data.get('sudo_password'),
        ssh_agent_forwarding=settings_data.get('ssh_agent_forwarding', False),
        verify_host_key=settings_data.get('verify_host_key', False)
    )

    nodes = []
    global_sudo_password = settings.sudo_password

    # 获取解密密钥
    key = None
    if any(node.get('encrypted_password') for node in config.get('nodes', [])):
        if HAS_CRYPTO:
            key = get_or_create_key()
        else:
            logger.warning("配置中有加密密码但未安装 cryptography 库，将无法解密")

    for node_data in config.get('nodes', []):
        private_key = node_data.get('private_key')
        if private_key:
            private_key = os.path.expanduser(private_key)

        # 处理密码
        password = node_data.get('password')
        if node_data.get('encrypted_password') and key:
            try:
                password = decrypt_password(node_data.get('encrypted_password'), key)
            except Exception as e:
                logger.warning(f"节点 {node_data.get('name', 'N/A')} 密码解密失败: {e}")

        node = NodeConfig(
            name=node_data.get('name', node_data.get('host')),
            host=node_data['host'],
            port=node_data.get('port', 22),
            username=node_data['username'],
            password=password,
            private_key=private_key,
            sudo_password=node_data.get('sudo_password', global_sudo_password),
            sudo_user=node_data.get('sudo_user'),
            tags=node_data.get('tags', [])
        )
        nodes.append(node)

    logger.info(f"已加载 {len(nodes)} 个节点配置")
    return nodes, settings


def expand_env_vars(config: dict) -> dict:
    """展开环境变量"""
    def expand_value(value):
        if isinstance(value, str):
            if value.startswith('${') and value.endswith('}'):
                env_var = value[2:-1]
                return os.environ.get(env_var, value)
            return os.path.expanduser(value)
        elif isinstance(value, dict):
            return {k: expand_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [expand_value(v) for v in value]
        return value

    return expand_value(config)


# ============== 执行逻辑 ==============
def execute_on_node(
    node: NodeConfig,
    command: Optional[str],
    script: Optional[str],
    timeout: int,
    use_pool: bool = False,
    retry_times: int = 3,
    retry_delay: float = 1.0,
    logger: Optional[logging.Logger] = None,
    template_vars: Optional[dict] = None
) -> ExecutionResult:
    """在单个节点上执行命令"""
    start_time = time.time()
    logger = logger or logging.getLogger("batch_exec")

    # 如果有模板变量，渲染命令
    if template_vars and command:
        command = render_template(command, template_vars, node)

    try:
        pool = None
        if use_pool:
            # 使用全局连接池，在主程序中统一创建和关闭
            global _GLOBAL_POOL
            pool = _GLOBAL_POOL
            if pool is None:
                # 兜底：如果主程序未初始化全局连接池，这里按旧行为创建一个本地池
                pool = ConnectionPool(max_connections=10, logger=logger)

        wrapper = SSHClientWrapper(node, timeout, pool, use_pool, logger)
        wrapper.connect_with_retry(retry_times, retry_delay)

        command_timeout = timeout * 2  # 命令执行时间可以长一些

        if command:
            exit_code, stdout, stderr = wrapper.execute(command, command_timeout)
        elif script:
            exit_code, stdout, stderr = wrapper.execute_script(script, command_timeout)
        else:
            raise ValueError("未指定命令或脚本")

        wrapper.close(keep_in_pool=use_pool)

        duration = time.time() - start_time
        return ExecutionResult(
            node_name=node.name,
            host=node.host,
            success=(exit_code == 0),
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            duration=duration
        )

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"节点 {node.name} 执行失败: {e}")
        return ExecutionResult(
            node_name=node.name,
            host=node.host,
            success=False,
            stdout='',
            stderr=str(e),
            exit_code=-1,
            duration=duration,
            error=str(e)
        )


def transfer_on_node(
    node: NodeConfig,
    transfer_type: str,  # 'upload', 'download', 'sync'
    local_path: str,
    remote_path: str,
    timeout: int,
    use_pool: bool = False,
    retry_times: int = 3,
    retry_delay: float = 1.0,
    logger: Optional[logging.Logger] = None
) -> TransferResult:
    """在单个节点上进行文件传输"""
    start_time = time.time()
    logger = logger or logging.getLogger("batch_exec")

    try:
        pool = None
        if use_pool:
            global _GLOBAL_POOL
            pool = _GLOBAL_POOL
            if pool is None:
                pool = ConnectionPool(max_connections=10, logger=logger)

        wrapper = SSHClientWrapper(node, timeout, pool, use_pool, logger)
        wrapper.connect_with_retry(retry_times, retry_delay)

        if transfer_type == 'upload':
            exit_code, stdout, stderr = wrapper.upload_file(local_path, remote_path)
        elif transfer_type == 'download':
            # 下载时为每个节点添加前缀避免冲突
            safe_node_name = node.name.replace('/', '_').replace(':', '_')
            local_file = os.path.join(local_path, f"{safe_node_name}_{os.path.basename(remote_path)}")
            exit_code, stdout, stderr = wrapper.download_file(remote_path, local_file)
            local_path = local_file  # 更新实际保存路径
        elif transfer_type == 'sync':
            exit_code, stdout, stderr = wrapper.sync_directory(local_path, remote_path)
        else:
            raise ValueError(f"未知的传输类型: {transfer_type}")

        wrapper.close(keep_in_pool=use_pool)

        # 获取传输字节数
        bytes_transferred = 0
        if transfer_type == 'upload' and os.path.exists(local_path):
            bytes_transferred = os.path.getsize(local_path)

        duration = time.time() - start_time
        return TransferResult(
            node_name=node.name,
            host=node.host,
            success=(exit_code == 0),
            local_path=local_path,
            remote_path=remote_path,
            bytes_transferred=bytes_transferred,
            duration=duration,
            error=stderr if exit_code != 0 else None
        )

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"节点 {node.name} 传输失败: {e}")
        return TransferResult(
            node_name=node.name,
            host=node.host,
            success=False,
            local_path=local_path,
            remote_path=remote_path,
            bytes_transferred=0,
            duration=duration,
            error=str(e)
        )


def print_transfer_result(result: TransferResult, logger: Optional[logging.Logger] = None):
    """打印传输结果"""
    logger = logger or logging.getLogger("batch_exec")
    status = "✓" if result.success else "✗"
    logger.info(f"[{status}] {result.node_name} ({result.host}) - {result.duration:.2f}s")
    if result.success:
        logger.info(f"    {result.local_path} -> {result.remote_path}")
        if result.bytes_transferred > 0:
            size_kb = result.bytes_transferred / 1024
            logger.info(f"    传输大小: {size_kb:.1f} KB")
    if result.error:
        logger.error(f"    错误: {result.error}")


def health_check_on_node(
    node: NodeConfig,
    timeout: int,
    use_pool: bool = False,
    retry_times: int = 3,
    retry_delay: float = 1.0,
    logger: Optional[logging.Logger] = None
) -> HealthStatus:
    """对单个节点进行健康检查"""
    logger = logger or logging.getLogger("batch_exec")

    try:
        pool = None
        if use_pool:
            global _GLOBAL_POOL
            pool = _GLOBAL_POOL
            if pool is None:
                pool = ConnectionPool(max_connections=10, logger=logger)

        wrapper = SSHClientWrapper(node, timeout, pool, use_pool, logger)
        wrapper.connect_with_retry(retry_times, retry_delay)

        status = wrapper.health_check()
        wrapper.close(keep_in_pool=use_pool)
        return status

    except Exception as e:
        return HealthStatus(
            node_name=node.name,
            host=node.host,
            connected=False,
            error=str(e)
        )


def service_status_on_node(
    node: NodeConfig,
    service_name: str,
    timeout: int,
    use_pool: bool = False,
    retry_times: int = 3,
    retry_delay: float = 1.0,
    logger: Optional[logging.Logger] = None
) -> ServiceStatus:
    """检查单个节点的服务状态"""
    logger = logger or logging.getLogger("batch_exec")

    try:
        pool = None
        if use_pool:
            global _GLOBAL_POOL
            pool = _GLOBAL_POOL
            if pool is None:
                pool = ConnectionPool(max_connections=10, logger=logger)

        wrapper = SSHClientWrapper(node, timeout, pool, use_pool, logger)
        wrapper.connect_with_retry(retry_times, retry_delay)

        status = wrapper.service_status(service_name)
        wrapper.close(keep_in_pool=use_pool)
        return status

    except Exception as e:
        return ServiceStatus(
            node_name=node.name,
            host=node.host,
            service_name=service_name,
            is_running=False,
            is_enabled=False,
            error=str(e)
        )


def print_health_status(status: HealthStatus, logger: Optional[logging.Logger] = None):
    """打印健康状态"""
    logger = logger or logging.getLogger("batch_exec")

    if status.connected:
        logger.info(f"[✓] {status.node_name} ({status.host}) - 健康")
        if status.cpu_usage:
            logger.info(f"    CPU: {status.cpu_usage:.1f}%")
        if status.memory_usage:
            logger.info(f"    内存: {status.memory_usage:.1f}%")
        if status.disk_usage:
            logger.info(f"    磁盘: {status.disk_usage:.1f}%")
        if status.uptime:
            logger.info(f"    运行时间: {status.uptime}")
        if status.load_avg:
            logger.info(f"    负载: {status.load_avg}")
    else:
        logger.error(f"[✗] {status.node_name} ({status.host}) - 不健康")
        if status.error:
            logger.error(f"    错误: {status.error}")


def print_service_status(status: ServiceStatus, logger: Optional[logging.Logger] = None):
    """打印服务状态"""
    logger = logger or logging.getLogger("batch_exec")

    running_icon = "✓" if status.is_running else "✗"
    enabled_icon = "✓" if status.is_enabled else "✗"

    logger.info(f"[{running_icon}] {status.node_name} ({status.host}) - {status.service_name}")
    logger.info(f"    运行: {'是' if status.is_running else '否'}")
    logger.info(f"    启用: {'是' if status.is_enabled else '否'}")
    if status.active_since:
        logger.info(f"    启动时间: {status.active_since}")
    if status.error:
        logger.error(f"    错误: {status.error[:100]}")


def render_template(template: str, variables: dict[str, str], node: Optional[NodeConfig] = None) -> str:
    """渲染命令模板，替换变量"""
    result = template

    # 替换用户提供的变量
    for key, value in variables.items():
        result = result.replace(f"{{{key}}}", value)

    # 替换节点相关变量
    if node:
        result = result.replace("{node}", node.name)
        result = result.replace("{host}", node.host)
        result = result.replace("{port}", str(node.port))

    # 替换内置变量
    result = result.replace("{date}", time.strftime("%Y-%m-%d"))
    result = result.replace("{time}", time.strftime("%H:%M:%S"))
    result = result.replace("{timestamp}", time.strftime("%Y%m%d_%H%M%S"))

    return result


def save_execution_history(history: ExecutionHistory, history_file: str):
    """保存执行历史到JSON文件"""
    history_data = {
        "timestamp": history.timestamp,
        "command": history.command,
        "script": history.script,
        "transfer_type": history.transfer_type,
        "monitor_type": history.monitor_type,
        "node_count": history.node_count,
        "success_count": history.success_count,
        "fail_count": history.fail_count,
        "duration": history.duration,
        "nodes": history.nodes
    }

    # 读取现有历史
    existing_history = []
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                existing_history = json.load(f)
        except Exception:
            pass

    existing_history.append(history_data)

    # 写入历史文件
    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(existing_history, f, ensure_ascii=False, indent=2)


def check_condition(condition: str, total_count: int, current_count: int) -> bool:
    """检查条件执行条件是否满足"""
    import re

    # 解析条件表达式，如 "success_count >= 50%"
    match = re.match(r'(success_count|fail_count)\s*(>=|<=|>|<|=)\s*(\d+)%?', condition)
    if not match:
        return False

    metric = match.group(1)
    operator = match.group(2)
    threshold = int(match.group(3))

    # 如果是百分比，转换为实际数量
    if condition.endswith('%'):
        threshold = int(total_count * threshold / 100)

    if metric == 'success_count':
        value = current_count
    elif metric == 'fail_count':
        value = total_count - current_count
    else:
        return False

    # 执行比较
    if operator == '>=':
        return value >= threshold
    elif operator == '<=':
        return value <= threshold
    elif operator == '>':
        return value > threshold
    elif operator == '<':
        return value < threshold
    elif operator == '=':
        return value == threshold

    return False


def print_result(result: ExecutionResult, verbose: bool = False, logger: Optional[logging.Logger] = None):
    """打印执行结果"""
    logger = logger or logging.getLogger("batch_exec")
    status = "✓" if result.success else "✗"
    logger.info(f"[{status}] {result.node_name} ({result.host}) - {result.duration:.2f}s")

    if result.error:
        logger.error(f"    错误: {result.error}")

    if verbose or result.stdout:
        if result.stdout.strip():
            logger.info(f"    输出:")
            for line in result.stdout.strip().split('\n'):
                logger.info(f"        {line}")

    if result.stderr and not result.success:
        if result.stderr.strip():
            logger.error(f"    错误输出:")
            for line in result.stderr.strip().split('\n'):
                logger.error(f"        {line}")


def export_results_json(results: list[ExecutionResult], output_path: str):
    """导出结果为JSON格式"""
    data = [
        {
            "node_name": r.node_name,
            "host": r.host,
            "success": r.success,
            "stdout": r.stdout,
            "stderr": r.stderr,
            "exit_code": r.exit_code,
            "duration": r.duration,
            "error": r.error
        }
        for r in results
    ]
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def export_results_csv(results: list[ExecutionResult], output_path: str):
    """导出结果为CSV格式"""
    import csv
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['node_name', 'host', 'success', 'exit_code', 'duration', 'stdout', 'stderr', 'error'])
        for r in results:
            writer.writerow([
                r.node_name, r.host, r.success, r.exit_code, r.duration,
                r.stdout.replace('\n', '\\n'), r.stderr.replace('\n', '\\n'), r.error
            ])


def export_results_html(results: list[ExecutionResult], output_path: str, title: str = "执行报告"):
    """导出结果为HTML格式"""
    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count
    total_duration = sum(r.duration for r in results)

    rows_html = ""
    for r in results:
        row_class = "success-row" if r.success else "fail-row"
        status = "✓ 成功" if r.success else "✗ 失败"
        output = r.stdout[:500] if r.stdout else ""
        error = r.error or r.stderr[:200] if r.stderr else ""

        rows_html += f'''<tr class="{row_class}">
            <td>{r.node_name}</td>
            <td>{r.host}</td>
            <td>{status}</td>
            <td>{r.exit_code}</td>
            <td>{r.duration:.2f}s</td>
            <td class="output">{output}</td>
            <td class="output">{error}</td>
        </tr>'''

    success_percent = (success_count / len(results) * 100) if results else 0

    # 使用拼接方式构建HTML，避免模板中的大括号冲突
    html_content = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        h1 {{ color: #333; }}
        .summary {{ background: white; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .summary .stat {{ display: inline-block; margin: 10px 20px; }}
        .summary .success {{ color: green; }}
        .summary .fail {{ color: red; }}
        .results {{ background: white; padding: 20px; border-radius: 8px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background: #4CAF50; color: white; }}
        .success-row {{ background: #e8f5e9; }}
        .fail-row {{ background: #ffebee; }}
        .output {{ font-family: monospace; white-space: pre-wrap; max-height: 200px; overflow: auto; }}
        .chart {{ margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <div class="summary">
        <h2>执行摘要</h2>
        <div class="stat">总节点数: <strong>{len(results)}</strong></div>
        <div class="stat success">成功: <strong>{success_count}</strong></div>
        <div class="stat fail">失败: <strong>{fail_count}</strong></div>
        <div class="stat">总耗时: <strong>{total_duration:.2f}s</strong></div>
    </div>

    <div class="chart">
        <svg viewBox="0 0 100 100" width="200" height="200">
            <circle cx="50" cy="50" r="40" fill="none" stroke="#ddd" stroke-width="20"/>
            <circle cx="50" cy="50" r="40" fill="none" stroke="#4CAF50" stroke-width="20"
                    stroke-dasharray="{success_percent:.0f} 100" stroke-dashoffset="25"/>
            <text x="50" y="55" text-anchor="middle" font-size="16">{success_percent:.0f}%</text>
        </svg>
    </div>

    <div class="results">
        <h2>详细结果</h2>
        <table>
            <tr>
                <th>节点</th>
                <th>主机</th>
                <th>状态</th>
                <th>退出码</th>
                <th>耗时</th>
                <th>输出</th>
                <th>错误</th>
            </tr>
            {rows_html}
        </table>
    </div>
</body>
</html>'''

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)


def compare_outputs(results: list[ExecutionResult], logger: logging.Logger):
    """比对多节点输出，高亮差异"""
    if len(results) < 2:
        logger.info("只有一个节点输出，无法比对")
        return

    outputs = {r.node_name: r.stdout.split('\n') if r.stdout else [] for r in results}
    node_names = list(outputs.keys())

    logger.info("=" * 60)
    logger.info("输出比对")
    logger.info("=" * 60)

    # 找出所有唯一行
    all_lines = set()
    for lines in outputs.values():
        all_lines.update(lines)

    # 对每个节点标记差异
    for i, line in enumerate(outputs[node_names[0]]):
        line_variations = [outputs[name][i] if i < len(outputs[name]) else "" for name in node_names]

        if len(set(line_variations)) > 1:
            logger.warning(f"行 {i+1} 存在差异:")
            for name, variation in zip(node_names, line_variations):
                logger.warning(f"  [{name}]: {variation}")
        else:
            logger.info(f"行 {i+1}: {line}")


def start_web_dashboard(results: list[ExecutionResult], port: int = 8080):
    """启动Web仪表盘"""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import threading

    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()

            success_count = sum(1 for r in results if r.success)
            fail_count = len(results) - success_count

            html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>执行仪表盘</title>
    <style>
        body {{ font-family: Arial; margin: 20px; background: #1a1a2e; color: white; }}
        .header {{ text-align: center; }}
        .stats {{ display: flex; justify-content: center; gap: 50px; margin: 30px 0; }}
        .stat-box {{ padding: 20px; border-radius: 10px; text-align: center; }}
        .total {{ background: #16213e; }}
        .success {{ background: #0f3460; color: #4CAF50; }}
        .fail {{ background: #16213e; color: #f44336; }}
        .stat-box h2 {{ margin: 0; font-size: 36px; }}
        .stat-box p {{ margin: 5px 0; }}
        .results {{ background: #16213e; padding: 20px; border-radius: 10px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 10px; border-bottom: 1px solid #333; }}
        th {{ background: #0f3460; }}
        .success-row {{ background: rgba(76, 175, 80, 0.2); }}
        .fail-row {{ background: rgba(244, 67, 54, 0.2); }}
    </style>
    <meta http-equiv="refresh" content="5">
</head>
<body>
    <div class="header">
        <h1>批量执行仪表盘</h1>
        <p>实时监控 - 自动刷新</p>
    </div>
    <div class="stats">
        <div class="stat-box total">
            <h2>{len(results)}</h2>
            <p>总节点</p>
        </div>
        <div class="stat-box success">
            <h2>{success_count}</h2>
            <p>成功</p>
        </div>
        <div class="stat-box fail">
            <h2>{fail_count}</h2>
            <p>失败</p>
        </div>
    </div>
    <div class="results">
        <table>
            <tr><th>节点</th><th>主机</th><th>状态</th><th>耗时</th></tr>
'''

            for r in results:
                row_class = "success-row" if r.success else "fail-row"
                status = "✓" if r.success else "✗"
                html += f"<tr class='{row_class}'><td>{r.node_name}</td><td>{r.host}</td><td>{status}</td><td>{r.duration:.2f}s</td></tr>"

            html += '''        </table>
    </div>
</body>
</html>'''

            self.wfile.write(html.encode())

        def log_message(self, format, *args):
            pass  # 禁用日志

    server = HTTPServer(('localhost', port), DashboardHandler)
    print(f"Web仪表盘已启动: http://localhost:{port}")
    print("按 Ctrl+C 退出")
    server.serve_forever()


# ============== 通知功能 ==============
def send_email_notification(to: str, subject: str, body: str, smtp_server: str = None,
                            smtp_port: int = 25, smtp_user: str = None, smtp_pass: str = None):
    """发送邮件通知"""
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        msg = MIMEMultipart()
        msg['From'] = smtp_user or 'batch_exec@localhost'
        msg['To'] = to
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        if smtp_server:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                if smtp_user and smtp_pass:
                    server.starttls()
                    server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            # 本地sendmail
            with smtplib.SMTP('localhost') as server:
                server.send_message(msg)

        return True
    except Exception as e:
        logging.getLogger("batch_exec").error(f"邮件发送失败: {e}")
        return False


def send_webhook_notification(url: str, data: dict):
    """发送Webhook通知"""
    try:
        import requests

        response = requests.post(url, json=data, timeout=10)
        return response.status_code == 200
    except Exception as e:
        logging.getLogger("batch_exec").error(f"Webhook发送失败: {e}")
        return False


def send_dingtalk_alert(token: str, message: str):
    """发送钉钉告警"""
    url = f"https://oapi.dingtalk.com/robot/send?access_token={token}"
    data = {
        "msgtype": "text",
        "text": {"content": message}
    }
    return send_webhook_notification(url, data)


def send_wechat_alert(key: str, message: str):
    """发送企业微信告警"""
    url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={key}"
    data = {
        "msgtype": "text",
        "text": {"content": message}
    }
    return send_webhook_notification(url, data)


def send_notification(notify_type: str, target: str, results: list[ExecutionResult],
                      command: str = None, logger: logging.Logger = None):
    """发送执行通知"""
    logger = logger or logging.getLogger("batch_exec")

    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count

    subject = f"批量执行报告: 成功 {success_count}/{len(results)}"
    body = f"""批量执行报告

执行命令: {command or 'N/A'}
执行时间: {time.strftime('%Y-%m-%d %H:%M:%S')}

结果统计:
- 总节点: {len(results)}
- 成功: {success_count}
- 失败: {fail_count}

详细结果:
"""

    for r in results:
        status = "成功" if r.success else "失败"
        body += f"- {r.node_name} ({r.host}): {status}, 耗时 {r.duration:.2f}s\n"
        if not r.success:
            body += f"  错误: {r.error or r.stderr[:100]}\n"

    if notify_type == 'email':
        return send_email_notification(target, subject, body)
    elif notify_type == 'webhook':
        return send_webhook_notification(target, {
            "subject": subject,
            "body": body,
            "success_count": success_count,
            "fail_count": fail_count,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        })

    return False


def send_alert(alert_type: str, target: str, results: list[ExecutionResult],
               command: str = None, logger: logging.Logger = None):
    """发送异常告警"""
    logger = logger or logging.getLogger("batch_exec")

    failed_nodes = [r for r in results if not r.success]
    if not failed_nodes:
        return True

    message = f"""【告警】批量执行失败

执行命令: {command or 'N/A'}
执行时间: {time.strftime('%Y-%m-%d %H:%M:%S')}
失败节点数: {len(failed_nodes)}

失败节点:
"""

    for r in failed_nodes:
        message += f"- {r.node_name} ({r.host}): {r.error or r.stderr[:50]}\n"

    if alert_type == 'dingtalk':
        return send_dingtalk_alert(target, message)
    elif alert_type == 'wechat':
        return send_wechat_alert(target, message)

    return False


# ============== 预检查和后验证 ==============
def run_pre_check(node: NodeConfig, checks: list[str], timeout: int,
                   use_pool: bool = False, retry_times: int = 3,
                   retry_delay: float = 1.0, logger: logging.Logger = None) -> dict:
    """运行预检查"""
    logger = logger or logging.getLogger("batch_exec")
    results = {}

    try:
        pool = None
        if use_pool:
            global _GLOBAL_POOL
            pool = _GLOBAL_POOL
            if pool is None:
                pool = ConnectionPool(max_connections=10, logger=logger)

        wrapper = SSHClientWrapper(node, timeout, pool, use_pool, logger)
        wrapper.connect_with_retry(retry_times, retry_delay)

        for check in checks:
            check_result = {"passed": False, "output": "", "error": None}

            try:
                if check.startswith("disk:"):
                    # 检查磁盘空间
                    threshold = int(check.split(':')[1])
                    exit_code, stdout, stderr = wrapper.execute(f"df / | tail -1 | awk '{{print $5}}' | cut -d'%' -f1")
                    if exit_code == 0:
                        usage = int(stdout.strip())
                        check_result["passed"] = usage < threshold
                        check_result["output"] = f"磁盘使用率: {usage}%"

                elif check.startswith("memory:"):
                    # 检查内存
                    threshold = int(check.split(':')[1])
                    exit_code, stdout, stderr = wrapper.execute("free | grep Mem | awk '{{print $3/$2 * 100.0}}'")
                    if exit_code == 0:
                        usage = float(stdout.strip())
                        check_result["passed"] = usage < threshold
                        check_result["output"] = f"内存使用率: {usage:.1f}%"

                elif check.startswith("process:"):
                    # 检查进程是否存在
                    process_name = check.split(':')[1]
                    exit_code, stdout, stderr = wrapper.execute(f"pgrep -x {process_name}")
                    check_result["passed"] = exit_code == 0
                    check_result["output"] = f"进程 {process_name}: {'存在' if exit_code == 0 else '不存在'}"

                elif check.startswith("file:"):
                    # 检查文件是否存在
                    file_path = check.split(':')[1]
                    exit_code, stdout, stderr = wrapper.execute(f"test -f {file_path}")
                    check_result["passed"] = exit_code == 0
                    check_result["output"] = f"文件 {file_path}: {'存在' if exit_code == 0 else '不存在'}"

                elif check.startswith("port:"):
                    # 检查端口是否监听
                    port = int(check.split(':')[1])
                    exit_code, stdout, stderr = wrapper.execute(f"ss -tln | grep :{port}")
                    check_result["passed"] = exit_code == 0
                    check_result["output"] = f"端口 {port}: {'监听' if exit_code == 0 else '未监听'}"

                else:
                    # 自定义命令检查
                    exit_code, stdout, stderr = wrapper.execute(check)
                    check_result["passed"] = exit_code == 0
                    check_result["output"] = stdout[:200]

            except Exception as e:
                check_result["error"] = str(e)

            results[check] = check_result

        wrapper.close(keep_in_pool=use_pool)

    except Exception as e:
        logger.error(f"节点 {node.name} 预检查连接失败: {e}")
        for check in checks:
            results[check] = {"passed": False, "output": "", "error": str(e)}

    return results


def run_post_verify(node: NodeConfig, verifies: list[str], timeout: int,
                    use_pool: bool = False, retry_times: int = 3,
                    retry_delay: float = 1.0, logger: logging.Logger = None) -> dict:
    """运行后验证"""
    logger = logger or logging.getLogger("batch_exec")
    results = {}

    try:
        pool = None
        if use_pool:
            global _GLOBAL_POOL
            pool = _GLOBAL_POOL
            if pool is None:
                pool = ConnectionPool(max_connections=10, logger=logger)

        wrapper = SSHClientWrapper(node, timeout, pool, use_pool, logger)
        wrapper.connect_with_retry(retry_times, retry_delay)

        for verify in verifies:
            verify_result = {"passed": False, "output": "", "error": None}

            try:
                if verify.startswith("service:"):
                    # 验证服务状态
                    service_name = verify.split(':')[1]
                    status = wrapper.service_status(service_name)
                    verify_result["passed"] = status.is_running
                    verify_result["output"] = f"服务 {service_name}: {'运行' if status.is_running else '停止'}"

                elif verify.startswith("file:"):
                    # 验证文件存在
                    file_path = verify.split(':')[1]
                    exit_code, stdout, stderr = wrapper.execute(f"test -f {file_path}")
                    verify_result["passed"] = exit_code == 0
                    verify_result["output"] = f"文件 {file_path}: {'存在' if exit_code == 0 else '不存在'}"

                elif verify.startswith("file_content:"):
                    # 验证文件内容
                    parts = verify.split(':', 2)
                    file_path = parts[1]
                    expected_content = parts[2]
                    exit_code, stdout, stderr = wrapper.execute(f"cat {file_path}")
                    verify_result["passed"] = expected_content in stdout
                    verify_result["output"] = f"文件内容验证"

                elif verify.startswith("port:"):
                    # 验证端口监听
                    port = int(verify.split(':')[1])
                    exit_code, stdout, stderr = wrapper.execute(f"ss -tln | grep :{port}")
                    verify_result["passed"] = exit_code == 0
                    verify_result["output"] = f"端口 {port}: {'监听' if exit_code == 0 else '未监听'}"

                elif verify.startswith("http:"):
                    # 验证HTTP响应
                    url = verify.split(':')[1]
                    exit_code, stdout, stderr = wrapper.execute(f"curl -s -o /dev/null -w '%{{http_code}}' {url}")
                    if exit_code == 0:
                        code = stdout.strip()
                        verify_result["passed"] = code == "200"
                        verify_result["output"] = f"HTTP状态码: {code}"

                else:
                    # 自定义命令验证
                    exit_code, stdout, stderr = wrapper.execute(verify)
                    verify_result["passed"] = exit_code == 0
                    verify_result["output"] = stdout[:200]

            except Exception as e:
                verify_result["error"] = str(e)

            results[verify] = verify_result

        wrapper.close(keep_in_pool=use_pool)

    except Exception as e:
        logger.error(f"节点 {node.name} 后验证连接失败: {e}")
        for verify in verifies:
            results[verify] = {"passed": False, "output": "", "error": str(e)}

    return results


# ============== 交互式模式 ==============
def interactive_mode(nodes: list[NodeConfig], settings: Settings, logger: logging.Logger):
    """交互式shell模式"""
    import select

    logger.info("进入交互式模式 (输入 'exit' 或 'quit' 退出)")
    logger.info("可用节点:")
    for i, node in enumerate(nodes):
        logger.info(f"  [{i}] {node.name} ({node.host})")

    print("\n请选择节点 (输入编号，多个用逗号分隔，或 'all' 或 'q' 退出): ", end='')

    selected_indices = []
    while True:
        choice = input().strip().lower()
        if choice in ('q', 'quit', 'exit'):
            return
        if choice == 'all':
            selected_indices = list(range(len(nodes)))
            break
        try:
            selected_indices = [int(x.strip()) for x in choice.split(',') if x.strip()]
            if all(0 <= i < len(nodes) for i in selected_indices):
                break
            logger.error("无效的节点编号")
        except ValueError:
            logger.error("请输入有效的编号")

    selected_nodes = [nodes[i] for i in selected_indices]
    logger.info(f"已选择 {len(selected_nodes)} 个节点")

    # 使用第一个节点的配置创建SSH客户端
    # 注意：交互模式下每个节点独立
    while True:
        print("\n$ ", end='')
        try:
            cmd = input().strip()
        except EOFError:
            break

        if cmd.lower() in ('exit', 'quit', 'q'):
            break
        if not cmd:
            continue

        logger.info(f"在 {len(selected_nodes)} 个节点上执行: {cmd}")

        results = []
        for node in selected_nodes:
            result = execute_on_node(node, cmd, None, settings.timeout)
            results.append(result)
            print_result(result, True, logger)

        # 统计
        success = sum(1 for r in results if r.success)
        logger.info(f"完成: {success}/{len(results)} 成功")


# ============== 进度条 ==============
class ProgressBar:
    """简单的进度条"""

    def __init__(self, total: int, desc: str = "进度"):
        self.total = total
        self.current = 0
        self.desc = desc

    def update(self, n: int = 1):
        self.current += n
        percent = self.current / self.total * 100
        bar_len = 30
        filled = int(bar_len * self.current / self.total)
        bar = '=' * filled + '-' * (bar_len - filled)
        print(f'\r{self.desc}: [{bar}] {self.current}/{self.total} ({percent:.1f}%)', end='', flush=True)

    def close(self):
        print()


# ============== 主程序 ==============
def main():
    parser = argparse.ArgumentParser(
        description='批量远程命令执行工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s -c nodes.yaml -x "uptime"
  %(prog)s -c nodes.yaml -s ./deploy.sh
  %(prog)s -c nodes.yaml -x "df -h" --node web-server-1
  %(prog)s -c nodes.yaml -x "apt update" --parallel 10
  %(prog)s -c nodes.yaml -x "uptime" --export-json results.json
  %(prog)s -c nodes.yaml --interactive
        """
    )

    parser.add_argument('-c', '--config', help='配置文件路径 (YAML格式)')
    parser.add_argument('-x', '--execute', dest='command', help='要执行的命令')
    parser.add_argument('-s', '--script', help='要执行的本地脚本文件')
    parser.add_argument('--node', dest='node_filter', help='只执行指定节点 (支持逗号分隔多个节点)')
    parser.add_argument('--tags', help='按标签过滤节点 (支持逗号分隔多个标签)')
    parser.add_argument('--timeout', type=int, help='SSH连接超时时间 (默认: 30秒)')
    parser.add_argument('--parallel', type=int, help='并行执行数 (默认: 5)')
    parser.add_argument('--no-parallel', action='store_true', help='禁用并行执行')
    parser.add_argument('-v', '--verbose', action='store_true', help='显示详细输出')
    parser.add_argument('--interactive', action='store_true', help='交互式shell模式')
    parser.add_argument('--export-json', dest='export_json', help='导出结果为JSON文件')
    parser.add_argument('--export-csv', dest='export_csv', help='导出结果为CSV文件')
    parser.add_argument('--export-html', dest='export_html', help='导出结果为HTML文件')
    parser.add_argument('--compare', action='store_true', help='比对多节点输出')
    parser.add_argument('--web-dashboard', action='store_true', help='启动Web仪表盘')

    # 通知告警参数
    parser.add_argument('--notify', action='append', help='执行通知 (格式: email:地址 或 webhook:URL)')
    parser.add_argument('--alert', action='append', help='异常告警 (格式: dingtalk:token 或 wechat:key)')

    # 预检查和后验证参数
    parser.add_argument('--pre-check', action='append', help='执行前检查 (如: disk:80, memory:90, process:nginx)')
    parser.add_argument('--post-verify', action='append', help='执行后验证 (如: service:nginx, file:/tmp/test, port:80)')
    parser.add_argument('--use-pool', action='store_true', help='启用连接池复用')
    parser.add_argument('--retry', type=int, default=3, help='重试次数 (默认: 3)')

    # 文件传输参数
    parser.add_argument('--upload', help='上传文件: local_path:remote_path')
    parser.add_argument('--download', help='下载文件: remote_path:local_dir')
    parser.add_argument('--sync', help='同步目录: local_dir:remote_dir')

    # 运维监控参数
    parser.add_argument('--health-check', action='store_true', help='节点健康检查')
    parser.add_argument('--tail', help='实时查看远程日志: /path/to/log')
    parser.add_argument('--tail-lines', type=int, default=20, help='日志行数 (默认: 20)')
    parser.add_argument('--service-status', help='检查服务状态 (逗号分隔多个服务)')

    # 高级执行参数
    parser.add_argument('--template', help='命令模板 (支持变量替换)')
    parser.add_argument('--var', action='append', help='模板变量 (格式: key=value)')
    parser.add_argument('--history-file', help='保存执行历史到JSON文件')
    parser.add_argument('--condition', help='条件执行 (如: success_count >= 50%%)')

    # 安全参数
    parser.add_argument('--encrypt-config', help='加密配置文件中的密码')
    parser.add_argument('--decrypt-config', help='解密配置文件')
    parser.add_argument('--ssh-agent-forwarding', action='store_true', help='启用SSH Agent转发')
    parser.add_argument('--verify-host-key', action='store_true', help='验证主机密钥指纹')
    parser.add_argument('--auto-accept-host', action='store_true', help='自动接受新主机密钥')

    # 调度与自动化参数
    parser.add_argument('--schedule', help='定时执行 (cron表达式，如: "0 2 * * *" 表示每天凌晨2点)')
    parser.add_argument('--schedule-once', action='store_true', help='定时任务只执行一次后退出')
    parser.add_argument('--schedule-log', help='定时任务日志文件路径')
    parser.add_argument('--workflow', help='任务编排 (YAML文件路径或 "task1->task2->task3" 格式)')
    parser.add_argument('--workflow-visualize', action='store_true', help='可视化任务依赖图')
    parser.add_argument('--patrol', action='store_true', help='启用定期巡检模式')
    parser.add_argument('--interval', type=int, default=3600, help='巡检间隔秒数 (默认: 3600)')
    parser.add_argument('--patrol-checks', action='append', help='巡检项目 (如: cpu, memory, disk, process:nginx)')
    parser.add_argument('--patrol-report', help='巡检报告输出路径')
    parser.add_argument('--patrol-alert', help='巡检告警配置 (格式: dingtalk:token 或 wechat:key)')
    parser.add_argument('--retry-failed', action='store_true', help='启用失败节点重试')
    parser.add_argument('--max-retry', type=int, default=5, help='最大重试次数 (默认: 5)')
    parser.add_argument('--retry-delay', type=int, default=60, help='重试间隔秒数 (默认: 60)')
    parser.add_argument('--retry-backoff', action='store_true', help='启用指数退避重试')
    parser.add_argument('--retry-log', help='重试状态日志文件路径')
    parser.add_argument('--retry-clear', action='store_true', help='清除重试状态')

    # 多节点协同执行参数
    parser.add_argument('--master', help='主从模式: 指定主节点名称')
    parser.add_argument('--slaves', help='主从模式: 指定从节点名称 (逗号分隔)')
    parser.add_argument('--batch-size', type=int, default=5, help='分批次执行: 每批次节点数 (默认: 5)')
    parser.add_argument('--batch-delay', type=int, default=10, help='分批次执行: 批次间隔秒数 (默认: 10)')
    parser.add_argument('--loop', action='store_true', help='启用轮询执行模式')
    parser.add_argument('--until', help='轮询终止条件 (如: all_success, success_count >= 3)')
    parser.add_argument('--max-loops', type=int, default=100, help='轮询最大循环次数 (默认: 100)')
    parser.add_argument('--loop-interval', type=int, default=5, help='轮询间隔秒数 (默认: 5)')
    parser.add_argument('--fallback', help='故障转移: 备用节点名称 (逗号分隔)')

    # 增强监控与采集参数
    parser.add_argument('--collect', action='store_true', help='启用实时指标采集模式')
    parser.add_argument('--metrics', action='append', help='采集指标类型 (如: cpu,memory,disk,net,load)')
    parser.add_argument('--duration', type=int, default=60, help='采集时长秒数 (默认: 60)')
    parser.add_argument('--collect-interval', type=int, default=5, help='采集间隔秒数 (默认: 5)')
    parser.add_argument('--prometheus-output', dest='prometheus_output', help='输出Prometheus格式指标文件')
    parser.add_argument('--baseline', help='基准对比文件路径')
    parser.add_argument('--save-baseline', dest='save_baseline', help='保存当前数据为新基准')
    parser.add_argument('--anomaly-detect', action='store_true', help='启用自动异常检测')
    parser.add_argument('--threshold', type=float, default=20.0, help='异常检测阈值百分比 (默认: 20)')
    parser.add_argument('--anomaly-report', dest='anomaly_report', help='异常报告输出路径')

    # 数据分析与报表参数
    parser.add_argument('--analyze-log', dest='analyze_log', help='解析分析日志文件路径')
    parser.add_argument('--analyze-output', dest='analyze_output', help='日志分析报告输出路径')
    parser.add_argument('--perf-report', action='store_true', help='生成性能报表')
    parser.add_argument('--perf-range', type=str, default='7d', help='性能报表时间范围 (如: 7d, 30d)')
    parser.add_argument('--perf-output', dest='perf_output', help='性能报表输出路径 (HTML)')
    parser.add_argument('--stats', action='store_true', help='执行统计分析')
    parser.add_argument('--by-day', action='store_true', help='按日期统计')
    parser.add_argument('--by-node', action='store_true', help='按节点统计')
    parser.add_argument('--stats-days', type=int, default=7, help='统计天数 (默认: 7)')
    parser.add_argument('--stats-output', dest='stats_output', help='统计报告输出路径')
    parser.add_argument('--predict', action='store_true', help='启用AI异常预测')
    parser.add_argument('--predict-model', dest='predict_model', help='预测模型文件路径')
    parser.add_argument('--predict-output', dest='predict_output', help='预测结果输出路径')
    parser.add_argument('--data-dir', dest='data_dir', default='/tmp', help='数据文件目录 (默认: /tmp)')

    # 用户体验增强参数
    parser.add_argument('--tui', action='store_true', help='启用TUI终端图形界面')
    parser.add_argument('--progress-chart', action='store_true', help='启用实时进度图表可视化')
    parser.add_argument('--side-by-side', action='store_true', help='启用结果并排对比视图')
    parser.add_argument('--search-history', dest='search_history', help='搜索命令历史 (关键词)')
    parser.add_argument('--search-field', dest='search_field', default='command', help='搜索字段 (默认: command)')
    parser.add_argument('--search-date', dest='search_date', help='按日期搜索历史')
    parser.add_argument('--search-status', dest='search_status', choices=['success', 'fail'], help='按状态搜索历史')
    parser.add_argument('--history-limit', type=int, default=20, help='历史搜索显示数量限制 (默认: 20)')

    args = parser.parse_args()

    global _GLOBAL_POOL

    # 处理配置文件加密/解密
    if args.encrypt_config:
        if not HAS_CRYPTO:
            print("错误: 需要安装 cryptography 库")
            sys.exit(1)

        config_path = os.path.expanduser(args.encrypt_config)
        if not os.path.exists(config_path):
            print(f"错误: 配置文件不存在: {config_path}")
            sys.exit(1)

        key = get_or_create_key()
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.load(f, Loader=SafeLoader)

        # 加密密码字段
        for node in config.get('nodes', []):
            if node.get('password'):
                node['encrypted_password'] = encrypt_password(node['password'], key)
                node['password'] = None

        # 写入加密后的配置
        output_path = config_path + '.encrypted'
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f)

        print(f"配置已加密保存到: {output_path}")
        print(f"加密密钥保存在: ~/.batch_exec_key")
        print("请妥善保管密钥文件!")
        sys.exit(0)

    if args.decrypt_config:
        if not HAS_CRYPTO:
            print("错误: 需要安装 cryptography 库")
            sys.exit(1)

        config_path = os.path.expanduser(args.decrypt_config)
        if not os.path.exists(config_path):
            print(f"错误: 配置文件不存在: {config_path}")
            sys.exit(1)

        key = get_or_create_key()
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.load(f, Loader=SafeLoader)

        # 解密密码字段
        for node in config.get('nodes', []):
            if node.get('encrypted_password'):
                try:
                    node['password'] = decrypt_password(node['encrypted_password'], key)
                    node['encrypted_password'] = None
                except Exception as e:
                    print(f"错误: 解密失败，可能是密钥不正确: {e}")
                    sys.exit(1)

        # 写入解密后的配置
        output_path = config_path.replace('.encrypted', '.decrypted')
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f)

        print(f"配置已解密保存到: {output_path}")
        sys.exit(0)

    # ============== 调度与自动化模式处理 ==============
    # 定时执行模式
    if args.schedule:
        if not HAS_CRONITER:
            print("错误: 需要安装 croniter 库: pip install croniter")
            sys.exit(1)

        if not args.config:
            parser.error("定时执行模式需要指定配置文件 (-c)")

        if not args.command and not args.script:
            parser.error("定时执行模式需要指定命令 (-x) 或脚本 (-s)")

        logger = setup_logging(args.verbose)

        # 定时任务日志文件
        if args.schedule_log:
            schedule_log_path = os.path.expanduser(args.schedule_log)
            file_handler = logging.FileHandler(schedule_log_path)
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            logger.addHandler(file_handler)

        try:
            nodes, settings = load_config(args.config, logger)

            # 节点过滤
            if args.node_filter:
                filter_names = set(n.strip() for n in args.node_filter.split(','))
                nodes = [n for n in nodes if n.name in filter_names]

            if args.tags:
                filter_tags = set(t.strip() for t in args.tags.split(','))
                nodes = [n for n in nodes if any(tag in filter_tags for tag in n.tags)]

            # 定义执行任务
            def scheduled_task():
                results = []
                use_parallel = not args.no_parallel
                max_workers = args.parallel or settings.max_workers

                if use_parallel and len(nodes) > 1:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {
                            executor.submit(
                                execute_on_node, node, args.command, args.script,
                                settings.timeout, args.use_pool,
                                args.retry, settings.retry_delay, logger
                            ): node for node in nodes
                        }
                        for future in as_completed(futures):
                            results.append(future.result())
                else:
                    for node in nodes:
                        result = execute_on_node(
                            node, args.command, args.script,
                            settings.timeout, args.use_pool,
                            args.retry, settings.retry_delay, logger
                        )
                        results.append(result)

                success_count = sum(1 for r in results if r.success)
                logger.info(f"执行完成: {success_count}/{len(results)} 成功")

                # 导出结果
                if args.export_json:
                    export_results_json(results, args.export_json)

                # 发送通知
                if args.notify:
                    for notify in args.notify:
                        notify_type, target = notify.split(':', 1)
                        send_notification(notify_type, target, results, args.command, logger)

                return results

            # 启动调度器
            scheduler = CronScheduler(args.schedule, logger)

            def signal_handler(sig, frame):
                logger.info("收到终止信号，停止调度器")
                scheduler.stop()
                sys.exit(0)

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

            scheduler.start(scheduled_task, once=args.schedule_once)

        except Exception as e:
            logger.error(f"定时执行错误: {e}")
            sys.exit(1)

        return

    # 工作流编排模式
    if args.workflow:
        if not args.config:
            parser.error("工作流模式需要指定配置文件 (-c)")

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            executor = WorkflowExecutor(args.workflow, nodes, settings, logger)

            # 可视化模式
            if args.workflow_visualize:
                print(executor.visualize())
                sys.exit(0)

            # 执行工作流
            success = executor.run()

            # 导出结果
            if args.export_json:
                all_results = []
                for task_name, results in executor.task_results.items():
                    all_results.extend(results)
                export_results_json(all_results, args.export_json)

            sys.exit(0 if success else 1)

        except Exception as e:
            logger.error(f"工作流执行错误: {e}")
            sys.exit(1)

        return

    # 定期巡检模式
    if args.patrol:
        if not args.config:
            parser.error("巡检模式需要指定配置文件 (-c)")

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            # 解析告警配置
            alert_config = None
            if args.patrol_alert:
                alert_type, target = args.patrol_alert.split(':', 1)
                alert_config = {'type': alert_type, 'target': target}

            patrol_runner = PatrolRunner(
                nodes, settings,
                interval=args.interval,
                checks=args.patrol_checks,
                report_path=args.patrol_report,
                alert_config=alert_config,
                logger=logger
            )

            def signal_handler(sig, frame):
                logger.info("收到终止信号，停止巡检")
                patrol_runner.stop()
                sys.exit(0)

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

            patrol_runner.start(once=args.schedule_once)

        except Exception as e:
            logger.error(f"巡检错误: {e}")
            sys.exit(1)

        return

    # 失败重试模式
    if args.retry_failed:
        if not args.config:
            parser.error("重试模式需要指定配置文件 (-c)")

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            retry_manager = RetryManager(
                max_retry=args.max_retry,
                retry_delay=args.retry_delay,
                backoff=args.retry_backoff,
                log_file=args.retry_log,
                logger=logger
            )

            # 清除状态
            if args.retry_clear:
                retry_manager.clear_status()
                logger.info("重试状态已清除")
                sys.exit(0)

            # 执行重试
            results = retry_manager.execute_retries(
                nodes, args.command, args.script, settings
            )

            # 导出结果
            if args.export_json:
                export_results_json(results, args.export_json)

            success_count = sum(1 for r in results if r.success)
            sys.exit(0 if success_count == len(results) else 1)

        except Exception as e:
            logger.error(f"重试执行错误: {e}")
            sys.exit(1)

        return

    # ============== 多节点协同执行模式处理 ==============
    # 主从模式执行
    if args.master and args.slaves:
        if not args.config:
            parser.error("主从模式需要指定配置文件 (-c)")

        if not args.command and not args.script:
            parser.error("主从模式需要指定命令 (-x) 或脚本 (-s)")

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            # 查找主节点
            master_node = None
            for node in nodes:
                if node.name == args.master:
                    master_node = node
                    break

            if not master_node:
                logger.error(f"未找到主节点: {args.master}")
                sys.exit(1)

            # 查找从节点
            slave_names = set(n.strip() for n in args.slaves.split(','))
            slave_nodes = [n for n in nodes if n.name in slave_names]

            if not slave_nodes:
                logger.error(f"未找到从节点: {args.slaves}")
                sys.exit(1)

            executor = MasterSlaveExecutor(master_node, slave_nodes, settings, logger)
            success = executor.execute(args.command, args.script)

            # 导出结果
            if args.export_json:
                export_results_json(executor.results, args.export_json)

            sys.exit(0 if success else 1)

        except Exception as e:
            logger.error(f"主从模式执行错误: {e}")
            sys.exit(1)

        return

    # 分批次执行模式
    if args.batch_size and args.batch_size != 5:  # 用户指定了批次大小
        if not args.config:
            parser.error("分批次执行需要指定配置文件 (-c)")

        if not args.command and not args.script:
            parser.error("分批次执行需要指定命令 (-x) 或脚本 (-s)")

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            executor = BatchExecutor(
                nodes, args.batch_size, args.batch_delay,
                settings, logger
            )
            success = executor.execute(args.command, args.script)

            # 导出结果
            if args.export_json:
                export_results_json(executor.results, args.export_json)

            sys.exit(0 if success else 1)

        except Exception as e:
            logger.error(f"分批次执行错误: {e}")
            sys.exit(1)

        return

    # 轮询执行模式
    if args.loop:
        if not args.config:
            parser.error("轮询执行需要指定配置文件 (-c)")

        if not args.command and not args.script:
            parser.error("轮询执行需要指定命令 (-x) 或脚本 (-s)")

        if not args.until:
            logger.warning("未指定轮询终止条件 (--until)，将执行最大循环次数")
            args.until = f"max_loops == {args.max_loops}"

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            executor = LoopExecutor(
                nodes, args.until, args.max_loops, args.loop_interval,
                settings, logger
            )
            success = executor.execute(args.command, args.script)

            # 导出结果
            if args.export_json:
                export_results_json(executor.results, args.export_json)

            sys.exit(0 if success else 1)

        except Exception as e:
            logger.error(f"轮询执行错误: {e}")
            sys.exit(1)

        return

    # 故障转移模式
    if args.fallback:
        if not args.config:
            parser.error("故障转移模式需要指定配置文件 (-c)")

        if not args.command and not args.script:
            parser.error("故障转移模式需要指定命令 (-x) 或脚本 (-s)")

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            # 使用第一个节点作为主节点
            primary_node = nodes[0]

            # 查找备用节点
            fallback_names = set(n.strip() for n in args.fallback.split(','))
            fallback_nodes = [n for n in nodes if n.name in fallback_names]

            if not fallback_nodes:
                logger.error(f"未找到备用节点: {args.fallback}")
                sys.exit(1)

            executor = FallbackExecutor(primary_node, fallback_nodes, settings, logger)
            success = executor.execute(args.command, args.script)

            # 导出结果
            if args.export_json:
                export_results_json(executor.results, args.export_json)

            if success:
                logger.info(f"成功节点: {executor.get_success_node().name}")

            sys.exit(0 if success else 1)

        except Exception as e:
            logger.error(f"故障转移执行错误: {e}")
            sys.exit(1)

        return

    # ============== 增强监控与采集模式处理 ==============
    # 实时指标采集模式
    if args.collect:
        if not args.config:
            parser.error("指标采集需要指定配置文件 (-c)")

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            # 节点过滤
            if args.node_filter:
                filter_names = set(n.strip() for n in args.node_filter.split(','))
                nodes = [n for n in nodes if n.name in filter_names]

            if args.tags:
                filter_tags = set(t.strip() for t in args.tags.split(','))
                nodes = [n for n in nodes if any(tag in filter_tags for tag in n.tags)]

            collector = MetricsCollector(
                nodes,
                args.metrics or ['cpu', 'memory', 'disk', 'net'],
                args.duration,
                args.collect_interval,
                settings,
                logger
            )

            samples = collector.collect()

            # 保存采集数据
            output_path = args.export_json or '/tmp/metrics_collection.json'
            collector.save_to_json(output_path)

            # Prometheus输出
            if args.prometheus_output:
                exporter = PrometheusExporter(samples, logger)
                exporter.export(args.prometheus_output)

            # 基准对比
            if args.baseline:
                comparator = BaselineComparator(args.baseline, samples, logger)
                comparison_results = comparator.compare()

                if args.export_json:
                    comparator.save_comparison(args.export_json.replace('.json', '_comparison.json'))

            # 保存为基准
            if args.save_baseline:
                comparator = BaselineComparator('', samples, logger)
                comparator.save_as_baseline(args.save_baseline)

            # 异常检测
            if args.anomaly_detect:
                detector = AnomalyDetector(args.baseline or args.save_baseline, args.threshold, samples, logger)
                anomalies = detector.detect()

                if args.anomaly_report:
                    detector.save_report(args.anomaly_report)

                # 如果有严重异常，退出码为非零
                if detector.has_critical_anomaly():
                    logger.warning("检测到严重异常，退出码为1")
                    sys.exit(1)

            sys.exit(0)

        except Exception as e:
            logger.error(f"指标采集错误: {e}")
            sys.exit(1)

        return

    # 异常检测模式（单独运行）
    if args.anomaly_detect and not args.collect:
        if not args.config:
            parser.error("异常检测需要指定配置文件 (-c)")

        if not args.baseline:
            parser.error("异常检测需要指定基准文件 (--baseline)")

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            # 节点过滤
            if args.node_filter:
                filter_names = set(n.strip() for n in args.node_filter.split(','))
                nodes = [n for n in nodes if n.name in filter_names]

            # 快速采集一次作为当前数据
            collector = MetricsCollector(
                nodes,
                ['cpu', 'memory', 'disk'],
                10,  # 快速采集10秒
                5,
                settings,
                logger
            )

            samples = collector.collect()

            # 异常检测
            detector = AnomalyDetector(args.baseline, args.threshold, samples, logger)
            anomalies = detector.detect()

            if args.anomaly_report:
                detector.save_report(args.anomaly_report)

            if detector.has_critical_anomaly():
                sys.exit(1)

            sys.exit(0)

        except Exception as e:
            logger.error(f"异常检测错误: {e}")
            sys.exit(1)

        return

    # Prometheus导出模式（使用现有数据）
    if args.prometheus_output and not args.collect:
        # 从历史数据文件生成Prometheus指标
        history_file = args.history_file or args.export_json

        if history_file and os.path.exists(history_file):
            logger = setup_logging(args.verbose)

            try:
                with open(history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # 构建MetricsSample列表
                samples = []
                if 'samples' in data:
                    for item in data['samples']:
                        sample = MetricsSample(
                            timestamp=item['timestamp'],
                            node_name=item['node_name'],
                            host=item['host'],
                            cpu_usage=item.get('cpu_usage'),
                            memory_usage=item.get('memory_usage'),
                            disk_usage=item.get('disk_usage'),
                            network_in=item.get('network_in'),
                            network_out=item.get('network_out'),
                            load_avg=item.get('load_avg'),
                            process_count=item.get('process_count')
                        )
                        samples.append(sample)

                exporter = PrometheusExporter(samples, logger)
                exporter.export(args.prometheus_output)
                sys.exit(0)

            except Exception as e:
                logger.error(f"Prometheus导出错误: {e}")
                sys.exit(1)
        else:
            logger.error(f"需要提供历史数据文件 (--history-file 或 --export-json)")
            sys.exit(1)

        return

    # ============== 数据分析与报表模式处理 ==============
    # 日志解析模式
    if args.analyze_log:
        logger = setup_logging(args.verbose)

        try:
            analyzer = LogAnalyzer(args.analyze_log, logger=logger)
            entries = analyzer.parse()

            if entries:
                analyzer.analyze()

                if args.analyze_output:
                    analyzer.save_report(args.analyze_output)

            sys.exit(0)

        except Exception as e:
            logger.error(f"日志解析错误: {e}")
            sys.exit(1)

        return

    # 统计分析模式
    if args.stats:
        logger = setup_logging(args.verbose)

        try:
            # 查找历史文件
            data_dir = args.data_dir
            history_files = []

            # 自动发现历史文件
            for filename in ['exec_history.json', 'history.json', 'metrics_collection.json']:
                filepath = os.path.join(data_dir, filename)
                if os.path.exists(filepath):
                    history_files.append(filepath)

            # 添加用户指定的历史文件
            if args.history_file:
                history_files.append(args.history_file)

            if not history_files:
                logger.warning("未找到历史数据文件，将使用默认路径")
                # 尝试从其他位置查找
                default_paths = ['/tmp/exec_history.json', '/tmp/history.json',
                                '/tmp/metrics_collection.json']
                for p in default_paths:
                    if os.path.exists(p):
                        history_files.append(p)

            analyzer = StatisticsAnalyzer(history_files, logger)

            if analyzer.load_data():
                analyzer.print_summary(args.by_day, args.by_node)

                if args.stats_output:
                    analyzer.save_report(args.stats_output, args.by_day, args.by_node)

            sys.exit(0)

        except Exception as e:
            logger.error(f"统计分析错误: {e}")
            sys.exit(1)

        return

    # 性能报表模式
    if args.perf_report:
        logger = setup_logging(args.verbose)

        try:
            # 解析时间范围
            range_days = int(args.perf_range.replace('d', '').replace('D', ''))

            # 查找指标文件
            data_dir = args.data_dir
            metrics_files = []

            for filename in ['metrics_collection.json', 'final_metrics.json',
                            'patrol_report.json']:
                filepath = os.path.join(data_dir, filename)
                if os.path.exists(filepath):
                    metrics_files.append(filepath)

            if args.export_json and os.path.exists(args.export_json):
                metrics_files.append(args.export_json)

            if not metrics_files:
                logger.warning("未找到指标数据文件")

            reporter = PerformanceReporter(metrics_files, logger)

            if reporter.load_metrics():
                reporter.calculate_trends(range_days)

                output_path = args.perf_output or '/tmp/performance_report.html'
                reporter.generate_html_report(output_path)

                # 也生成JSON报表
                json_output = output_path.replace('.html', '.json')
                reporter.generate_json_report(json_output)

                logger.info(f"性能报表已生成: {output_path}")

            sys.exit(0)

        except Exception as e:
            logger.error(f"性能报表生成错误: {e}")
            sys.exit(1)

        return

    # AI异常预测模式
    if args.predict:
        logger = setup_logging(args.verbose)

        try:
            # 加载历史数据
            data_dir = args.data_dir
            history_data = []

            # 从基准文件加载
            if args.baseline and os.path.exists(args.baseline):
                with open(args.baseline, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'baseline' in data:
                        history_data.extend(data['baseline'])

            # 从指标文件加载
            metrics_files = ['metrics_collection.json', 'patrol_report.json',
                            'final_metrics.json']
            for filename in metrics_files:
                filepath = os.path.join(data_dir, filename)
                if os.path.exists(filepath):
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if 'samples' in data:
                            history_data.extend(data['samples'])

            if not history_data:
                logger.warning("无历史数据用于预测")
                sys.exit(0)

            predictor = AnomalyPredictor(history_data, args.predict_model, logger)
            predictions = predictor.predict()

            if args.predict_output:
                predictor.save_predictions(args.predict_output)

            # 如果有高风险预测，退出码为非零
            if predictor.has_high_risk_prediction():
                logger.warning("检测到高风险预测，退出码为1")
                sys.exit(1)

            sys.exit(0)

        except Exception as e:
            logger.error(f"异常预测错误: {e}")
            sys.exit(1)

        return

    # ============== 用户体验增强模式处理 ==============
    # TUI终端图形界面模式
    if args.tui:
        if not args.config:
            parser.error("TUI模式需要指定配置文件 (-c)")

        if not HAS_RICH:
            print("错误: 需要安装 rich 库: pip install rich")
            sys.exit(1)

        logger = setup_logging(args.verbose)

        try:
            nodes, settings = load_config(args.config, logger)

            tui = TUIInterface(nodes, settings, logger)

            # 如果有命令或脚本，直接执行
            if args.command or args.script:
                tui.run_with_tui(command=args.command, script=args.script)
                sys.exit(0)
            else:
                # 交互式菜单
                tui.interactive_menu()
                sys.exit(0)

        except Exception as e:
            logger.error(f"TUI执行错误: {e}")
            sys.exit(1)

        return

    # 命令历史搜索模式
    if args.search_history or args.search_date or args.search_status:
        logger = setup_logging(args.verbose)

        try:
            history_file = args.history_file or "/tmp/exec_history.json"
            searcher = HistorySearcher(history_file, logger)

            results = []
            if args.search_history:
                results = searcher.search(args.search_history, args.search_field)
            elif args.search_date:
                results = searcher.search_by_date(args.search_date)
            elif args.search_status == 'success':
                results = searcher.search_by_status(success_only=True)
            elif args.search_status == 'fail':
                results = searcher.search_by_status(success_only=False)

            # 限制显示数量
            results = results[:args.history_limit]
            searcher.display_results(results)

            sys.exit(0)

        except Exception as e:
            logger.error(f"历史搜索错误: {e}")
            sys.exit(1)

        return

    # 交互式模式不需要命令
    if args.interactive:
        if not args.config:
            parser.error("交互式模式需要指定配置文件 (-c)")

        logger = setup_logging(True)

        try:
            # 加载配置（不展开环境变量，因为在交互模式下可能需要手动设置）
            with open(os.path.expanduser(args.config), 'r', encoding='utf-8') as f:
                config = yaml.load(f, Loader=SafeLoader)

            errors = validate_config(config)
            if errors:
                logger.error("配置错误:")
                for e in errors:
                    logger.error(f"  - {e}")
                sys.exit(1)

            nodes, settings = load_config(args.config, logger)
            interactive_mode(nodes, settings, logger)
        except Exception as e:
            logger.error(f"错误: {e}")
            sys.exit(1)
        return

    # Web仪表盘模式 - 显示最近的执行结果
    if args.web_dashboard:
        logger = setup_logging(True)

        # 读取最近的历史记录
        history_file = args.history_file or "/tmp/exec_history.json"
        if os.path.exists(history_file):
            with open(history_file, 'r', encoding='utf-8') as f:
                history = json.load(f)

            if history:
                # 创建模拟结果用于仪表盘显示
                results = []
                for h in history[-10:]:  # 显示最近10次执行
                    for node in h.get('nodes', []):
                        results.append(ExecutionResult(
                            node_name=node,
                            host="",
                            success=True,
                            stdout="",
                            stderr="",
                            exit_code=0,
                            duration=h.get('duration', 0)
                        ))

                try:
                    start_web_dashboard(results)
                except KeyboardInterrupt:
                    print("\n仪表盘已关闭")
                sys.exit(0)
            else:
                logger.error("没有执行历史记录")
                sys.exit(1)
        else:
            logger.error(f"历史文件不存在: {history_file}")
            logger.info("请先执行命令并使用 --history-file 保存历史")
            sys.exit(1)

    # 常规模式检查
    has_action = args.command or args.script or args.upload or args.download or args.sync \
                 or args.health_check or args.tail or args.service_status or args.template
    if not has_action:
        parser.error("必须指定操作类型: -x/--execute, -s/--script, --upload, --download, --sync, "
                     "--health-check, --tail, --service-status 或 --template")

    if not args.config:
        parser.error("必须指定配置文件 (-c/--config)")

    if args.script and not os.path.exists(args.script):
        print(f"错误: 脚本文件不存在: {args.script}")
        sys.exit(1)

    # 设置日志
    logger = setup_logging(args.verbose)

    try:
        # 加载配置
        nodes, settings = load_config(args.config, logger)
    except FileNotFoundError as e:
        logger.error(f"错误: {e}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error(f"错误: 配置文件格式错误: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"错误: {e}")
        sys.exit(1)

    if not nodes:
        logger.error("错误: 配置文件中没有定义节点")
        sys.exit(1)

    # 应用设置
    timeout = settings.timeout
    if args.timeout:
        timeout = args.timeout

    use_parallel = settings.parallel
    if args.no_parallel:
        use_parallel = False

    max_workers = settings.max_workers
    if args.parallel:
        max_workers = args.parallel

    retry_times = args.retry
    retry_delay = settings.retry_delay

    use_pool = args.use_pool
    # 初始化全局连接池（仅在启用 --use-pool 时）
    if use_pool:
        _GLOBAL_POOL = ConnectionPool(max_connections=10, logger=logger)

    # 节点过滤
    if args.node_filter:
        filter_names = set(n.strip() for n in args.node_filter.split(','))
        nodes = [n for n in nodes if n.name in filter_names]
        if not nodes:
            logger.error(f"错误: 未找到匹配的节点: {args.node_filter}")
            sys.exit(1)

    # 标签过滤
    if args.tags:
        filter_tags = set(t.strip() for t in args.tags.split(','))
        nodes = [n for n in nodes if any(tag in filter_tags for tag in n.tags)]
        if not nodes:
            logger.error(f"错误: 未找到匹配标签的节点: {args.tags}")
            sys.exit(1)
        logger.info(f"按标签过滤: {args.tags}")

    # 解析模板变量
    template_vars = {}
    if args.var:
        for var in args.var:
            if '=' in var:
                key, value = var.split('=', 1)
                template_vars[key] = value
            else:
                logger.warning(f"忽略无效变量格式: {var}")

    # 处理模板命令
    if args.template and not args.command:
        args.command = args.template

    # 显示执行信息
    logger.info("=" * 60)
    logger.info("批量远程命令执行工具")
    logger.info("=" * 60)
    logger.info(f"配置文件: {args.config}")
    logger.info(f"目标节点: {len(nodes)} 个")
    for node in nodes:
        logger.info(f"  - {node.name} ({node.host})")

    # 判断执行类型
    transfer_type = None
    local_path = None
    remote_path = None
    monitor_type = None

    if args.upload:
        parts = args.upload.split(':')
        if len(parts) != 2:
            parser.error("--upload 格式应为 local_path:remote_path")
        local_path, remote_path = parts
        transfer_type = 'upload'
        logger.info(f"执行内容: 上传文件 {local_path} -> {remote_path}")
    elif args.download:
        parts = args.download.split(':')
        if len(parts) != 2:
            parser.error("--download 格式应为 remote_path:local_dir")
        remote_path, local_path = parts
        transfer_type = 'download'
        # 硝保下载目录存在
        os.makedirs(local_path, exist_ok=True)
        logger.info(f"执行内容: 下载文件 {remote_path} -> {local_path}")
    elif args.sync:
        parts = args.sync.split(':')
        if len(parts) != 2:
            parser.error("--sync 格式应为 local_dir:remote_dir")
        local_path, remote_path = parts
        transfer_type = 'sync'
        logger.info(f"执行内容: 同步目录 {local_path} -> {remote_path}")
    elif args.health_check:
        monitor_type = 'health'
        logger.info("执行内容: 节点健康检查")
    elif args.tail:
        monitor_type = 'tail'
        remote_path = args.tail
        logger.info(f"执行内容: 日志监控 {remote_path}")
    elif args.service_status:
        monitor_type = 'service'
        logger.info(f"执行内容: 服务状态检查 {args.service_status}")
    else:
        logger.info(f"执行内容: {args.command or f'脚本: {args.script}'}")

    logger.info(f"执行模式: {'并行' if use_parallel else '串行'}")
    if use_pool:
        logger.info(f"连接池: 启用")
    logger.info("=" * 60)

    # 执行命令或传输
    results = []
    transfer_results = []
    progress = ProgressBar(len(nodes), "执行进度")

    if transfer_type:
        # 文件传输模式
        if use_parallel and len(nodes) > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        transfer_on_node, node, transfer_type,
                        local_path, remote_path, timeout,
                        use_pool, retry_times, retry_delay, logger
                    ): node for node in nodes
                }

                for future in as_completed(futures):
                    result = future.result()
                    transfer_results.append(result)
                    print_transfer_result(result, logger)
                    progress.update(1)
        else:
            for node in nodes:
                result = transfer_on_node(
                    node, transfer_type, local_path, remote_path,
                    timeout, use_pool, retry_times, retry_delay, logger
                )
                transfer_results.append(result)
                print_transfer_result(result, logger)
                progress.update(1)

        progress.close()

        # 关闭全局连接池
        if use_pool:
            if _GLOBAL_POOL:
                _GLOBAL_POOL.close_all()
            _GLOBAL_POOL = None

        # 统计传输结果
        logger.info("=" * 60)
        success_count = sum(1 for r in transfer_results if r.success)
        fail_count = len(transfer_results) - success_count
        logger.info(f"传输完成: 成功 {success_count}, 失败 {fail_count}")

        if fail_count > 0:
            logger.warning("失败节点:")
            for r in transfer_results:
                if not r.success:
                    logger.warning(f"  - {r.node_name} ({r.host}): {r.error}")
        logger.info("=" * 60)

        sys.exit(0 if fail_count == 0 else 1)

    # 运维监控模式
    if monitor_type == 'health':
        health_results = []
        progress = ProgressBar(len(nodes), "健康检查")

        if use_parallel and len(nodes) > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        health_check_on_node, node, timeout,
                        use_pool, retry_times, retry_delay, logger
                    ): node for node in nodes
                }

                for future in as_completed(futures):
                    status = future.result()
                    health_results.append(status)
                    print_health_status(status, logger)
                    progress.update(1)
        else:
            for node in nodes:
                status = health_check_on_node(node, timeout, use_pool, retry_times, retry_delay, logger)
                health_results.append(status)
                print_health_status(status, logger)
                progress.update(1)

        progress.close()

        if use_pool:
            if _GLOBAL_POOL:
                _GLOBAL_POOL.close_all()
            _GLOBAL_POOL = None

        # 统计健康状态
        healthy_count = sum(1 for s in health_results if s.connected)
        unhealthy_count = len(health_results) - healthy_count
        logger.info("=" * 60)
        logger.info(f"健康检查完成: 健康 {healthy_count}, 不健康 {unhealthy_count}")
        logger.info("=" * 60)

        sys.exit(0 if unhealthy_count == 0 else 1)

    elif monitor_type == 'tail':
        # 日志监控 - 串行执行每个节点
        for node in nodes:
            logger.info(f"--- {node.name} ({node.host}) ---")
            try:
                pool = None
                if use_pool:
                    pool = _GLOBAL_POOL
                    if pool is None:
                        pool = ConnectionPool(max_connections=10, logger=logger)

                wrapper = SSHClientWrapper(node, timeout, pool, use_pool, logger)
                wrapper.connect_with_retry(retry_times, retry_delay)

                exit_code, stdout, stderr = wrapper.tail_file(remote_path, args.tail_lines)
                if exit_code == 0:
                    print(stdout)
                else:
                    logger.error(f"读取日志失败: {stderr}")
                wrapper.close(keep_in_pool=use_pool)
            except Exception as e:
                logger.error(f"节点 {node.name} 日志监控失败: {e}")

        if use_pool:
            if _GLOBAL_POOL:
                _GLOBAL_POOL.close_all()
            _GLOBAL_POOL = None

        sys.exit(0)

    elif monitor_type == 'service':
        services = [s.strip() for s in args.service_status.split(',')]
        service_results = []

        for service_name in services:
            logger.info(f"检查服务: {service_name}")
            progress = ProgressBar(len(nodes), f"服务 {service_name}")

            if use_parallel and len(nodes) > 1:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            service_status_on_node, node, service_name,
                            timeout, use_pool, retry_times, retry_delay, logger
                        ): node for node in nodes
                    }

                    for future in as_completed(futures):
                        status = future.result()
                        service_results.append(status)
                        print_service_status(status, logger)
                        progress.update(1)
            else:
                for node in nodes:
                    status = service_status_on_node(node, service_name, timeout, use_pool, retry_times, retry_delay, logger)
                    service_results.append(status)
                    print_service_status(status, logger)
                    progress.update(1)

            progress.close()

        if use_pool:
            if _GLOBAL_POOL:
                _GLOBAL_POOL.close_all()
            _GLOBAL_POOL = None

        # 统计服务状态
        running_count = sum(1 for s in service_results if s.is_running)
        stopped_count = len(service_results) - running_count
        logger.info("=" * 60)
        logger.info(f"服务检查完成: 运行 {running_count}, 停止 {stopped_count}")
        logger.info("=" * 60)

        sys.exit(0 if stopped_count == 0 else 1)

    # 命令执行模式
    # 执行预检查
    if args.pre_check:
        logger.info("=" * 60)
        logger.info("执行预检查")
        logger.info("=" * 60)

        pre_check_results = {}
        for node in nodes:
            logger.info(f"检查节点: {node.name}")
            checks = run_pre_check(node, args.pre_check, timeout, use_pool, retry_times, retry_delay, logger)
            pre_check_results[node.name] = checks

            all_passed = all(c["passed"] for c in checks.values())
            status = "✓" if all_passed else "✗"
            logger.info(f"  [{status}] {node.name}")

            for check_name, check_result in checks.items():
                check_status = "✓" if check_result["passed"] else "✗"
                logger.info(f"    [{check_status}] {check_name}: {check_result['output']}")
                if check_result["error"]:
                    logger.error(f"      错误: {check_result['error']}")

        # 检查是否有失败的预检查
        failed_nodes = [name for name, checks in pre_check_results.items()
                       if not all(c["passed"] for c in checks.values())]

        if failed_nodes:
            logger.error(f"预检查失败节点: {failed_nodes}")
            logger.error("终止执行")
            sys.exit(1)

        logger.info("所有预检查通过，开始执行")

    results = []

    # 进度可视化选择
    if args.progress_chart and HAS_RICH:
        progress_chart = ProgressChart(len(nodes), logger)
        use_progress_chart = True
    else:
        progress = ProgressBar(len(nodes), "执行进度")
        use_progress_chart = False

    execution_start = time.time()

    if use_parallel and len(nodes) > 1:
        # 并行执行
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    execute_on_node, node, args.command, args.script,
                    timeout, use_pool, retry_times, retry_delay, logger, template_vars
                ): node for node in nodes
            }

            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                print_result(result, args.verbose, logger)

                if use_progress_chart:
                    progress_chart.update(result.node_name, result.duration, result.success)
                else:
                    progress.update(1)

                # 条件执行检查
                if args.condition:
                    current_success = sum(1 for r in results if r.success)
                    if check_condition(args.condition, len(nodes), current_success):
                        logger.warning(f"条件满足 {args.condition}, 终止后续执行")
                        break
    else:
        # 串行执行
        for node in nodes:
            result = execute_on_node(node, args.command, args.script, timeout, use_pool, retry_times, retry_delay, logger, template_vars)
            results.append(result)
            print_result(result, args.verbose, logger)

            if use_progress_chart:
                progress_chart.update(result.node_name, result.duration, result.success)
            else:
                progress.update(1)

            # 条件执行检查
            if args.condition:
                current_success = sum(1 for r in results if r.success)
                if check_condition(args.condition, len(nodes), current_success):
                    logger.warning(f"条件满足 {args.condition}, 终止后续执行")
                    break

    if use_progress_chart:
        progress_chart.close()
    else:
        progress.close()

    # 关闭全局连接池
    if use_pool:
        if _GLOBAL_POOL:
            _GLOBAL_POOL.close_all()
        _GLOBAL_POOL = None

    # 执行后验证
    if args.post_verify:
        logger.info("=" * 60)
        logger.info("执行后验证")
        logger.info("=" * 60)

        post_verify_results = {}
        for node in nodes:
            logger.info(f"验证节点: {node.name}")
            verifies = run_post_verify(node, args.post_verify, timeout, use_pool, retry_times, retry_delay, logger)
            post_verify_results[node.name] = verifies

            all_passed = all(v["passed"] for v in verifies.values())
            status = "✓" if all_passed else "✗"
            logger.info(f"  [{status}] {node.name}")

            for verify_name, verify_result in verifies.items():
                verify_status = "✓" if verify_result["passed"] else "✗"
                logger.info(f"    [{verify_status}] {verify_name}: {verify_result['output']}")
                if verify_result["error"]:
                    logger.error(f"      错误: {verify_result['error']}")

        # 检查是否有失败的后验证
        failed_nodes = [name for name, verifies in post_verify_results.items()
                       if not all(v["passed"] for v in verifies.values())]

        if failed_nodes:
            logger.error(f"后验证失败节点: {failed_nodes}")

    # 导出结果
    if args.export_json:
        try:
            export_results_json(results, args.export_json)
            logger.info(f"结果已导出到: {args.export_json}")
        except Exception as e:
            logger.error(f"导出JSON失败: {e}")

    if args.export_csv:
        try:
            export_results_csv(results, args.export_csv)
            logger.info(f"结果已导出到: {args.export_csv}")
        except Exception as e:
            logger.error(f"导出CSV失败: {e}")

    if args.export_html:
        try:
            export_results_html(results, args.export_html)
            logger.info(f"HTML报告已导出到: {args.export_html}")
        except Exception as e:
            logger.error(f"导出HTML失败: {e}")

    # 输出比对
    if args.compare:
        compare_outputs(results, logger)

    # 结果并排对比视图
    if args.side_by_side and HAS_RICH:
        side_by_side_view = SideBySideView(results, logger)
        side_by_side_view.display()

    # 发送通知
    if args.notify:
        for notify in args.notify:
            if ':' in notify:
                notify_type, target = notify.split(':', 1)
                logger.info(f"发送通知: {notify_type} -> {target}")
                if send_notification(notify_type, target, results, args.command, logger):
                    logger.info("通知发送成功")
                else:
                    logger.warning("通知发送失败")

    # 发送告警
    if args.alert and fail_count > 0:
        for alert in args.alert:
            if ':' in alert:
                alert_type, target = alert.split(':', 1)
                logger.info(f"发送告警: {alert_type}")
                if send_alert(alert_type, target, results, args.command, logger):
                    logger.info("告警发送成功")
                else:
                    logger.warning("告警发送失败")

    # 统计结果
    logger.info("=" * 60)
    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count
    execution_duration = time.time() - execution_start
    logger.info(f"执行完成: 成功 {success_count}, 失败 {fail_count} (耗时 {execution_duration:.1f}s)")

    if fail_count > 0:
        logger.warning("失败节点:")
        for r in results:
            if not r.success:
                logger.warning(f"  - {r.node_name} ({r.host}): {r.error or r.stderr.strip()[:50]}")

    # 保存执行历史
    if args.history_file:
        history = ExecutionHistory(
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
            command=args.command,
            script=args.script,
            node_count=len(nodes),
            success_count=success_count,
            fail_count=fail_count,
            duration=execution_duration,
            nodes=[n.name for n in nodes]
        )
        save_execution_history(history, args.history_file)
        logger.info(f"执行历史已保存到: {args.history_file}")

    logger.info("=" * 60)

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == '__main__':
    main()