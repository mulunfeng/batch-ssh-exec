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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Any
import json

import paramiko
import yaml
from paramiko.ssh_exception import SSHException, AuthenticationException

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader


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


# ============== SSH客户端封装 ==============
class SSHClientWrapper:
    """SSH客户端封装（带重试机制）"""

    def __init__(
        self,
        node: NodeConfig,
        timeout: int = 30,
        pool: Optional[ConnectionPool] = None,
        use_pool: bool = True,
        logger: Optional[logging.Logger] = None
    ):
        self.node = node
        self.timeout = timeout
        self.use_pool = use_pool
        self.logger = logger or logging.getLogger("batch_exec")
        self.pool = pool
        self.client: Optional[paramiko.SSHClient] = None

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
            'allow_agent': False,
            'look_for_keys': False,
        }

        if self.node.private_key:
            key = paramiko.RSAKey.from_private_key_file(self.node.private_key)
            connect_kwargs['pkey'] = key
        elif self.node.password:
            connect_kwargs['password'] = self.node.password
        else:
            raise ValueError(f"节点 {self.node.name} 未配置密码或密钥")

        client.connect(**connect_kwargs)
        return client

    def execute(self, command: str, command_timeout: Optional[int] = None) -> tuple[int, str, str]:
        """执行命令，支持sudo"""
        if not self.client:
            raise Exception("未建立SSH连接")

        timeout = command_timeout or self.timeout

        # 如果配置了sudo密码，使用sudo执行
        if self.node.sudo_password:
            sudo_user = self.node.sudo_user or "root"
            full_command = f"sudo -S -u {sudo_user} bash -c '{command.replace(\"'\", \"'\\''\")}'"
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

    def close(self, keep_in_pool: bool = True):
        """关闭连接"""
        if self.client and self.pool and keep_in_pool:
            # 连接会保留在池中，由池统一管理
            pass
        elif self.client:
            self.client.close()
            self.client = None


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
        if not node.get('password') and not node.get('private_key'):
            errors.append(f"节点 {i+1} ({node.get('host', 'N/A')}): 未配置密码或私钥")

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
        sudo_password=settings_data.get('sudo_password')
    )

    nodes = []
    global_sudo_password = settings.sudo_password

    for node_data in config.get('nodes', []):
        private_key = node_data.get('private_key')
        if private_key:
            private_key = os.path.expanduser(private_key)

        node = NodeConfig(
            name=node_data.get('name', node_data.get('host')),
            host=node_data['host'],
            port=node_data.get('port', 22),
            username=node_data['username'],
            password=node_data.get('password'),
            private_key=private_key,
            sudo_password=node_data.get('sudo_password', global_sudo_password),
            sudo_user=node_data.get('sudo_user')
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
    logger: Optional[logging.Logger] = None
) -> ExecutionResult:
    """在单个节点上执行命令"""
    start_time = time.time()
    logger = logger or logging.getLogger("batch_exec")

    try:
        pool = None
        if use_pool:
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
    finally:
        if pool:
            pool.close_all()


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
    parser.add_argument('--timeout', type=int, help='SSH连接超时时间 (默认: 30秒)')
    parser.add_argument('--parallel', type=int, help='并行执行数 (默认: 5)')
    parser.add_argument('--no-parallel', action='store_true', help='禁用并行执行')
    parser.add_argument('-v', '--verbose', action='store_true', help='显示详细输出')
    parser.add_argument('--interactive', action='store_true', help='交互式shell模式')
    parser.add_argument('--export-json', dest='export_json', help='导出结果为JSON文件')
    parser.add_argument('--export-csv', dest='export_csv', help='导出结果为CSV文件')
    parser.add_argument('--use-pool', action='store_true', help='启用连接池复用')
    parser.add_argument('--retry', type=int, default=3, help='重试次数 (默认: 3)')

    args = parser.parse_args()

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

    # 常规模式检查
    if not args.command and not args.script:
        parser.error("必须指定 -x/--execute 或 -s/--script")

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

    # 节点过滤
    if args.node_filter:
        filter_names = set(n.strip() for n in args.node_filter.split(','))
        nodes = [n for n in nodes if n.name in filter_names]
        if not nodes:
            logger.error(f"错误: 未找到匹配的节点: {args.node_filter}")
            sys.exit(1)

    # 显示执行信息
    logger.info("=" * 60)
    logger.info("批量远程命令执行工具")
    logger.info("=" * 60)
    logger.info(f"配置文件: {args.config}")
    logger.info(f"目标节点: {len(nodes)} 个")
    for node in nodes:
        logger.info(f"  - {node.name} ({node.host})")
    logger.info(f"执行内容: {args.command or f'脚本: {args.script}'}")
    logger.info(f"执行模式: {'并行' if use_parallel else '串行'}")
    if use_pool:
        logger.info(f"连接池: 启用")
    logger.info("=" * 60)

    # 执行命令
    results = []
    progress = ProgressBar(len(nodes), "执行进度")

    if use_parallel and len(nodes) > 1:
        # 并行执行
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    execute_on_node, node, args.command, args.script,
                    timeout, use_pool, retry_times, retry_delay, logger
                ): node for node in nodes
            }

            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                print_result(result, args.verbose, logger)
                progress.update(1)
    else:
        # 串行执行
        for node in nodes:
            result = execute_on_node(node, args.command, args.script, timeout, use_pool, retry_times, retry_delay, logger)
            results.append(result)
            print_result(result, args.verbose, logger)
            progress.update(1)

    progress.close()

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

    # 统计结果
    logger.info("=" * 60)
    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count
    logger.info(f"执行完成: 成功 {success_count}, 失败 {fail_count}")

    if fail_count > 0:
        logger.warning("失败节点:")
        for r in results:
            if not r.success:
                logger.warning(f"  - {r.node_name} ({r.host}): {r.error or r.stderr.strip()[:50]}")
    logger.info("=" * 60)

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == '__main__':
    main()