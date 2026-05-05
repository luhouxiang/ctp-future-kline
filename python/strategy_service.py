# -*- coding: utf-8 -*-
"""
策略服务示例程序（中文详注版）
================================

本文件实现了一个以 gRPC 暴露的策略运行服务，核心职责包括：
1. 注册并列出可用策略；
2. 启动/停止策略实例；
3. 接收 tick / bar / replay bar 事件并交给对应策略处理；
4. 返回策略信号、无信号原因、诊断指标 metrics 与流程追踪 trace；
5. 提供回测与参数扫描的占位接口，方便上层系统先完成联调。

当前内置两个策略：
- SampleMomentumStrategy：一个非常简单的示例策略，根据 K 线涨跌或 tick 买卖价差给出目标仓位。
- MA20PullbackShortStrategy：一个 MA20 反抽做空策略，使用状态机识别：
  先跌破 MA -> 反抽触碰 MA -> 再跌破触碰 K 的开盘价 -> 触发 SHORT 信号。

注意：
- 注释仅解释代码意图，不改变原有逻辑。
- 请求和响应在 gRPC 层被包装为 JSON bytes，服务内部统一使用 dict 处理。
- 若未安装 grpcio，本文件仍可被导入，但启动服务会抛出 RuntimeError。
"""

# argparse：解析命令行参数，例如服务监听地址 --addr。
import argparse
# json：负责 gRPC bytes 与 Python dict 之间的序列化/反序列化。
import json
# logging：记录服务启动、请求异常、warmup 异常等日志。
import logging
import os
import sys
# time：生成更新时间、服务时间，并解析部分事件时间戳。
import time
# futures.ThreadPoolExecutor：作为 gRPC server 的线程池执行器。
from concurrent import futures
# dataclass/field：用于简洁定义策略状态对象 PullbackShortState。
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
# RLock：可重入锁，用来保护多线程 gRPC 请求下的策略状态字典。
from threading import RLock

from ma20_weak_pullback_baseline import build_strategy as build_ma20_weak_baseline_strategy
from ma20_weak_pullback_hard_filter import build_strategy as build_ma20_weak_hard_filter_strategy
from ma20_weak_pullback_score_filter import build_strategy as build_ma20_weak_score_filter_strategy

# grpc 是运行服务时的可选依赖：
# - 如果环境中安装了 grpcio，则可以正常启动 gRPC 服务；
# - 如果没有安装，允许模块被导入，但 build_server/add_unary 会明确报错。
try:
    import grpc
except ModuleNotFoundError:
    grpc = None

# 模块级 logger，供本文件所有函数/类复用。
logger = logging.getLogger(__name__)
DEFAULT_STRATEGY_LOG_FILE = os.path.join("flow", "strategy_logs", "strategy_service.log")


def _log_formatter():
    return logging.Formatter("%(asctime)s %(levelname)s %(name)s %(filename)s:%(lineno)d %(message)s")


def _configure_logging(log_file, level_name="INFO"):
    """Configure console and UTF-8 rotating file logs for strategy execution."""
    level = getattr(logging, str(level_name or "INFO").upper(), logging.INFO)
    handlers = [logging.StreamHandler()]
    if log_file:
        log_dir = os.path.dirname(os.path.abspath(log_file))
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        handlers.append(RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"))
    formatter = _log_formatter()
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=level, handlers=handlers, force=True)


def _ensure_logging_configured(log_file=DEFAULT_STRATEGY_LOG_FILE, level_name=None):
    root = logging.getLogger()
    target_path = os.path.abspath(log_file) if log_file else ""
    level = getattr(logging, str(level_name or os.environ.get("STRATEGY_LOG_LEVEL", "INFO")).upper(), logging.INFO)
    if not root.handlers:
        _configure_logging(log_file, level_name or os.environ.get("STRATEGY_LOG_LEVEL", "INFO"))
        return
    root.setLevel(level)
    if not target_path:
        return
    for handler in root.handlers:
        if isinstance(handler, logging.FileHandler) and os.path.abspath(getattr(handler, "baseFilename", "")) == target_path:
            return
    log_dir = os.path.dirname(target_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    handler = RotatingFileHandler(target_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    handler.setFormatter(_log_formatter())
    root.addHandler(handler)


def _log_strategy_phase(request, response):
    """Write one compact line for every strategy phase emitted by a decision."""
    trace = (response or {}).get("trace") or {}
    if not trace:
        return
    instance = request.get("instance") or {}
    metrics = (response or {}).get("metrics") or trace.get("metrics") or {}
    logger.info(
        "strategy_phase instance_id=%s strategy_id=%s symbol=%s mode=%s event_type=%s step=%s/%s:%s status=%s reason=%s signal=%s",
        trace.get("instance_id") or instance.get("instance_id", ""),
        trace.get("strategy_id") or instance.get("strategy_id", ""),
        trace.get("symbol") or request.get("symbol", ""),
        trace.get("mode") or request.get("mode", ""),
        trace.get("event_type", ""),
        trace.get("step_index", ""),
        trace.get("step_total", ""),
        trace.get("step_key", ""),
        trace.get("status", ""),
        trace.get("reason") or (response or {}).get("reason", ""),
        metrics.get("signal", ""),
    )


# -----------------------------------------------------------------------------
# JSON 编解码工具
# -----------------------------------------------------------------------------
def _loads(data):
    """将 gRPC 收到的 bytes 请求体反序列化为 Python dict。

    参数：
        data: gRPC unary 请求传入的 bytes。上层约定其内容为 UTF-8 编码的 JSON。

    返回：
        dict: 若 data 为空，返回空字典；否则返回 json.loads 后的对象。

    说明：
        当前服务使用泛型 gRPC handler，没有使用 protobuf message 类型；
        因此 request_deserializer 直接返回原始 bytes，再由这里统一转 dict。
    """
    if not data:
        return {}
    return json.loads(data.decode("utf-8"))


def _dumps(value):
    """将 Python 对象序列化为 UTF-8 JSON bytes，作为 gRPC 响应体。

    ensure_ascii=False：保留中文字符，便于调试时直接阅读。
    separators=(",", ":")：去掉多余空格，压缩响应体体积。
    """
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# -----------------------------------------------------------------------------
# MA20 反抽做空策略的状态常量
# -----------------------------------------------------------------------------
# 初始状态：等待价格有效跌破 MA20。
WAIT_BREAK_BELOW_MA20 = "WAIT_BREAK_BELOW_MA20"
# 已经确认跌破 MA20：接下来等待价格反抽并触碰 MA20。
BROKEN_BELOW_MA20 = "BROKEN_BELOW_MA20"
# 已找到触碰 MA20 的 K 线：接下来等待 tick 最新价跌破该触碰 K 的开盘价。
WAIT_BREAK_TOUCH_OPEN = "WAIT_BREAK_TOUCH_OPEN"
# 已发出信号，等待后续 K 线确认信号成功、失败或观察窗口结束。
SIGNAL_ACTIVE = "SIGNAL_ACTIVE"
# 策略流程已完成：已经触发 SHORT 信号，后续不再重复触发。
DONE = "DONE"

# MA20 弱反弹做空策略步骤：bar 驱动，不再依赖 tick 入场。
TREND_STRUCTURE_FILTER = "TREND_STRUCTURE_FILTER"
WAIT_PULLBACK_TOUCH_MA20 = "WAIT_PULLBACK_TOUCH_MA20"
WAIT_BREAK_REACTION_LOW = "WAIT_BREAK_REACTION_LOW"
SHORT_SIGNAL = "SHORT_SIGNAL"
MA20_WEAK_STRATEGY_ID = "ma20.weak_pullback_short"
MA20_WEAK_BASELINE_STRATEGY_ID = "ma20.weak_pullback_short.baseline"
MA20_WEAK_HARD_FILTER_STRATEGY_ID = "ma20.weak_pullback_short.hard_filter"
MA20_WEAK_SCORE_FILTER_STRATEGY_ID = "ma20.weak_pullback_short.score_filter"


# -----------------------------------------------------------------------------
# 基础类型转换与请求字段读取工具
# -----------------------------------------------------------------------------
def _float(value, default=0.0):
    """安全地将任意值转换为 float，失败时返回默认值。

    这样可以避免行情字段为空字符串、None 或异常类型时导致策略直接崩溃。
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value, default=0):
    """安全地将任意值转换为 int，失败时返回默认值。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _instance_id(request):
    """从请求中读取策略实例 ID。

    请求结构预期类似：
        {"instance": {"instance_id": "..."}, ...}

    若字段不存在，返回空字符串，避免 KeyError。
    """
    return (request.get("instance") or {}).get("instance_id", "")


def _params(request):
    """从请求中读取策略实例参数 params。

    返回值始终是 dict；如果 instance 或 params 缺失，则返回空字典。
    """
    return (request.get("instance") or {}).get("params") or {}


def _no_signal(request, reason, metrics=None, trace=None):
    """构造统一的“无交易信号”响应。

    参数：
        request: 原始请求 dict，用于回填 instance_id/symbol/event_time 等上下文。
        reason: 无信号原因，供上层或前端展示。
        metrics: 策略诊断指标，例如当前状态、均线值、等待 bar 数等。
        trace: 更细粒度的步骤追踪信息，用于解释状态机当前卡在哪一步。

    返回：
        dict: 标准响应对象，no_signal=True，confidence=0。

    说明：
        target_position 默认保持当前仓位 current_position；
        也就是说，无信号时策略不主动改变仓位。
    """
    out = {
        "no_signal": True,
        "instance_id": _instance_id(request),
        "symbol": request.get("symbol", ""),
        "event_time": request.get("event_time", ""),
        "target_position": request.get("current_position", 0),
        "confidence": 0,
        "reason": reason,
        "metrics": metrics or {},
    }
    # trace 是可选字段：只有在调用方传入时才添加，避免响应过于臃肿。
    if trace is not None:
        out["trace"] = trace
    logger.info("no_signal_from_strategy: instance_id=%s symbol=%s event_time=%s reason=%s metrics=%s trace=%s",
                out["instance_id"], out["symbol"], out["event_time"], reason, metrics, trace)
    return out


def _trace(request, event_type, step_key, step_label, step_index, status, reason, checks=None, metrics=None, signal_preview=None):
    """构造统一的策略流程追踪对象。

    trace 用于描述一次事件处理时，策略状态机位于哪一步、该步通过/等待/失败的原因，
    以及参与判断的检查项 checks。

    主要字段：
        event_type: 事件类型，例如 "bar" 或 "key_tick"。
        step_key: 当前状态/步骤的机器可读标识。
        step_label: 当前状态/步骤的中文展示文案。
        step_index: 当前步骤序号，用于前端进度展示。
        step_total: 总步骤数，默认从 metrics.step_total 读取，否则为 5。
        status: waiting/passed/failed/done 等状态。
        checks: 单项条件检查列表。
        signal_preview: 若即将或已经触发信号，可放入目标仓位、置信度等预览信息。
    """
    instance = request.get("instance") or {}
    return {
        "instance_id": _instance_id(request),
        "strategy_id": instance.get("strategy_id", ""),
        "symbol": request.get("symbol", ""),
        "timeframe": instance.get("timeframe", ""),
        "mode": request.get("mode", ""),
        "event_type": event_type,
        "event_time": request.get("event_time", ""),
        "step_key": step_key,
        "step_label": step_label,
        "step_index": step_index,
        "step_total": int((metrics or {}).get("step_total") or 5),
        "status": status,
        "reason": reason,
        "checks": checks or [],
        "metrics": metrics or {},
        "signal_preview": signal_preview or {},
    }


def _check(name, passed, current=None, target=None, delta=None, description=""):
    """构造一个条件检查项。

    参数：
        name: 检查项名称，例如“收盘低于 MA20”。
        passed: 条件是否通过，会被强制转换为 bool。
        current: 当前实际值。
        target: 目标/阈值。
        delta: current 与 target 的差值，方便前端或日志判断距离条件还差多少。
        description: 人类可读的补充说明。
    """
    logger.info("check_condition: name=%s passed=%s current=%s target=%s delta=%s description=%s",
                name, bool(passed), current, target, delta, description)
    return {
        "name": name,
        "passed": bool(passed),
        "current": current,
        "target": target,
        "delta": delta,
        "description": description,
    }


# -----------------------------------------------------------------------------
# 策略基类
# -----------------------------------------------------------------------------
class Strategy:
    """所有策略的最小接口基类。

    约定：
    - 每个策略通过 definition 暴露元数据，例如 strategy_id、display_name、默认参数等。
    - Runtime 服务会调用 start_instance/stop_instance 管理实例生命周期。
    - 行情事件通过 on_tick/on_bar/on_replay_bar 分发给策略。

    子类可以只覆盖自己关心的方法；未覆盖的方法默认返回“忽略/无信号”。
    """

    # 策略元数据，子类通常会覆盖。
    definition = {}

    def start_instance(self, instance):
        """启动策略实例时调用。默认无状态策略不需要做任何事。"""
        return None

    def stop_instance(self, instance_id):
        """停止策略实例时调用。默认无状态策略不需要清理。"""
        return None

    def on_tick(self, request):
        """处理 tick 事件。默认忽略 tick。"""
        return _no_signal(request, "tick ignored")

    def on_bar(self, request):
        logger.info("on_bar called with request: %s", request)
        """处理 K 线 bar 事件。默认忽略 bar。"""
        return _no_signal(request, "bar ignored")

    def on_replay_bar(self, request):
        """处理回放 K 线事件。默认复用 on_bar 逻辑。"""
        return self.on_bar(request)


# -----------------------------------------------------------------------------
# 请求校验与时间处理工具
# -----------------------------------------------------------------------------
def _mode_key(request):
    """将请求中的 mode 归一化为状态字典使用的 key。

    当前只区分两类：
    - "replay"：回放模式；
    - "live"：非 replay 的所有情况都归为实盘/实时模式。

    这样同一个 instance_id + symbol 在 live 和 replay 下可以拥有互不干扰的状态。
    """
    mode = str(request.get("mode") or "").strip().lower()
    if mode == "replay":
        return "replay"
    return "live"


def _require_fields(source, fields):
    """校验 source 中是否包含必填字段。

    若字段缺失、为 None 或空字符串，则抛出 ValueError。
    上层 add_unary 会捕获该异常并转换为 gRPC INVALID_ARGUMENT。
    """
    missing = [name for name in fields if source.get(name) is None or source.get(name) == ""]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")


def _event_ts(value):
    """将事件时间转换为可比较的秒级时间戳。

    支持的输入：
    - None 或空字符串：返回 None；
    - int/float：直接转为 float；
    - 数字字符串：转为 float；
    - ISO 风格字符串：例如 2026-01-01T09:30:00 或带 Z 后缀的 UTC 时间。

    返回 None 表示无法解析，此时调用方通常不做乱序检查。
    """
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    try:
        # 将尾部 Z 替换成 +00:00，但下面仅截取前 19 位，即 YYYY-mm-ddTHH:MM:SS。
        # 这里使用 time.strptime + time.mktime，得到的是本地时区解释下的时间戳。
        normalized = text.replace("Z", "+00:00")
        return time.mktime(time.strptime(normalized[:19], "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None


# -----------------------------------------------------------------------------
# 示例动量策略
# -----------------------------------------------------------------------------
class SampleMomentumStrategy(Strategy):
    """一个极简示例策略，用于演示策略接口如何返回信号。

    K 线事件：
        - close > open：目标仓位 1；
        - close < open：目标仓位 -1；
        - close == open：目标仓位 0。

    tick 事件：
        - last_price >= ask：目标仓位 1；
        - last_price <= bid：目标仓位 -1；
        - 否则目标仓位 0。

    注意：
        该策略不是真实交易策略，只是接口样例。
    """

    # definition 会被 ListStrategies 返回给外部系统。
    definition = {
        "strategy_id": "sample.momentum",
        "display_name": "Sample Momentum",
        "entry_script": "python/strategy_service.py",
        "version": "1.0.0",
        "default_params": {"threshold": 0.2},
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    def _decision(self, request):
        """统一处理 bar/tick 两类事件并生成示例目标仓位。"""
        bar = request.get("bar") or {}
        tick = request.get("tick") or {}
        metrics = {}
        # 默认保持当前仓位；只有 bar/tick 判断出方向时才覆盖。
        target = request.get("current_position", 0)
        if bar:
            # K 线逻辑：用收盘价减开盘价作为方向判断。
            close = _float(bar.get("close"))
            open_price = _float(bar.get("open"), close)
            delta = close - open_price
            metrics = {"bar_delta": delta}
            target = 1 if delta > 0 else -1 if delta < 0 else 0
        elif tick:
            # tick 逻辑：根据最新价相对一档买卖价的位置生成方向。
            last_price = _float(tick.get("last_price"))
            bid = _float(tick.get("bid_price1"), last_price)
            ask = _float(tick.get("ask_price1"), last_price)
            metrics = {"spread": ask - bid}
            target = 1 if last_price >= ask else -1 if last_price <= bid else 0
        return {
            "no_signal": False,
            "instance_id": _instance_id(request),
            "symbol": request.get("symbol", ""),
            "event_time": request.get("event_time", ""),
            "target_position": target,
            "confidence": 0.5,
            "reason": "sample strategy decision",
            "metrics": metrics,
        }

    def on_tick(self, request):
        """tick 事件复用 _decision。"""
        return self._decision(request)

    def on_bar(self, request):
        """bar 事件复用 _decision。"""
        return self._decision(request)


# -----------------------------------------------------------------------------
# MA20 反抽做空策略状态对象
# -----------------------------------------------------------------------------
@dataclass
class PullbackShortState:
    """保存 MA20 反抽做空策略在单个实例+品种+模式下的状态。

    状态维度由 (mode, instance_id, symbol) 唯一定位。
    这样可以让同一策略同时服务多个策略实例、多个交易品种、live/replay 两种模式。
    """

    # 当前状态机状态，默认等待第一次跌破 MA。
    state: str = WAIT_BREAK_BELOW_MA20
    # 最近收盘价序列，用来计算移动平均线。会被裁剪，避免无限增长。
    closes: list[float] = field(default_factory=list)
    # 已处理的最后一根 K 线时间戳，用于过滤重复或乱序 K 线。
    last_bar_ts: float | None = None
    # 上一根 K 线的收盘价，用于判断“从上向下跌破”。
    prev_close: float | None = None
    # 上一根 K 线对应的均线值，用于判断“从上向下跌破”。
    prev_ma: float | None = None
    # 反抽触碰 MA 的那根 K 线的开盘价；后续 tick 跌破该价时触发做空。
    touch_open: float | None = None
    # 反抽触碰 MA 的那根 K 线的最高价，用于记录形态信息。
    touch_high: float | None = None
    # 触碰发生时的 MA 值，用于后续 metrics/trace 展示。
    touch_ma20: float | None = None
    # 触碰 K 的时间，便于前端或日志定位触发形态。
    touch_time: str = ""
    # 从找到触碰 K 之后已经等待了多少根 bar，用于超时重置。
    wait_bars: int = 0
    # 形态失败后是否要求下一次必须“整根 K 线都在 MA 下方”才重新确认跌破。
    reset_requires_full_break: bool = False
    # 是否已经看到至少一根 OHLC 全部在 MA 上方的 K 线；只有 armed 后才允许等待“跌破 MA”。
    break_below_armed: bool = False
    signal_entry: float | None = None
    signal_profit_target: float | None = None
    signal_adverse_target: float | None = None
    signal_bars: int = 0
    signal_time: str = ""


@dataclass
class WeakPullbackShortState:
    """State for the automated weak MA20 pullback short strategy."""

    state: str = WAIT_BREAK_BELOW_MA20
    opens: list[float] = field(default_factory=list)
    highs: list[float] = field(default_factory=list)
    lows: list[float] = field(default_factory=list)
    closes: list[float] = field(default_factory=list)
    last_bar_ts: float | None = None
    prev_close: float | None = None
    prev_ma20: float | None = None
    break_time: str = ""
    break_index: int = -1
    bars_since_break: int = 0
    reaction_low: float | None = None
    touch_open: float | None = None
    touch_high: float | None = None
    touch_ma20: float | None = None
    touch_time: str = ""
    trigger_wait_bars: int = 0
    regime: str = ""
    bullish_pause_score: float = 0.0
    bearish_failure_score: float = 0.0
    signal_entry: float | None = None
    signal_profit_target: float | None = None
    signal_adverse_target: float | None = None
    signal_bars: int = 0
    signal_time: str = ""


# -----------------------------------------------------------------------------
# MA20 反抽做空策略
# -----------------------------------------------------------------------------
class MA20PullbackShortStrategy(Strategy):
    """MA20 反抽做空策略。

    该策略使用状态机识别一个偏空形态：

    第 1 阶段：等待 MA 数据足够。
    第 2 阶段：等待价格从上向下跌破 MA。
    第 3 阶段：跌破后，等待价格反抽，K 线最高价触碰/超过 MA。
    第 4 阶段：找到触碰 K 后，等待 tick 最新价跌破该 K 线开盘价。
    第 5 阶段：触发 SHORT，目标仓位 -1，状态置为 DONE。

    失败重置条件：
    - 在等待 tick 跌破触碰 K 开盘价期间，如果某根 K 线开盘和收盘都重新站上 MA，形态失败；
    - 如果等待 bar 数超过 max_wait_bars，也视为形态失败；
    - 失败后 reset_requires_full_break=True，要求下一轮用 full_break 重新确认下破。

    参数：
    - ma_period：均线周期，默认 20，最小 2；
    - max_wait_bars：触碰 K 出现后最多等待多少根 bar，默认 6，最小 1。
    """

    definition = {
        "strategy_id": "ma20.pullback_short",
        "display_name": "MA20 Pullback Short",
        "entry_script": "python/strategy_service.py",
        "version": "1.0.0",
        "default_params": {
            "ma_period": 20,
            "max_wait_bars": 6,
            "observation_bars": 24,
            "profit_atr_multiple": 1.0,
            "adverse_atr_multiple": 0.8,
        },
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    def __init__(self, definition=None):
        """初始化状态容器与锁。"""
        if definition is not None:
            self.definition = definition
        # states 保存每个 (mode, instance_id, symbol) 对应的 PullbackShortState。
        self.states = {}
        # gRPC server 使用线程池处理请求，因此策略状态读写需要加锁。
        self._lock = RLock()

    def start_instance(self, instance):
        """启动一个策略实例，并为其订阅品种初始化状态。

        instance 预期包含：
        - instance_id: 策略实例 ID；
        - symbols: 交易品种列表；
        - params: 策略参数，可包含 warmup_bars；
        - mode: live 或 replay。
        """
        instance_id = instance.get("instance_id", "")
        symbols = instance.get("symbols") or []
        params = instance.get("params") or {}
        mode = "replay" if str(instance.get("mode") or "").strip().lower() == "replay" else "live"
        # 若 symbols 为空，使用空字符串作为兜底 symbol key，保证状态仍可创建。
        if not symbols:
            symbols = [""]
        with self._lock:
            for symbol in symbols:
                self.states[(mode, instance_id, str(symbol))] = PullbackShortState()
        # 如果启动实例时提供了历史 K 线，则先喂入状态机用于预热 MA 与状态。
        self._apply_warmup_bars(instance, params.get("warmup_bars") or [], mode)

    def stop_instance(self, instance_id):
        """停止策略实例，并清理该实例下所有 symbol/mode 的状态。"""
        with self._lock:
            # 复制 keys 后再删除，避免遍历字典时修改字典。
            for key in list(self.states):
                if len(key) >= 2 and key[1] == instance_id:
                    del self.states[key]

    def _state_for(self, request):
        """根据请求定位策略状态；如果不存在则懒创建。"""
        key = (_mode_key(request), _instance_id(request), request.get("symbol", ""))
        if key not in self.states:
            self.states[key] = PullbackShortState()
        return self.states[key]

    def _apply_warmup_bars(self, instance, bars, mode):
        """用 warmup_bars 预热策略状态。

        warmup_bars 通常是策略启动前的一段历史 K 线，用于：
        - 填充 closes，以便尽快计算 MA；
        - 让状态机在正式接收实时行情前进入正确状态。

        warmup=True 当前只作为语义标识传入 _on_bar_locked，原逻辑中未单独分支。
        """
        if not isinstance(bars, list) or not bars:
            return
        symbol = ""
        symbols = instance.get("symbols") or []
        if symbols:
            symbol = str(symbols[0])
        for bar in bars:
            # 非 dict 的 warmup 数据无法解析，直接跳过。
            if not isinstance(bar, dict):
                continue
            req = {
                "instance": instance,
                "symbol": str(bar.get("symbol") or symbol),
                "mode": mode,
                "event_time": bar.get("data_time") or bar.get("adjusted_time") or "",
                "bar": bar,
            }
            try:
                with self._lock:
                    self._on_bar_locked(req, warmup=True)
            except Exception as exc:
                # warmup 异常不阻断实例启动，只记录警告。
                logger.warning("warmup bar ignored: %s", exc)

    def _settings(self, request):
        """读取并规范化策略参数。"""
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        # 均线周期至少为 2，避免 period=0/1 等无意义或危险配置。
        ma_period = max(2, _int(params.get("ma_period"), 20))
        # 触碰后等待 bar 数至少为 1。
        max_wait_bars = max(1, _int(params.get("max_wait_bars"), 6))
        return ma_period, max_wait_bars

    def _signal_settings(self, request):
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return {
            "observation_bars": max(1, _int(params.get("observation_bars"), 24)),
            "profit_atr_multiple": max(0.0001, _float(params.get("profit_atr_multiple"), 1.0)),
            "adverse_atr_multiple": max(0.0001, _float(params.get("adverse_atr_multiple"), 0.8)),
        }

    def _ma(self, closes, period):
        """计算最近 period 个收盘价的简单移动平均线 SMA。"""
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period

    def _reset_after_failure(self, state):
        """形态失败后的状态重置。

        重置内容：
        - 回到等待跌破 MA 状态；
        - 清除触碰 K 相关记录；
        - wait_bars 清零；
        - 要求下一次必须 full_break 才能重新确认跌破。
        """
        state.state = WAIT_BREAK_BELOW_MA20
        state.touch_open = None
        state.touch_high = None
        state.touch_ma20 = None
        state.touch_time = ""
        state.wait_bars = 0
        state.reset_requires_full_break = True
        state.break_below_armed = False
        self._clear_signal(state)

    def _reset_after_signal_result(self, state):
        """信号成功/失败/超时结束后，回到等待下一次 MA 跌破。"""
        state.state = WAIT_BREAK_BELOW_MA20
        state.touch_open = None
        state.touch_high = None
        state.touch_ma20 = None
        state.touch_time = ""
        state.wait_bars = 0
        state.reset_requires_full_break = False
        state.break_below_armed = False
        self._clear_signal(state)

    def _clear_signal(self, state):
        state.signal_entry = None
        state.signal_profit_target = None
        state.signal_adverse_target = None
        state.signal_bars = 0
        state.signal_time = ""

    def _start_short_signal(self, state, entry_price, high, low, data_time, request):
        settings = self._signal_settings(request)
        atr = max(0.0001, high - low)
        state.state = SIGNAL_ACTIVE
        state.signal_entry = entry_price
        state.signal_profit_target = entry_price - settings["profit_atr_multiple"] * atr
        state.signal_adverse_target = entry_price + settings["adverse_atr_multiple"] * atr
        state.signal_bars = 0
        state.signal_time = data_time
        return atr

    def _signal_metrics(self, state, ma20, ma_period):
        metrics = self._base_metrics(state, ma20, ma_period)
        metrics.update({
            "signal_entry": state.signal_entry,
            "profit_target": state.signal_profit_target,
            "adverse_target": state.signal_adverse_target,
            "signal_bars": state.signal_bars,
            "signal_time": state.signal_time,
        })
        return metrics

    def _evaluate_active_signal(self, request, state, high, low, close, ma20, ma_period):
        settings = self._signal_settings(request)
        state.signal_bars += 1
        hit_adverse = state.signal_adverse_target is not None and high >= state.signal_adverse_target
        hit_profit = state.signal_profit_target is not None and low <= state.signal_profit_target
        checks = [
            _check("触及止损价", hit_adverse, high, state.signal_adverse_target),
            _check("触及止盈价", hit_profit, low, state.signal_profit_target),
            _check("观察窗口未结束", state.signal_bars < settings["observation_bars"], state.signal_bars, settings["observation_bars"]),
        ]
        if hit_adverse:
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "failure"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "信号失败", 5, "failed", "adverse target hit before profit target", checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, "adverse target hit before profit target", metrics, trace)
        if hit_profit:
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "success"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "信号成功", 5, "passed", "profit target hit before adverse target", checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, "profit target hit before adverse target", metrics, trace)
        if state.signal_bars >= settings["observation_bars"]:
            metrics = self._signal_metrics(state, ma20, ma_period)
            metrics["signal_result"] = "unresolved"
            trace = _trace(request, "bar", "SIGNAL_RESULT", "信号观察结束", 5, "done", "observation window ended without profit/adverse target", checks, metrics)
            self._reset_after_signal_result(state)
            return _no_signal(request, "observation window ended without profit/adverse target", metrics, trace)
        metrics = self._signal_metrics(state, ma20, ma_period)
        trace = _trace(request, "bar", SIGNAL_ACTIVE, "信号观察中", 5, "waiting", "waiting for signal result", checks, metrics)
        return _no_signal(request, "waiting for signal result", metrics, trace)

    def _ma_label(self, period):
        """根据均线周期生成展示标签，例如 period=20 时为 MA20。"""
        return f"MA{period}"

    def _trim_closes(self, state, ma_period):
        """裁剪 closes 序列，避免历史收盘价无限增长。

        需要保留 ma_period + 1 个值，是因为：
        - ma_period 个值用于计算当前 MA；
        - 额外 1 个值有助于比较前一根 bar 的 close/MA 状态。
        """
        keep = max(ma_period + 1, 2)
        if len(state.closes) > keep:
            state.closes = state.closes[-keep:]

    def _base_metrics(self, state, ma20=None, ma_period=20):
        """生成 MA20 策略通用诊断指标。"""
        return {
            "signal": "",
            "state": state.state,
            "ma_period": ma_period,
            "ma": ma20,
            # 保留 ma20 字段名兼容前端；即使 ma_period 不是 20，这里仍沿用原字段。
            "ma20": ma20,
            "touch_open": state.touch_open,
            "touch_high": state.touch_high,
            "touch_ma20": state.touch_ma20,
            "touch_time": state.touch_time,
            "trigger_price": None,
            "wait_bars": state.wait_bars,
            "step_total": 5,
        }

    def _bar_trace(self, request, state, step_key, step_label, step_index, status, reason, checks, ma20=None, ma_period=20):
        """构造 bar 事件的 trace。"""
        return _trace(
            request,
            "bar",
            step_key,
            step_label,
            step_index,
            status,
            reason,
            checks,
            self._base_metrics(state, ma20, ma_period),
        )

    def _tick_trace(self, request, state, status, reason, checks, last_price):
        """构造关键 tick 事件的 trace。

        关键 tick 指：策略已经找到触碰 K，正在等待最新价跌破触碰 K 开盘价。
        """
        ma_period, _ = self._settings(request)
        metrics = self._base_metrics(state, state.touch_ma20, ma_period)
        metrics["trigger_price"] = last_price
        return _trace(
            request,
            "key_tick",
            WAIT_BREAK_TOUCH_OPEN,
            "等待跌破触碰K开盘价",
            4,
            status,
            reason,
            checks,
            metrics,
        )

    def on_bar(self, request):
        """线程安全地处理 K 线事件。"""
        with self._lock:
            return self._on_bar_locked(request, warmup=False)

    def _on_bar_locked(self, request, warmup=False):
        """处理 K 线事件的核心状态机逻辑。

        调用前应已持有 self._lock。

        参数：
            request: K 线请求，预期包含 bar.open/high/low/close。
            warmup: 是否为预热 K 线。当前实现不基于该标志改变行为。
        """
        bar = request.get("bar") or {}
        if not bar:
            return _no_signal(request, "no bar")
        # open/high/low/close 是本策略处理 bar 的最低要求。
        _require_fields(bar, ("open", "high", "low", "close"))

        state = self._state_for(request)
        ma_period, max_wait_bars = self._settings(request)
        ma_label = self._ma_label(ma_period)
        open_price = _float(bar.get("open"))
        high = _float(bar.get("high"))
        low = _float(bar.get("low"))
        close = _float(bar.get("close"))
        data_time = bar.get("data_time") or request.get("event_time", "")
        data_ts = _event_ts(data_time)
        logger.info("Strategy.on_bar: instance_id=%s symbol=%s event_time=%s open=%.4f high=%.4f low=%.4f close=%.4f",
                    _instance_id(request), request.get("symbol", ""), data_time, open_price, high, low, close)

        # 如果能解析出事件时间，则过滤重复或乱序 bar，避免状态机倒退或重复计数。
        if data_ts is not None and state.last_bar_ts is not None and data_ts <= state.last_bar_ts:
            return _no_signal(request, "duplicate or out-of-order bar", self._base_metrics(state, state.prev_ma, ma_period))
        if data_ts is not None:
            state.last_bar_ts = data_ts

        # 先用追加 close 之前的 closes 计算 previous_ma，用于判断上根 K 是否在 MA 上方。
        previous_ma = self._ma(state.closes, ma_period)
        previous_close = state.prev_close
        # 将当前收盘价纳入均线样本，再计算当前 MA。
        state.closes.append(close)
        self._trim_closes(state, ma_period)
        ma20 = self._ma(state.closes, ma_period)

        # MA 样本不足：只能更新 prev_close/prev_ma，并返回等待。
        if ma20 is None:
            state.prev_close = close
            state.prev_ma = ma20
            checks = [
                _check("MA样本数量", len(state.closes) >= ma_period, len(state.closes), ma_period, len(state.closes) - ma_period, "等待收集足够K线计算均线")
            ]
            trace = self._bar_trace(request, state, "WAIT_MA_READY", f"等待 {ma_label} 数据足够", 1, "waiting", "waiting for enough bars", checks, ma20, ma_period)
            return _no_signal(request, "waiting for enough bars", self._base_metrics(state, ma20, ma_period), trace)

        if state.state == SIGNAL_ACTIVE:
            out = self._evaluate_active_signal(request, state, high, low, close, ma20, ma_period)
            state.prev_close = close
            state.prev_ma = ma20
            return out

        # 兼容旧状态：如果已经完成，则回到等待下一轮跌破。
        if state.state == DONE:
            self._reset_after_signal_result(state)
            state.prev_close = close
            state.prev_ma = ma20
            trace = self._bar_trace(request, state, WAIT_BREAK_BELOW_MA20, f"等待跌破 {ma_label}", 1, "waiting", "ready for next attempt", [], ma20, ma_period)
            return _no_signal(request, "ready for next attempt", self._base_metrics(state, ma20, ma_period), trace)

        # 默认 trace 信息。后续每个状态分支会按实际情况覆盖。
        step_key = state.state
        step_label = f"等待跌破 {ma_label}"
        step_index = 1
        status = "waiting"
        reason = "no trade signal"
        checks = []
        full_above = open_price > ma20 and high > ma20 and low > ma20 and close > ma20

        if state.state == WAIT_BREAK_BELOW_MA20:
            if not state.break_below_armed:
                state.break_below_armed = full_above
                checks = [
                    _check(f"整根K线站上{ma_label}", full_above, close, ma20, close - ma20, f"等待出现 OHLC 全部在 {ma_label} 上方的K线")
                ]
                step_key = WAIT_BREAK_BELOW_MA20
                step_label = f"等待先完整站上 {ma_label}"
                step_index = 1
                reason = "waiting for first full bar above MA"
                logger.info(
                    "Strategy.wait_arm: instance_id=%s symbol=%s event_time=%s ma=%.4f full_above=%s armed=%s",
                    _instance_id(request),
                    request.get("symbol", ""),
                    data_time,
                    ma20,
                    full_above,
                    state.break_below_armed,
                )
                state.prev_close = close
                state.prev_ma = ma20
                trace = self._bar_trace(request, state, step_key, step_label, step_index, status, reason, checks, ma20, ma_period)
                return _no_signal(request, reason, self._base_metrics(state, ma20, ma_period), trace)
            # full_break：整根 K 线开盘和收盘都在 MA 下方。
            # 在形态失败重置后，代码要求用 full_break 重新确认跌破，避免在 MA 附近反复误触发。
            full_break = open_price < ma20 and close < ma20
            # cross_break：上根 close 在上根 MA 上方/等于 MA，当前 close 跌到当前 MA 下方。
            # 这是首次确认跌破时使用的主要条件。
            cross_break = previous_close is not None and previous_ma is not None and previous_close >= previous_ma and close < ma20
            checks = [
                _check(f"开盘低于{ma_label}", open_price < ma20, open_price, ma20, open_price - ma20),
                _check(f"收盘低于{ma_label}", close < ma20, close, ma20, close - ma20),
                _check("从上向下跌破", cross_break, close, ma20, close - ma20),
            ]
            if (state.reset_requires_full_break and full_break) or (not state.reset_requires_full_break and cross_break):
                # 跌破确认后进入下一阶段：等待反抽触碰 MA。
                state.state = BROKEN_BELOW_MA20
                state.reset_requires_full_break = False
                step_key = BROKEN_BELOW_MA20
                step_label = f"已跌破，等待反抽触碰 {ma_label}"
                step_index = 2
                status = "passed"
                reason = f"break below {ma_label} confirmed"
            else:
                # 条件未满足，继续停留在等待跌破阶段。
                step_key = WAIT_BREAK_BELOW_MA20
                step_label = f"等待跌破 {ma_label}"
                step_index = 1

        elif state.state == BROKEN_BELOW_MA20:
            # 跌破后，等待价格向上反抽；只要当根 K 的最高价 >= MA，即认为触碰 MA。
            checks = [
                _check(f"最高价触碰{ma_label}", high >= ma20, high, ma20, high - ma20, f"等待反抽到{ma_label}附近")
            ]
            if high >= ma20:
                # 记录触碰 K 的关键信息：后续 tick 跌破 touch_open 时触发 SHORT。
                state.state = WAIT_BREAK_TOUCH_OPEN
                state.touch_open = open_price
                state.touch_high = high
                state.touch_ma20 = ma20
                state.touch_time = data_time
                state.wait_bars = 1
                step_key = WAIT_BREAK_TOUCH_OPEN
                step_label = "等待跌破触碰K开盘价"
                step_index = 3
                status = "passed"
                reason = f"{ma_label} touch bar found"
            else:
                # 还没有反抽触碰 MA，继续等待。
                step_key = BROKEN_BELOW_MA20
                step_label = f"已跌破，等待反抽触碰 {ma_label}"
                step_index = 2

        elif state.state == WAIT_BREAK_TOUCH_OPEN:
            # stood_above：开盘和收盘均在 MA 上方，表示价格重新站上均线，做空形态失效。
            stood_above = open_price > ma20 and close > ma20
            wait_remaining = max_wait_bars - state.wait_bars
            checks = [
                _check(f"未重新站上{ma_label}", not stood_above, close, ma20, close - ma20, "重新站上则形态失败"),
                _check("等待未超时", state.wait_bars < max_wait_bars, state.wait_bars, max_wait_bars, wait_remaining),
                _check("触碰K开盘价有效", state.touch_open is not None, state.touch_open, "not null"),
                _check("K线跌破触碰K开盘价", state.touch_open is not None and low <= state.touch_open, low, state.touch_open),
            ]
            if state.touch_open is not None and low <= state.touch_open:
                atr = self._start_short_signal(state, state.touch_open, high, low, data_time, request)
                metrics = self._signal_metrics(state, ma20, ma_period)
                metrics.update(
                    {
                        "signal": "SHORT",
                        "touch_open": state.touch_open,
                        "touch_high": state.touch_high,
                        "touch_ma20": state.touch_ma20,
                        "touch_time": state.touch_time,
                        "trigger_price": state.touch_open,
                        "entry_price": state.touch_open,
                        "atr": atr,
                    }
                )
                trace = _trace(
                    request,
                    "bar",
                    SHORT_SIGNAL,
                    "做空信号",
                    4,
                    "passed",
                    "SHORT: bar broke below MA touch bar open",
                    checks,
                    metrics,
                    {"target_position": -1, "confidence": 0.8, "signal": "SHORT"},
                )
                state.prev_close = close
                state.prev_ma = ma20
                return {
                    "no_signal": False,
                    "instance_id": _instance_id(request),
                    "symbol": request.get("symbol", ""),
                    "event_time": request.get("event_time", ""),
                    "target_position": -1,
                    "confidence": 0.8,
                    "reason": "SHORT: bar broke below MA touch bar open",
                    "metrics": metrics,
                    "trace": trace,
                }
            if stood_above:
                # 价格重新站上 MA：形态失败，重置状态机。
                self._reset_after_failure(state)
                state.break_below_armed = True
                step_key = WAIT_BREAK_BELOW_MA20
                step_label = f"等待跌破 {ma_label}"
                step_index = 1
                status = "failed"
                reason = f"reset: stood above {ma_label}"
            else:
                # 未重新站上 MA，则等待 bar 数加一。
                state.wait_bars += 1
                if state.wait_bars >= max_wait_bars:
                    # 等待过久仍未由 tick 跌破 touch_open：形态过期，重置。
                    self._reset_after_failure(state)
                    step_key = WAIT_BREAK_BELOW_MA20
                    step_label = f"等待跌破 {ma_label}"
                    step_index = 1
                    status = "failed"
                    reason = "reset: wait bars exceeded"
                else:
                    # 继续等待关键 tick 跌破触碰 K 开盘价。
                    step_key = WAIT_BREAK_TOUCH_OPEN
                    step_label = "等待跌破触碰K开盘价"
                    step_index = 3

        # 处理完当前 bar 后，更新前值，供下一根 bar 判断 cross_break 使用。
        state.prev_close = close
        state.prev_ma = ma20
        trace = self._bar_trace(request, state, step_key, step_label, step_index, status, reason, checks, ma20, ma_period)
        return _no_signal(request, reason, self._base_metrics(state, ma20, ma_period), trace)

    def on_tick(self, request):
        """处理 tick 事件。

        MA20 策略只有在状态机进入 WAIT_BREAK_TOUCH_OPEN 后才关心 tick。
        触发条件是：最新价 last_price < 触碰 K 的开盘价 touch_open。
        """
        with self._lock:
            tick = request.get("tick") or {}
            _require_fields(tick, ("last_price",))
            state = self._state_for(request)
            ma_period, _ = self._settings(request)
            # 尚未找到有效触碰 K 时，tick 不参与判断。
            if state.state != WAIT_BREAK_TOUCH_OPEN or state.touch_open is None:
                return _no_signal(request, "waiting for setup", self._base_metrics(state, None, ma_period))

            last_price = _float(tick.get("last_price"))
            checks = [
                _check("最新价跌破触碰K开盘价", last_price < state.touch_open, last_price, state.touch_open, last_price - state.touch_open)
            ]
            if last_price >= state.touch_open:
                # 最新价还没有跌破触碰 K 开盘价，继续等待。
                trace = self._tick_trace(request, state, "waiting", "touch open not broken", checks, last_price)
                return _no_signal(request, "touch open not broken", self._base_metrics(state, None, ma_period), trace)

            # 触发条件满足：生成 SHORT 信号，并进入信号结果观察。
            atr_high = max(state.touch_high or last_price, last_price, state.touch_open or last_price)
            atr_low = min(state.touch_open or last_price, last_price)
            atr = self._start_short_signal(state, last_price, atr_high, atr_low, request.get("event_time", ""), request)
            metrics = self._signal_metrics(state, state.touch_ma20, ma_period)
            metrics.update(
                {
                    "signal": "SHORT",
                    "touch_open": state.touch_open,
                    "touch_high": state.touch_high,
                    "touch_ma20": state.touch_ma20,
                    "touch_time": state.touch_time,
                    "trigger_price": last_price,
                    "entry_price": last_price,
                    "atr": atr,
                }
            )
            trace = _trace(
                request,
                "key_tick",
                SHORT_SIGNAL,
                "做空信号",
                5,
                "passed",
                "SHORT: tick broke below MA touch bar open",
                checks,
                metrics,
                {"target_position": -1, "confidence": 0.8, "signal": "SHORT"},
            )
            return {
                "no_signal": False,
                "instance_id": _instance_id(request),
                "symbol": request.get("symbol", ""),
                "event_time": request.get("event_time", ""),
                "target_position": -1,
                "confidence": 0.8,
                "reason": "SHORT: tick broke below MA touch bar open",
                "metrics": metrics,
                "trace": trace,
            }

    def on_replay_bar(self, request):
        """回放 K 线事件复用实时 K 线逻辑。"""
        return self.on_bar(request)


class MA20WeakBaselineStrategy(MA20PullbackShortStrategy):
    """The baseline MA20 break-touch-break strategy as a selectable variant."""

    def __init__(self):
        definition = dict(MA20PullbackShortStrategy.definition)
        definition.update({
            "strategy_id": MA20_WEAK_BASELINE_STRATEGY_ID,
            "display_name": "MA20 Weak Pullback Baseline",
            "default_params": {
                "ma_period": 20,
                "max_wait_bars": 6,
                "observation_bars": 24,
                "profit_atr_multiple": 1.0,
                "adverse_atr_multiple": 0.8,
                "algorithm": "baseline",
            },
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })
        super().__init__(definition=definition)


class MA20WeakPullbackShortStrategy(Strategy):
    """Automated MA20 weak pullback short strategy.

    The strategy rejects strong uptrend pullbacks first, then requires structure
    break before waiting for a weak MA20 touch and a break of the reaction low.
    It is bar-driven so historical replay and backtests can reproduce entries.
    """

    definition = {
        "strategy_id": MA20_WEAK_STRATEGY_ID,
        "display_name": "MA20 Weak Pullback Short",
        "entry_script": "python/strategy_service.py",
        "version": "1.0.0",
        "default_params": {
            "ma20_period": 20,
            "ma60_period": 60,
            "ma120_period": 120,
            "atr_period": 14,
            "observation_bars": 24,
            "profit_atr_multiple": 1.0,
            "adverse_atr_multiple": 0.8,
            "swing_lookback_bars": 20,
            "slope_lookback_bars": 5,
            "structure_wait_bars": 3,
            "touch_wait_bars": 12,
            "trigger_wait_bars": 6,
            "use_score_filter": True,
        },
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    def __init__(self, definition=None, use_score_filter=None):
        if definition is not None:
            self.definition = definition
        if use_score_filter is not None:
            default_params = dict(self.definition.get("default_params") or {})
            default_params["use_score_filter"] = bool(use_score_filter)
            self.definition = {**self.definition, "default_params": default_params}
        self.states = {}
        self._lock = RLock()

    def start_instance(self, instance):
        instance_id = instance.get("instance_id", "")
        symbols = instance.get("symbols") or [""]
        mode = "replay" if str(instance.get("mode") or "").strip().lower() == "replay" else "live"
        with self._lock:
            for symbol in symbols:
                self.states[(mode, instance_id, str(symbol))] = WeakPullbackShortState()
        self._apply_warmup_bars(instance, (instance.get("params") or {}).get("warmup_bars") or [], mode)

    def stop_instance(self, instance_id):
        with self._lock:
            for key in list(self.states):
                if len(key) >= 2 and key[1] == instance_id:
                    del self.states[key]

    def _state_for(self, request):
        key = (_mode_key(request), _instance_id(request), request.get("symbol", ""))
        if key not in self.states:
            self.states[key] = WeakPullbackShortState()
        return self.states[key]

    def _settings(self, request):
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return {
            "ma20": max(2, _int(params.get("ma20_period"), _int(params.get("ma_period"), 20))),
            "ma60": max(3, _int(params.get("ma60_period"), 60)),
            "ma120": max(4, _int(params.get("ma120_period"), 120)),
            "atr": max(2, _int(params.get("atr_period"), 14)),
            "swing": max(2, _int(params.get("swing_lookback_bars"), 20)),
            "slope": max(1, _int(params.get("slope_lookback_bars"), 5)),
            "structure_wait": max(1, _int(params.get("structure_wait_bars"), 3)),
            "touch_wait": max(1, _int(params.get("touch_wait_bars"), 12)),
            "trigger_wait": max(1, _int(params.get("trigger_wait_bars"), 6)),
            "use_score_filter": bool(params.get("use_score_filter", True)),
        }

    def _apply_warmup_bars(self, instance, bars, mode):
        instance_id = instance.get("instance_id", "")
        params = instance.get("params") or {}
        for idx, bar in enumerate(bars):
            try:
                symbol = str(bar.get("symbol") or (instance.get("symbols") or [""])[0])
                req = {
                    "instance": {
                        "instance_id": instance_id,
                        "strategy_id": self.definition["strategy_id"],
                        "timeframe": instance.get("timeframe", ""),
                        "params": params,
                    },
                    "symbol": symbol,
                    "mode": mode,
                    "event_time": bar.get("data_time") or bar.get("event_time") or f"warmup-{idx}",
                    "bar": bar,
                }
                self._on_bar_locked(req)
            except Exception as exc:
                logger.warning("weak pullback warmup bar ignored: %s", exc)

    def _sma(self, values, period):
        if len(values) < period:
            return None
        return sum(values[-period:]) / period

    def _slope(self, values, period, lookback):
        if len(values) < period + lookback:
            return 0.0
        now = sum(values[-period:]) / period
        prev = sum(values[-period - lookback:-lookback]) / period
        return (now - prev) / lookback

    def _atr(self, state, period):
        if len(state.closes) < period + 1:
            return None
        trs = []
        start = len(state.closes) - period
        for idx in range(start, len(state.closes)):
            prev_close = state.closes[idx - 1] if idx > 0 else state.closes[idx]
            trs.append(max(
                state.highs[idx] - state.lows[idx],
                abs(state.highs[idx] - prev_close),
                abs(state.lows[idx] - prev_close),
            ))
        return sum(trs) / len(trs)

    def _last_swing_low(self, state, lookback):
        if len(state.lows) < 2:
            return None
        prior = state.lows[:-1]
        return min(prior[-lookback:]) if prior else None

    def _indicators(self, state, settings):
        ma20 = self._sma(state.closes, settings["ma20"])
        ma60 = self._sma(state.closes, settings["ma60"])
        ma120 = self._sma(state.closes, settings["ma120"])
        return {
            "ma20": ma20,
            "ma60": ma60,
            "ma120": ma120,
            "ma20_slope": self._slope(state.closes, settings["ma20"], settings["slope"]),
            "ma60_slope": self._slope(state.closes, settings["ma60"], settings["slope"]),
            "ma120_slope": self._slope(state.closes, settings["ma120"], settings["slope"]),
            "atr14": self._atr(state, settings["atr"]),
            "last_swing_low": self._last_swing_low(state, settings["swing"]),
        }

    def _trim(self, state, settings):
        keep = max(settings["ma120"] + settings["slope"] + 2, settings["swing"] + 2, settings["atr"] + 2)
        for name in ("opens", "highs", "lows", "closes"):
            values = getattr(state, name)
            if len(values) > keep:
                setattr(state, name, values[-keep:])

    def _base_metrics(self, state, ind=None):
        ind = ind or {}
        return {
            "signal": "",
            "state": state.state,
            "regime": state.regime,
            "ma20": ind.get("ma20"),
            "ma60": ind.get("ma60"),
            "ma120": ind.get("ma120"),
            "ma20_slope": ind.get("ma20_slope"),
            "ma60_slope": ind.get("ma60_slope"),
            "ma120_slope": ind.get("ma120_slope"),
            "atr14": ind.get("atr14"),
            "last_swing_low": ind.get("last_swing_low"),
            "reaction_low": state.reaction_low,
            "touch_open": state.touch_open,
            "touch_high": state.touch_high,
            "touch_ma20": state.touch_ma20,
            "touch_time": state.touch_time,
            "bullish_pause_score": state.bullish_pause_score,
            "bearish_failure_score": state.bearish_failure_score,
            "bars_since_break": state.bars_since_break,
            "trigger_wait_bars": state.trigger_wait_bars,
            "signal_entry": state.signal_entry,
            "profit_target": state.signal_profit_target,
            "adverse_target": state.signal_adverse_target,
            "signal_bars": state.signal_bars,
            "signal_time": state.signal_time,
            "step_total": 6,
        }

    def _bar_trace(self, request, state, key, label, index, status, reason, checks, ind=None, preview=None):
        return _trace(request, "bar", key, label, index, status, reason, checks, self._base_metrics(state, ind), preview)

    def _reset(self, state):
        state.state = WAIT_BREAK_BELOW_MA20
        state.break_time = ""
        state.break_index = -1
        state.bars_since_break = 0
        state.reaction_low = None
        state.touch_open = None
        state.touch_high = None
        state.touch_ma20 = None
        state.touch_time = ""
        state.trigger_wait_bars = 0
        state.regime = ""
        state.bullish_pause_score = 0.0
        state.bearish_failure_score = 0.0
        self._clear_signal(state)

    def _clear_signal(self, state):
        state.signal_entry = None
        state.signal_profit_target = None
        state.signal_adverse_target = None
        state.signal_bars = 0
        state.signal_time = ""

    def _signal_settings(self, request):
        params = dict(self.definition.get("default_params") or {})
        params.update(_params(request))
        return {
            "observation_bars": max(1, _int(params.get("observation_bars"), 24)),
            "profit_atr_multiple": max(0.0001, _float(params.get("profit_atr_multiple"), 1.0)),
            "adverse_atr_multiple": max(0.0001, _float(params.get("adverse_atr_multiple"), 0.8)),
        }

    def _start_short_signal(self, state, entry_price, atr, data_time, request):
        settings = self._signal_settings(request)
        atr = max(0.0001, atr or 0.0)
        state.state = SIGNAL_ACTIVE
        state.signal_entry = entry_price
        state.signal_profit_target = entry_price - settings["profit_atr_multiple"] * atr
        state.signal_adverse_target = entry_price + settings["adverse_atr_multiple"] * atr
        state.signal_bars = 0
        state.signal_time = data_time
        return atr

    def _evaluate_active_signal(self, request, state, high, low, ind):
        settings = self._signal_settings(request)
        state.signal_bars += 1
        hit_adverse = state.signal_adverse_target is not None and high >= state.signal_adverse_target
        hit_profit = state.signal_profit_target is not None and low <= state.signal_profit_target
        checks = [
            _check("触及止损价", hit_adverse, high, state.signal_adverse_target),
            _check("触及止盈价", hit_profit, low, state.signal_profit_target),
            _check("观察窗口未结束", state.signal_bars < settings["observation_bars"], state.signal_bars, settings["observation_bars"]),
        ]
        if hit_adverse:
            metrics = self._base_metrics(state, ind)
            metrics["signal_result"] = "failure"
            trace = self._bar_trace(request, state, "SIGNAL_RESULT", "信号失败", 6, "failed", "adverse target hit before profit target", checks, ind)
            self._reset(state)
            return _no_signal(request, "adverse target hit before profit target", metrics, trace)
        if hit_profit:
            metrics = self._base_metrics(state, ind)
            metrics["signal_result"] = "success"
            trace = self._bar_trace(request, state, "SIGNAL_RESULT", "信号成功", 6, "passed", "profit target hit before adverse target", checks, ind)
            self._reset(state)
            return _no_signal(request, "profit target hit before adverse target", metrics, trace)
        if state.signal_bars >= settings["observation_bars"]:
            metrics = self._base_metrics(state, ind)
            metrics["signal_result"] = "unresolved"
            trace = self._bar_trace(request, state, "SIGNAL_RESULT", "信号观察结束", 6, "done", "observation window ended without profit/adverse target", checks, ind)
            self._reset(state)
            return _no_signal(request, "observation window ended without profit/adverse target", metrics, trace)
        trace = self._bar_trace(request, state, SIGNAL_ACTIVE, "信号观察中", 6, "waiting", "waiting for signal result", checks, ind)
        return _no_signal(request, "waiting for signal result", self._base_metrics(state, ind), trace)

    def _strong_uptrend(self, close, ind):
        ma20, ma60, ma120 = ind.get("ma20"), ind.get("ma60"), ind.get("ma120")
        return (
            ma20 is not None and ma60 is not None and ma120 is not None
            and ma20 > ma60 > ma120
            and ind.get("ma60_slope", 0) > 0
            and ind.get("ma120_slope", 0) > 0
            and close > ma60
        )

    def _structure_broken(self, close, ind):
        last_swing_low = ind.get("last_swing_low")
        return last_swing_low is not None and close < last_swing_low

    def _classify_regime(self, close, ind):
        if self._structure_broken(close, ind) and ind.get("ma20_slope", 0) < 0:
            return "BEARISH_REVERSAL_CANDIDATE"
        ma60 = ind.get("ma60")
        if ma60 is not None and close < ma60 and ind.get("ma20_slope", 0) < 0 and ind.get("ma60_slope", 0) <= 0:
            return "WEAK_BEARISH_CANDIDATE"
        return "UNCLEAR"

    def _scores(self, state, open_price, low, close, ind):
        bull = 0.0
        bear = 0.0
        if self._strong_uptrend(close, ind):
            bull += 2
        ma20, ma60, ma120 = ind.get("ma20"), ind.get("ma60"), ind.get("ma120")
        if ma20 is not None and ma60 is not None and ma120 is not None and ma20 > ma60 > ma120:
            bull += 1
        structure = self._structure_broken(close, ind)
        if structure:
            bear += 2
        else:
            bull += 2
        atr = ind.get("atr14") or 0
        if ma20 is not None and atr > 0:
            break_strength = ma20 - close
            if break_strength < 0.5 * atr:
                bull += 1
            else:
                bear += 1
        if state.bars_since_break <= 3 and ma20 is not None and close > ma20:
            bull += 1
        if ind.get("ma20_slope", 0) < 0:
            bear += 1
        if ind.get("ma60_slope", 0) <= 0:
            bear += 1
        if ma20 is not None and close < ma20:
            bear += 1
        if state.reaction_low is not None and low <= state.reaction_low:
            bear += 2
        if close > open_price:
            bull += 1
        elif close < open_price:
            bear += 1
        state.bullish_pause_score = bull
        state.bearish_failure_score = bear
        return bull, bear

    def _handle_structure_filter(self, request, state, open_price, low, close, ind, settings):
        strong_up = self._strong_uptrend(close, ind)
        structure = self._structure_broken(close, ind)
        bull, bear = self._scores(state, open_price, low, close, ind)
        checks = [
            _check("不是强上涨回撤", not (strong_up and not structure), close, ind.get("ma60")),
            _check("跌破最近结构低点", structure, close, ind.get("last_swing_low")),
            _check("结构等待未超时", state.bars_since_break <= settings["structure_wait"], state.bars_since_break, settings["structure_wait"]),
        ]
        if strong_up and not structure:
            reason = "strong uptrend pullback, ignore MA20 break"
            self._reset(state)
            trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "趋势/结构过滤", 2, "failed", reason, checks, ind)
            return _no_signal(request, reason, self._base_metrics(state, ind), trace)
        if not structure:
            if close > ind.get("ma20"):
                reason = "BULLISH_PAUSE: fast reclaim above MA20"
                self._reset(state)
                trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "快速收回 MA20", 2, "failed", reason, checks, ind)
                return _no_signal(request, reason, self._base_metrics(state, ind), trace)
            if state.bars_since_break >= settings["structure_wait"]:
                reason = "structure break not confirmed"
                self._reset(state)
                trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "结构破坏未确认", 2, "failed", reason, checks, ind)
                return _no_signal(request, reason, self._base_metrics(state, ind), trace)
            state.state = TREND_STRUCTURE_FILTER
            trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "等待破坏上涨结构", 2, "waiting", "waiting for swing low break", checks, ind)
            return _no_signal(request, "waiting for swing low break", self._base_metrics(state, ind), trace)
        regime = self._classify_regime(close, ind)
        state.regime = regime
        checks.append(_check("属于弱势或转空候选", regime in ("BEARISH_REVERSAL_CANDIDATE", "WEAK_BEARISH_CANDIDATE"), regime, "bearish candidate"))
        if regime not in ("BEARISH_REVERSAL_CANDIDATE", "WEAK_BEARISH_CANDIDATE"):
            reason = "not bearish regime"
            self._reset(state)
            trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "趋势/结构过滤", 2, "failed", reason, checks, ind)
            return _no_signal(request, reason, self._base_metrics(state, ind), trace)
        state.state = WAIT_PULLBACK_TOUCH_MA20
        trace = self._bar_trace(request, state, TREND_STRUCTURE_FILTER, "趋势/结构过滤通过", 2, "passed", regime, checks, ind)
        return _no_signal(request, regime, self._base_metrics(state, ind), trace)

    def on_bar(self, request):
        with self._lock:
            return self._on_bar_locked(request)

    def _on_bar_locked(self, request):
        bar = request.get("bar") or {}
        if not bar:
            return _no_signal(request, "no bar")
        _require_fields(bar, ("open", "high", "low", "close"))
        state = self._state_for(request)
        settings = self._settings(request)
        open_price = _float(bar.get("open"))
        high = _float(bar.get("high"))
        low = _float(bar.get("low"))
        close = _float(bar.get("close"))
        data_time = bar.get("data_time") or request.get("event_time", "")
        logger.info(
            "WeakStrategy.on_bar: instance_id=%s symbol=%s event_time=%s open=%.4f high=%.4f low=%.4f close=%.4f",
            _instance_id(request),
            request.get("symbol", ""),
            data_time,
            open_price,
            high,
            low,
            close,
        )
        data_ts = _event_ts(data_time)
        if data_ts is not None and state.last_bar_ts is not None and data_ts <= state.last_bar_ts:
            return _no_signal(request, "duplicate or out-of-order bar", self._base_metrics(state))
        if data_ts is not None:
            state.last_bar_ts = data_ts

        previous_ma20 = self._sma(state.closes, settings["ma20"])
        previous_close = state.prev_close
        state.opens.append(open_price)
        state.highs.append(high)
        state.lows.append(low)
        state.closes.append(close)
        self._trim(state, settings)
        ind = self._indicators(state, settings)
        ma20 = ind.get("ma20")
        if ind.get("ma120") is None or ma20 is None:
            state.prev_close = close
            state.prev_ma20 = ma20
            checks = [_check("MA120样本数量", len(state.closes) >= settings["ma120"], len(state.closes), settings["ma120"])]
            trace = self._bar_trace(request, state, "WAIT_MA_READY", "等待 MA20/MA60/MA120 数据足够", 1, "waiting", "waiting for enough bars", checks, ind)
            return _no_signal(request, "waiting for enough bars", self._base_metrics(state, ind), trace)

        def finish(out):
            state.prev_close = close
            state.prev_ma20 = ma20
            return out

        if state.state == SIGNAL_ACTIVE:
            return finish(self._evaluate_active_signal(request, state, high, low, ind))

        if state.state == DONE:
            self._reset(state)
            trace = self._bar_trace(request, state, WAIT_BREAK_BELOW_MA20, "等待跌破 MA20", 1, "waiting", "ready for next attempt", [], ind)
            return finish(_no_signal(request, "ready for next attempt", self._base_metrics(state, ind), trace))

        if state.state == WAIT_BREAK_BELOW_MA20:
            cross_break = previous_close is not None and previous_ma20 is not None and previous_close >= previous_ma20 and close < ma20
            checks = [
                _check("从上向下跌破MA20", cross_break, close, ma20, close - ma20),
                _check("上一根收盘在MA20上方", previous_close is not None and previous_ma20 is not None and previous_close >= previous_ma20, previous_close, previous_ma20),
            ]
            if not cross_break:
                trace = self._bar_trace(request, state, WAIT_BREAK_BELOW_MA20, "等待跌破 MA20", 1, "waiting", "waiting for MA20 cross break", checks, ind)
                return finish(_no_signal(request, "waiting for MA20 cross break", self._base_metrics(state, ind), trace))
            state.break_time = data_time
            state.break_index = len(state.closes) - 1
            state.bars_since_break = 1
            state.reaction_low = low
            out = self._handle_structure_filter(request, state, open_price, low, close, ind, settings)
            return finish(out)

        if state.state == TREND_STRUCTURE_FILTER:
            state.bars_since_break += 1
            state.reaction_low = low if state.reaction_low is None else min(state.reaction_low, low)
            out = self._handle_structure_filter(request, state, open_price, low, close, ind, settings)
            return finish(out)

        if state.state == WAIT_PULLBACK_TOUCH_MA20:
            state.bars_since_break += 1
            state.reaction_low = low if state.reaction_low is None else min(state.reaction_low, low)
            stood_above = open_price > ma20 and close > ma20
            wait_used = max(0, state.bars_since_break - settings["structure_wait"])
            checks = [
                _check("未重新站上MA20", not stood_above, close, ma20, close - ma20),
                _check("反抽等待未超时", wait_used <= settings["touch_wait"], wait_used, settings["touch_wait"]),
                _check("最高价触碰MA20", high >= ma20, high, ma20, high - ma20),
            ]
            if stood_above:
                reason = "reset: stood above MA20 before weak pullback"
                self._reset(state)
                trace = self._bar_trace(request, state, WAIT_PULLBACK_TOUCH_MA20, "等待弱反触碰 MA20", 3, "failed", reason, checks, ind)
                return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
            if wait_used > settings["touch_wait"]:
                reason = "weak pullback touch timeout"
                self._reset(state)
                trace = self._bar_trace(request, state, WAIT_PULLBACK_TOUCH_MA20, "等待弱反触碰 MA20", 3, "failed", reason, checks, ind)
                return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
            if high >= ma20:
                state.state = WAIT_BREAK_REACTION_LOW
                state.touch_open = open_price
                state.touch_high = high
                state.touch_ma20 = ma20
                state.touch_time = data_time
                state.trigger_wait_bars = 0
                trace = self._bar_trace(request, state, WAIT_PULLBACK_TOUCH_MA20, "弱反触碰 MA20", 3, "passed", "weak pullback touched MA20", checks, ind)
                return finish(_no_signal(request, "weak pullback touched MA20", self._base_metrics(state, ind), trace))
            trace = self._bar_trace(request, state, WAIT_PULLBACK_TOUCH_MA20, "等待弱反触碰 MA20", 3, "waiting", "waiting for weak pullback touch", checks, ind)
            return finish(_no_signal(request, "waiting for weak pullback touch", self._base_metrics(state, ind), trace))

        if state.state == WAIT_BREAK_REACTION_LOW:
            state.trigger_wait_bars += 1
            stood_above = open_price > ma20 and close > ma20
            bull, bear = self._scores(state, open_price, low, close, ind)
            checks = [
                _check("未重新站上MA20", not stood_above, close, ma20, close - ma20),
                _check("触发等待未超时", state.trigger_wait_bars <= settings["trigger_wait"], state.trigger_wait_bars, settings["trigger_wait"]),
                _check("跌破反抽低点", state.reaction_low is not None and low <= state.reaction_low, low, state.reaction_low),
                _check("空头评分不低于多头停顿", (not settings["use_score_filter"]) or bear >= bull, bear, bull),
            ]
            if stood_above:
                reason = "reset: stood above MA20 before reaction low break"
                self._reset(state)
                trace = self._bar_trace(request, state, WAIT_BREAK_REACTION_LOW, "等待跌破反抽低点", 4, "failed", reason, checks, ind)
                return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
            if state.trigger_wait_bars > settings["trigger_wait"]:
                reason = "reaction low break timeout"
                self._reset(state)
                trace = self._bar_trace(request, state, WAIT_BREAK_REACTION_LOW, "等待跌破反抽低点", 4, "failed", reason, checks, ind)
                return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
            if state.reaction_low is not None and low <= state.reaction_low:
                if settings["use_score_filter"] and bull > bear:
                    reason = "bullish pause score exceeds bearish failure score"
                    self._reset(state)
                    trace = self._bar_trace(request, state, WAIT_BREAK_REACTION_LOW, "评分过滤", 4, "failed", reason, checks, ind)
                    return finish(_no_signal(request, reason, self._base_metrics(state, ind), trace))
                atr = self._start_short_signal(state, state.reaction_low, ind.get("atr14") or (high - low), data_time, request)
                metrics = self._base_metrics(state, ind)
                metrics.update({
                    "signal": "SHORT",
                    "entry_price": state.reaction_low,
                    "trigger_price": state.reaction_low,
                    "reaction_low": state.reaction_low,
                    "atr14": atr,
                })
                trace = _trace(
                    request,
                    "bar",
                    SHORT_SIGNAL,
                    "做空信号",
                    5,
                    "passed",
                    "SHORT: broke weak pullback reaction low",
                    checks,
                    metrics,
                    {"target_position": -1, "confidence": 0.82, "signal": "SHORT"},
                )
                return finish({
                    "no_signal": False,
                    "instance_id": _instance_id(request),
                    "symbol": request.get("symbol", ""),
                    "event_time": request.get("event_time", ""),
                    "target_position": -1,
                    "confidence": 0.82,
                    "reason": "SHORT: broke weak pullback reaction low",
                    "metrics": metrics,
                    "trace": trace,
                })
            trace = self._bar_trace(request, state, WAIT_BREAK_REACTION_LOW, "等待跌破反抽低点", 4, "waiting", "waiting for reaction low break", checks, ind)
            return finish(_no_signal(request, "waiting for reaction low break", self._base_metrics(state, ind), trace))

        return finish(_no_signal(request, "unknown state", self._base_metrics(state, ind)))

    def on_tick(self, request):
        state = self._state_for(request)
        return _no_signal(request, "bar driven strategy ignores ticks", self._base_metrics(state))

    def on_replay_bar(self, request):
        return self.on_bar(request)


class MA20WeakPullbackVariantStrategy(MA20WeakPullbackShortStrategy):
    """Hard/score filtered weak pullback variants exposed as separate strategies."""

    def __init__(self, strategy_id, display_name, algorithm, use_score_filter):
        definition = dict(MA20WeakPullbackShortStrategy.definition)
        default_params = dict(definition.get("default_params") or {})
        default_params.update({
            "algorithm": algorithm,
            "use_score_filter": bool(use_score_filter),
        })
        definition.update({
            "strategy_id": strategy_id,
            "display_name": display_name,
            "default_params": default_params,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })
        super().__init__(definition=definition, use_score_filter=use_score_filter)


# -----------------------------------------------------------------------------
# 策略服务门面：把 gRPC 方法映射到具体策略
# -----------------------------------------------------------------------------
class StrategyService:
    """策略服务对象。

    该类不直接依赖 grpc，而是提供普通 Python 方法；
    add_unary 会把这些方法包装成 gRPC unary-unary handler。
    """

    def __init__(self):
        """注册内置策略实例。"""
        _ensure_logging_configured()
        self.strategies = {
            "sample.momentum": SampleMomentumStrategy(),
            "ma20.pullback_short": MA20PullbackShortStrategy(),
            MA20_WEAK_STRATEGY_ID: MA20WeakPullbackShortStrategy(),
            MA20_WEAK_BASELINE_STRATEGY_ID: build_ma20_weak_baseline_strategy(sys.modules[__name__]),
            MA20_WEAK_HARD_FILTER_STRATEGY_ID: build_ma20_weak_hard_filter_strategy(sys.modules[__name__]),
            MA20_WEAK_SCORE_FILTER_STRATEGY_ID: build_ma20_weak_score_filter_strategy(sys.modules[__name__]),
        }
        logger.info("strategy registry initialized2: %s", ",".join(sorted(self.strategies)))

    def Ping(self, request, context):
        """健康检查接口。"""
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def ListStrategies(self, request, context):
        """返回当前服务支持的策略元数据列表。"""
        return {
            "strategies": [strategy.definition for strategy in self.strategies.values()]
        }

    def LoadStrategy(self, request, context):
        """加载/校验指定策略。

        当前实现中的策略均已在 __init__ 注册，因此这里只校验 strategy_id 是否存在。
        """
        strategy_id = request.get("strategy_id", "")
        if strategy_id not in self.strategies:
            raise ValueError(f"unknown strategy_id: {strategy_id}")
        logger.info("strategy loaded: %s", strategy_id)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StartInstance(self, request, context):
        """启动策略实例，并把 instance 交给对应策略初始化。"""
        instance = request.get("instance") or {}
        strategy = self._strategy_for_instance(instance)
        strategy.start_instance(instance)
        logger.info(
            "strategy instance started2: instance_id=%s strategy_id=%s symbols=%s mode=%s",
            instance.get("instance_id", ""),
            instance.get("strategy_id", ""),
            ",".join(str(item) for item in (instance.get("symbols") or [])),
            instance.get("mode", ""),
        )
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StopInstance(self, request, context):
        """停止策略实例。

        为简化实现，这里会遍历所有策略并让它们清理该 instance_id 下的状态。
        """
        instance_id = request.get("instance_id", "")
        for strategy in self.strategies.values():
            strategy.stop_instance(instance_id)
        logger.info("strategy instance stopped: instance_id=%s", instance_id)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def OnTick(self, request, context):
        """接收 tick 事件并分发给请求中指定的策略。"""
        return self._run_and_log_phase(request, self._strategy_for_request(request).on_tick)

    def OnBar(self, request, context):
        """接收实时 K 线事件并分发给请求中指定的策略。"""
        return self._run_and_log_phase(request, self._strategy_for_request(request).on_bar)

    def OnReplayBar(self, request, context):
        """接收回放 K 线事件并分发给请求中指定的策略。"""
        return self._run_and_log_phase(request, self._strategy_for_request(request).on_replay_bar)

    def RunBacktest(self, request, context):
        """运行回测的占位接口。

        当前只返回固定 mock 结果，表示接口链路可用；
        若要接入真实回测，需要在这里调用回测引擎并返回 equity_curve/trades 等结果。
        """
        return {
            "run_id": request.get("run_id", ""),
            "status": "done",
            "summary": {"trades": 0, "pnl": 0, "note": "mock backtest stub"},
            "result": {"equity_curve": [], "trades": []},
        }

    def GetBacktestResult(self, request, context):
        """查询回测结果的占位接口。"""
        return {
            "run_id": request.get("run_id", ""),
            "status": "done",
            "summary": {"note": "mock backtest result stub"},
            "result": {"equity_curve": [], "trades": []},
        }

    def RunParameterSweep(self, request, context):
        """参数扫描/优化的占位接口。

        request.grid 预期是一个参数名到候选值列表的 dict。
        当前 summary.candidates 简单统计所有列表长度之和，并非笛卡尔积组合数量。
        """
        return {
            "run_id": f"opt-{int(time.time() * 1000)}",
            "status": "done",
            "summary": {"candidates": sum(len(v) for v in request.get("grid", {}).values()), "note": "mock optimizer stub"},
        }

    def _strategy_for_instance(self, instance):
        """根据 instance.strategy_id 获取策略对象。"""
        strategy_id = instance.get("strategy_id", "")
        strategy = self.strategies.get(strategy_id)
        if strategy is None:
            raise ValueError(f"unknown strategy_id: {strategy_id}")
        return strategy

    def _strategy_for_request(self, request):
        """从请求的 instance 字段中解析并获取策略对象。"""
        return self._strategy_for_instance(request.get("instance") or {})

    def _run_and_log_phase(self, request, fn):
        out = fn(request)
        _log_strategy_phase(request, out)
        return out


# -----------------------------------------------------------------------------
# gRPC handler 构建
# -----------------------------------------------------------------------------
def add_unary(handler, service_name, method_name, fn):
    """把普通 Python 方法包装成 gRPC unary-unary handler。

    参数：
        handler: 保留参数，当前实现未使用。调用处传入 service。
        service_name: gRPC 服务名，例如 "strategy.Runtime"。
        method_name: gRPC 方法名，例如 "OnBar"。
        fn: 真正处理请求的 Python 函数。

    工作流程：
    1. gRPC 收到 bytes 请求；
    2. invoke 使用 _loads 转成 dict；
    3. 调用 fn(dict, context)；
    4. 使用 _dumps 将返回 dict 转成 JSON bytes。

    异常处理：
    - ValueError：视为请求参数错误，返回 INVALID_ARGUMENT；
    - 其他异常：视为服务内部错误，返回 INTERNAL。
    """
    if grpc is None:
        raise RuntimeError("grpcio is required to run the strategy service")
    full_name = f"{service_name}/{method_name}"

    def invoke(request, context):
        """gRPC 实际调用的内部函数。"""
        try:
            return fn(_loads(request), context)
        except ValueError as exc:
            logger.warning("strategy request rejected: %s: %s", full_name, exc)
            if context is not None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            raise
        except Exception as exc:
            logger.exception("strategy request failed: %s", full_name)
            if context is not None:
                context.abort(grpc.StatusCode.INTERNAL, str(exc))
            raise

    return grpc.unary_unary_rpc_method_handler(
        invoke,
        # 请求反序列化器不做 protobuf 解析，直接把 bytes 交给 invoke。
        request_deserializer=lambda b: b,
        # 响应序列化器将 dict 转为 JSON bytes。
        response_serializer=_dumps,
    )


def build_server():
    """创建并配置 gRPC server。"""
    if grpc is None:
        raise RuntimeError("grpcio is required to run the strategy service")
    _ensure_logging_configured()
    service = StrategyService()
    # 使用最多 8 个工作线程并发处理 unary 请求。
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    # 使用 generic handler 手动注册服务与方法，无需 protobuf 生成代码。
    server.add_generic_rpc_handlers(
        (
            # 健康检查服务。
            grpc.method_handlers_generic_handler(
                "strategy.Health",
                {"Ping": add_unary(service, "strategy.Health", "Ping", service.Ping)},
            ),
            # 策略注册表服务。
            grpc.method_handlers_generic_handler(
                "strategy.Registry",
                {"ListStrategies": add_unary(service, "strategy.Registry", "ListStrategies", service.ListStrategies)},
            ),
            # 策略运行时服务：加载、启动、停止、接收行情事件。
            grpc.method_handlers_generic_handler(
                "strategy.Runtime",
                {
                    "LoadStrategy": add_unary(service, "strategy.Runtime", "LoadStrategy", service.LoadStrategy),
                    "StartInstance": add_unary(service, "strategy.Runtime", "StartInstance", service.StartInstance),
                    "StopInstance": add_unary(service, "strategy.Runtime", "StopInstance", service.StopInstance),
                    "OnTick": add_unary(service, "strategy.Runtime", "OnTick", service.OnTick),
                    "OnBar": add_unary(service, "strategy.Runtime", "OnBar", service.OnBar),
                    "OnReplayBar": add_unary(service, "strategy.Runtime", "OnReplayBar", service.OnReplayBar),
                },
            ),
            # 回测服务，目前为 mock/stub。
            grpc.method_handlers_generic_handler(
                "strategy.Backtest",
                {
                    "RunBacktest": add_unary(service, "strategy.Backtest", "RunBacktest", service.RunBacktest),
                    "GetBacktestResult": add_unary(service, "strategy.Backtest", "GetBacktestResult", service.GetBacktestResult),
                },
            ),
            # 参数优化服务，目前为 mock/stub。
            grpc.method_handlers_generic_handler(
                "strategy.Optimizer",
                {"RunParameterSweep": add_unary(service, "strategy.Optimizer", "RunParameterSweep", service.RunParameterSweep)},
            ),
        )
    )
    return server


# -----------------------------------------------------------------------------
# 命令行入口
# -----------------------------------------------------------------------------
def main():
    """命令行启动入口。

    示例：
        python strategy_service.py --addr 127.0.0.1:50051
    """
    # 解析监听地址，默认只监听本机 127.0.0.1:50051。
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:50051")
    parser.add_argument("--log-file", default=DEFAULT_STRATEGY_LOG_FILE)
    parser.add_argument("--log-level", default=os.environ.get("STRATEGY_LOG_LEVEL", "INFO"))
    args = parser.parse_args()
    _configure_logging(args.log_file, args.log_level)
    # 创建 gRPC server，绑定端口并启动。
    server = build_server()
    bound_port = server.add_insecure_port(args.addr)
    if bound_port == 0:
        logger.error("strategy service bind failed: addr=%s log_file=%s", args.addr, args.log_file)
        raise SystemExit(f"strategy service bind failed: {args.addr}")
    server.start()
    logger.info("strategy service listening on %s log_file=%s", args.addr, args.log_file)
    print(f"strategy service listening on {args.addr}", flush=True)
    # 阻塞主线程，直到进程被终止。
    server.wait_for_termination()


# 只有直接运行本文件时才启动服务；被 import 时不会自动启动。
if __name__ == "__main__":
    main()
