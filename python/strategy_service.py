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
# time：生成更新时间、服务时间，并解析部分事件时间戳。
import time
# futures.ThreadPoolExecutor：作为 gRPC server 的线程池执行器。
from concurrent import futures
# dataclass/field：用于简洁定义策略状态对象 PullbackShortState。
from dataclasses import dataclass, field
# RLock：可重入锁，用来保护多线程 gRPC 请求下的策略状态字典。
from threading import RLock

# grpc 是运行服务时的可选依赖：
# - 如果环境中安装了 grpcio，则可以正常启动 gRPC 服务；
# - 如果没有安装，允许模块被导入，但 build_server/add_unary 会明确报错。
try:
    import grpc
except ModuleNotFoundError:
    grpc = None

# 模块级 logger，供本文件所有函数/类复用。
logger = logging.getLogger(__name__)


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
# 策略流程已完成：已经触发 SHORT 信号，后续不再重复触发。
DONE = "DONE"


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
        "default_params": {"ma_period": 20, "max_wait_bars": 6},
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    def __init__(self):
        """初始化状态容器与锁。"""
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
        params = _params(request)
        # 均线周期至少为 2，避免 period=0/1 等无意义或危险配置。
        ma_period = max(2, _int(params.get("ma_period"), 20))
        # 触碰后等待 bar 数至少为 1。
        max_wait_bars = max(1, _int(params.get("max_wait_bars"), 6))
        return ma_period, max_wait_bars

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
        close = _float(bar.get("close"))
        data_time = bar.get("data_time") or request.get("event_time", "")
        data_ts = _event_ts(data_time)

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

        # 如果已经完成，则不再重复触发信号，只继续维护 prev_close/prev_ma。
        if state.state == DONE:
            state.prev_close = close
            state.prev_ma = ma20
            trace = self._bar_trace(request, state, DONE, "已触发/已完成", 5, "done", "strategy already done", [], ma20, ma_period)
            return _no_signal(request, "strategy already done", self._base_metrics(state, ma20, ma_period), trace)

        # 默认 trace 信息。后续每个状态分支会按实际情况覆盖。
        step_key = state.state
        step_label = f"等待跌破 {ma_label}"
        step_index = 2
        status = "waiting"
        reason = "no trade signal"
        checks = []

        if state.state == WAIT_BREAK_BELOW_MA20:
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
                step_index = 3
                status = "passed"
                reason = f"break below {ma_label} confirmed"
            else:
                # 条件未满足，继续停留在等待跌破阶段。
                step_key = WAIT_BREAK_BELOW_MA20
                step_label = f"等待跌破 {ma_label}"
                step_index = 2

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
                step_index = 4
                status = "passed"
                reason = f"{ma_label} touch bar found"
            else:
                # 还没有反抽触碰 MA，继续等待。
                step_key = BROKEN_BELOW_MA20
                step_label = f"已跌破，等待反抽触碰 {ma_label}"
                step_index = 3

        elif state.state == WAIT_BREAK_TOUCH_OPEN:
            # stood_above：开盘和收盘均在 MA 上方，表示价格重新站上均线，做空形态失效。
            stood_above = open_price > ma20 and close > ma20
            wait_remaining = max_wait_bars - state.wait_bars
            checks = [
                _check(f"未重新站上{ma_label}", not stood_above, close, ma20, close - ma20, "重新站上则形态失败"),
                _check("等待未超时", state.wait_bars < max_wait_bars, state.wait_bars, max_wait_bars, wait_remaining),
                _check("触碰K开盘价有效", state.touch_open is not None, state.touch_open, "not null"),
            ]
            if stood_above:
                # 价格重新站上 MA：形态失败，重置状态机。
                self._reset_after_failure(state)
                step_key = WAIT_BREAK_BELOW_MA20
                step_label = f"等待跌破 {ma_label}"
                step_index = 2
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
                    step_index = 2
                    status = "failed"
                    reason = "reset: wait bars exceeded"
                else:
                    # 继续等待关键 tick 跌破触碰 K 开盘价。
                    step_key = WAIT_BREAK_TOUCH_OPEN
                    step_label = "等待跌破触碰K开盘价"
                    step_index = 4

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

            # 触发条件满足：将状态置为 DONE，生成 SHORT 信号。
            state.state = DONE
            metrics = self._base_metrics(state, state.touch_ma20, ma_period)
            metrics.update(
                {
                    "signal": "SHORT",
                    "touch_open": state.touch_open,
                    "touch_high": state.touch_high,
                    "touch_ma20": state.touch_ma20,
                    "touch_time": state.touch_time,
                    "trigger_price": last_price,
                    "entry_price": last_price,
                }
            )
            trace = _trace(
                request,
                "key_tick",
                DONE,
                "已触发/已完成",
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
        self.strategies = {
            "sample.momentum": SampleMomentumStrategy(),
            "ma20.pullback_short": MA20PullbackShortStrategy(),
        }

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
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StartInstance(self, request, context):
        """启动策略实例，并把 instance 交给对应策略初始化。"""
        instance = request.get("instance") or {}
        strategy = self._strategy_for_instance(instance)
        strategy.start_instance(instance)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def StopInstance(self, request, context):
        """停止策略实例。

        为简化实现，这里会遍历所有策略并让它们清理该 instance_id 下的状态。
        """
        instance_id = request.get("instance_id", "")
        for strategy in self.strategies.values():
            strategy.stop_instance(instance_id)
        return {"ok": True, "version": "python-sample-v1", "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    def OnTick(self, request, context):
        """接收 tick 事件并分发给请求中指定的策略。"""
        return self._strategy_for_request(request).on_tick(request)

    def OnBar(self, request, context):
        """接收实时 K 线事件并分发给请求中指定的策略。"""
        return self._strategy_for_request(request).on_bar(request)

    def OnReplayBar(self, request, context):
        """接收回放 K 线事件并分发给请求中指定的策略。"""
        return self._strategy_for_request(request).on_replay_bar(request)

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
    # 配置基础日志格式。
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # 解析监听地址，默认只监听本机 127.0.0.1:50051。
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:50051")
    args = parser.parse_args()
    # 创建 gRPC server，绑定端口并启动。
    server = build_server()
    server.add_insecure_port(args.addr)
    server.start()
    print(f"strategy service listening on {args.addr}", flush=True)
    # 阻塞主线程，直到进程被终止。
    server.wait_for_termination()


# 只有直接运行本文件时才启动服务；被 import 时不会自动启动。
if __name__ == "__main__":
    main()
