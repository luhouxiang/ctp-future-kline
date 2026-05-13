# -*- coding: utf-8 -*-
"""策略服务通用基础设施。

本模块放所有策略都需要、但不属于某个具体交易算法的代码：
- 日志配置：Python 服务既要被命令行直接运行，也会被 Go 进程拉起，所以需要可重复调用的日志初始化；
- JSON 编解码：HTTP 传输层统一使用 JSON bytes 和 dict；
- 请求字段读取与类型转换：外部 JSON 字段可能是数字、字符串或空值，策略入口统一兜底；
- 响应/trace/check 构造：让所有策略返回同一种 no_signal、metrics、trace 结构；
- Strategy 基类：给注册表提供统一接口，具体策略只覆盖自己关心的事件。

为什么这些代码要独立出来：
如果每个策略都自己写 `_no_signal`、`_trace` 或类型转换，前端解释策略过程时会看到不同字段形状。
集中在这里可以保证“无信号原因、诊断指标、状态步骤”在所有策略之间保持一致。
"""

from __future__ import annotations

import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from typing import Any

from strategy_types import (
    CheckDict,
    JSONObject,
    MetricsDict,
    ModeKey,
    RequestDict,
    ResponseDict,
    StrategyDefinition,
    TraceDict,
)

logger = logging.getLogger(__name__)

# 默认日志文件放在 flow/strategy_logs 下，和 Go 侧 runtime 日志目录保持一致，便于排查跨进程问题。
DEFAULT_STRATEGY_LOG_FILE: str = os.path.join("flow", "strategy_logs", "strategy_service.log")


def _log_formatter() -> logging.Formatter:
    """统一日志格式。

    为什么带 filename 和 lineno：
    策略模块拆分后，同一个请求可能经过 registry、runtime_service、具体策略多个文件。
    文件名和行号能直接定位是哪一层输出的日志，避免只看中文 reason 时难以追踪。
    """
    # 增加 datefmt 参数，格式：月-日 时:分:秒。
    return logging.Formatter(
        "[%(asctime)s][%(levelname)s]%(filename)s:%(lineno)d %(message)s", datefmt="%m-%d %H:%M:%S"
    )


def _configure_logging(log_file: str | os.PathLike[str] | None, level_name: str | None = "INFO") -> None:
    """配置控制台与 UTF-8 滚动文件日志。

    使用 `force=True` 是有意的：本服务可能被单测、直接命令行、Go 子进程以不同方式启动。
    如果沿用 Python 进程里已有的 basicConfig，日志可能丢到错误位置或用非 UTF-8 编码写中文。
    """
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


def _ensure_logging_configured(
    log_file: str | os.PathLike[str] | None = DEFAULT_STRATEGY_LOG_FILE,
    level_name: str | None = None,
) -> None:
    """确保日志至少包含目标文件 handler。

    这个函数和 `_configure_logging` 分开，是为了兼容“模块被导入”和“服务被直接启动”两种路径：
    - 直接启动时 main() 会明确指定日志文件和级别；
    - 单测或外部 import 时不能强制覆盖已有日志配置，只补齐缺失的文件 handler。
    """
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


def _rfc3339_utc_now() -> str:
    """返回 Go `time.Time` 能直接解析的 UTC RFC3339 时间。

    为什么不再使用 `%Y-%m-%dT%H:%M:%S`：
    Go 的策略定义结构里 `updated_at` 是 `time.Time`，JSON 解码必须看到 `Z` 或 `+08:00` 这类时区。
    如果 Python 返回无时区字符串，Go 侧 `ListStrategies` 会解码失败，页面就会拿到空策略列表。
    """
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _log_strategy_phase(request: RequestDict, response: ResponseDict | None) -> None:
    """为每次策略状态推进写一行紧凑日志。

    trace 已经会返回给调用方，但日志仍然保留一份是为了排查线上/回放差异：
    如果 Go 侧只记录最终响应，这里可以看到每个事件落在哪个 step、为什么等待或失败。
    """
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
def _loads(data: bytes | bytearray | memoryview | None) -> RequestDict:
    """将 JSON bytes 请求体反序列化为 Python dict。

    参数：
        data: 请求传入的 bytes。上层约定其内容为 UTF-8 编码的 JSON。

    返回：
        dict: 若 data 为空，返回空字典；否则返回 json.loads 后的对象。

    说明：
        传输层直接传递原始 JSON bytes，再由这里统一转 dict。
    """
    if not data:
        return {}
    return json.loads(data.decode("utf-8"))


def _dumps(value: Any) -> bytes:
    """将 Python 对象序列化为 UTF-8 JSON bytes，作为响应体。

    ensure_ascii=False：保留中文字符，便于调试时直接阅读。
    separators=(",", ":")：去掉多余空格，压缩响应体体积。
    """
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# -----------------------------------------------------------------------------

# 基础类型转换与请求字段读取工具
# -----------------------------------------------------------------------------
def _float(value: Any, default: float = 0.0) -> float:
    """安全地将任意值转换为 float，失败时返回默认值。

    这样可以避免行情字段为空字符串、None 或异常类型时导致策略直接崩溃。
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    """安全地将任意值转换为 int，失败时返回默认值。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _instance_id(request: RequestDict) -> str:
    """从请求中读取策略实例 ID。

    请求结构预期类似：
        {"instance": {"instance_id": "..."}, ...}

    若字段不存在，返回空字符串，避免 KeyError。
    """
    return (request.get("instance") or {}).get("instance_id", "")


def _params(request: RequestDict) -> dict[str, Any]:
    """从请求中读取策略实例参数 params。

    返回值始终是 dict；如果 instance 或 params 缺失，则返回空字典。
    """
    return (request.get("instance") or {}).get("params") or {}


def _mode_from_value(value: Any) -> ModeKey:
    """归一化运行模式，作为运行实例和状态字典的 key。"""
    return "replay" if str(value or "").strip().lower() == "replay" else "live"


def _instance_mode(instance: JSONObject | None) -> ModeKey:
    """从策略实例配置中读取归一化运行模式。"""
    return _mode_from_value((instance or {}).get("mode"))


def _normalize_symbols(symbols: Any) -> list[str]:
    """将实例 symbols 规范成字符串列表。"""
    if not isinstance(symbols, list):
        return []
    return [str(item) for item in symbols if str(item) != ""]


def _warmup_bar_time(bar: Any) -> str:
    """读取 warmup 日志展示用时间。

    该函数只服务日志摘要，不参与状态机时间顺序判断；真正的事件时间读取由 `_bar_event_time` 完成。
    """
    if not isinstance(bar, dict):
        return ""
    return _bar_event_time(bar)


def _bar_event_time(bar: Any, fallback: Any = "") -> str:
    """读取 K 线事件时间。

    当前优先且只使用 `adjusted_time`，这是为了让回放锚点、补齐 warmup、前端绘图使用同一条调整后时间轴。
    旧字段 `plot_time/event_time/fallback` 不能随意混用，否则同一根 K 线可能在不同路径得到不同时间戳，
    进而触发乱序过滤或重复处理。若以后恢复 fallback，需要同步补充回归测试。
    """
    if not isinstance(bar, dict):
        return str(fallback or "")
    return str(bar.get("adjusted_time")) # or bar.get("plot_time") or bar.get("event_time") or fallback or "")


def _instance_start_log_payload(instance: JSONObject) -> dict[str, Any]:
    """提取实例启动日志摘要，避免直接打印全部 warmup_bars。

    warmup_bars 可能有几百到上千根，直接写入日志会让一次 StartInstance 淹没真实错误。
    这里只保留数量、首尾时间和参数摘要，既能排查 Go 侧是否按目标数量取数，又不会污染日志文件。
    """
    params = dict(instance.get("params") or {})
    warmup_bars = params.get("warmup_bars")
    warmup_count = len(warmup_bars) if isinstance(warmup_bars, list) else _int(params.get("warmup_count"), 0)
    warmup_first_time = ""
    warmup_last_time = ""
    if isinstance(warmup_bars, list):
        if warmup_bars:
            warmup_first_time = _warmup_bar_time(warmup_bars[0])
            warmup_last_time = _warmup_bar_time(warmup_bars[-1])
        params["warmup_bars"] = f"<{len(warmup_bars)} bars>"
    return {
        "instance_id": instance.get("instance_id", ""),
        "strategy_id": instance.get("strategy_id", ""),
        "display_name": instance.get("display_name", ""),
        "mode": instance.get("mode", ""),
        "account_id": instance.get("account_id", ""),
        "symbols": instance.get("symbols") or [],
        "timeframe": instance.get("timeframe", ""),
        "start_time": params.get("start_time") or params.get("chart_start_time") or "",
        "warmup_target": _int(params.get("warmup_target"), 0),
        "warmup_count": warmup_count,
        "warmup_anchor_time": params.get("warmup_anchor_time", ""),
        "warmup_first_time": warmup_first_time,
        "warmup_last_time": warmup_last_time,
        "params": params,
    }


def _no_signal(
    request: RequestDict,
    reason: str,
    metrics: MetricsDict | None = None,
    trace: TraceDict | None = None,
    isLog: bool = True,
) -> ResponseDict:
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
    if isLog:
        logger.info("no_signal_from_strategy: instance_id=%s,symbol=%s,event_time=%s,reason=%s",
                    out["instance_id"], out["symbol"], out["event_time"], reason)
    return out


def _trace(
    request: RequestDict,
    event_type: str,
    step_key: str,
    step_label: str,
    step_index: int,
    status: str,
    reason: str,
    checks: list[CheckDict] | None = None,
    metrics: MetricsDict | None = None,
    signal_preview: dict[str, Any] | None = None,
) -> TraceDict:
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


def _check(
    name: str,
    passed: Any,
    current: Any = None,
    target: Any = None,
    delta: Any = None,
    description: str = "",
) -> CheckDict:
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
    definition: StrategyDefinition = {}

    def start_instance(self, instance: JSONObject) -> None:
        """启动策略实例时调用。默认无状态策略不需要做任何事。"""
        return None

    def stop_instance(self, instance_id: str) -> None:
        """停止策略实例时调用。默认无状态策略不需要清理。"""
        return None

    def required_warmup_bars(self, instance: JSONObject) -> int:
        """返回启动该策略实例前至少需要预热的 K 线数量。"""
        return 0

    def validate_warmup(self, instance: JSONObject, applied_counts: dict[str, int] | None = None) -> None:
        """校验 warmup 后的运行状态。默认无额外校验。"""
        return None

    def on_tick(self, request: RequestDict) -> ResponseDict:
        """处理 tick 事件。默认忽略 tick。"""
        return _no_signal(request, "tick ignored")

    def on_bar(self, request: RequestDict) -> ResponseDict:
        """处理 K 线 bar 事件。默认忽略 bar。"""
        logger.info("on_bar called with request: %s", request)
        return _no_signal(request, "bar ignored")

    def on_replay_bar(self, request: RequestDict) -> ResponseDict:
        """处理回放 K 线事件。默认复用 on_bar 逻辑。"""
        return self.on_bar(request)


# -----------------------------------------------------------------------------
# 请求校验与时间处理工具
# -----------------------------------------------------------------------------
def _mode_key(request: RequestDict) -> ModeKey:
    """将请求中的 mode 归一化为状态字典使用的 key。

    当前只区分两类：
    - "replay"：回放模式；
    - "live"：非 replay 的所有情况都归为实盘/实时模式。

    这样同一个 instance_id + symbol 在 live 和 replay 下可以拥有互不干扰的状态。
    """
    return _mode_from_value(request.get("mode"))


def _require_fields(source: JSONObject, fields: tuple[str, ...]) -> None:
    """校验 source 中是否包含必填字段。

    若字段缺失、为 None 或空字符串，则抛出 ValueError。
    上层 HTTP 入口会捕获该异常并转换为 4xx 错误。
    """
    missing = [name for name in fields if source.get(name) is None or source.get(name) == ""]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")


def _strategy_params_for_instance(definition: StrategyDefinition | None, instance: JSONObject | None) -> dict[str, Any]:
    """合并策略默认参数与实例参数。

    default_params 是策略作者给出的稳定默认值；instance.params 是用户在前端或 Go 配置里覆盖的值。
    每次读取时重新合并，可以避免某个实例修改 params 后污染策略模板。
    """
    params = dict((definition or {}).get("default_params") or {})
    params.update((instance or {}).get("params") or {})
    return params


def _event_ts(value: Any) -> float | None:
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
