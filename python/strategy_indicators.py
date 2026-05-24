# -*- coding: utf-8 -*-
"""策略通用技术指标工具。

目的：
- 把 MA、ATR、波动范围等标准指标从具体策略文件中剥离出来；
- 让不同策略复用同一套指标实现，避免每个策略各自手写一份导致口径不一致；
- 保持函数纯净：只根据输入数据返回计算结果，不读取策略状态，也不生成 trace/展示文案。

功能：
- `simple_moving_average`：计算简单移动平均线 SMA；
- `moving_average_slope`：计算均线在指定 lookback 上的平均斜率；
- `trim_tail`：裁剪历史序列，控制内存占用；
- `bar_range`：计算单根 K 线波动范围，并提供最小值兜底；
- `true_range`：计算单根真实波幅 TR；
- `trimmed_top_average`：取近期最大若干 TR 并去掉极端值后求平均。
"""

from __future__ import annotations


def simple_moving_average(values: list[float], period: int) -> float | None:
    """计算最近 period 个值的简单移动平均线。

    参数：
    - values：按时间顺序排列的数值序列，通常是收盘价列表；
    - period：均线周期，例如 20 表示 MA20。

    返回 None 表示样本不足或周期无效；调用方据此决定是否继续等待 warmup。
    """
    if period <= 0 or len(values) < period:
        return None
    return sum(values[-period:]) / period


def moving_average_slope(values: list[float], period: int, lookback: int) -> float | None:
    """计算均线斜率。

    返回最近 period 均值与 lookback 根之前 period 均值的平均变化。
    样本不足时返回 None，调用方可选择等待或使用兜底值。
    """
    if period <= 0 or lookback <= 0 or len(values) < period + lookback:
        return None
    current = sum(values[-period:]) / period
    previous = sum(values[-period - lookback:-lookback]) / period
    return (current - previous) / lookback


def trim_tail(values: list[float], keep: int) -> list[float]:
    """只保留序列末尾 keep 个值。

    参数：
    - values：需要裁剪的历史序列；
    - keep：需要保留的最新元素数量。

    策略通常只需要最近若干根 K 线计算指标，裁剪可避免 replay/live 长时间运行后列表无限增长。
    """
    if keep <= 0 or len(values) <= keep:
        return values
    return values[-keep:]


def bar_range(high: float, low: float, minimum: float = 0.0001) -> float:
    """计算单根 K 线的高低点范围。

    参数：
    - high：K 线最高价；
    - low：K 线最低价；
    - minimum：返回值下限，避免范围为 0。

    minimum 用于处理 high == low 或异常窄幅 K 线，避免后续止盈止损阈值变成 0。
    """
    return max(minimum, high - low)


def true_range(high: float, low: float, previous_close: float | None = None, minimum: float = 0.0001) -> float:
    """计算单根真实波幅 TR。"""
    if previous_close is None:
        return bar_range(high, low, minimum)
    return max(minimum, high - low, abs(high - previous_close), abs(low - previous_close))


def trimmed_top_average(values: list[float], lookback: int, top_n: int, drop_n: int) -> float | None:
    """取最近 lookback 个值中最大的 top_n 个，去掉最大的 drop_n 个后求平均。

    例如 lookback=60/top_n=10/drop_n=2，就是“最近 60 个 TR 的最大 10 个，
    去掉最大的 2 个，对剩余 8 个求平均”。
    """
    if lookback <= 0 or top_n <= 0 or drop_n < 0 or top_n <= drop_n:
        return None
    if len(values) < lookback:
        return None
    selected = sorted(values[-lookback:], reverse=True)[:top_n]
    trimmed = selected[drop_n:]
    if not trimmed:
        return None
    return sum(trimmed) / len(trimmed)
