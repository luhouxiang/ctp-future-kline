graph TD
%% 定义节点样式
classDef wait fill:#e3f2fd,stroke:#1565c0,stroke-width:2px;
classDef active fill:#fff9c4,stroke:#fbc02d,stroke-width:2px;
classDef end fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;

Start((策略启动)) --> State1[WAIT_BREAK_BELOW_MA20<br>等待跌破MA20]
class State1 wait

State1 -- "收盘价跌破MA20" --> State2[BROKEN_BELOW_MA20<br>已跌破，等待反抽触碰]
class State2 wait

State2 -- "价格反抽并触碰MA20" --> State3[WAIT_BREAK_TOUCH_OPEN<br>等待跌破触碰K的开盘价]
class State3 wait

State3 -- "跌破触碰K开盘价" --> State4[SIGNAL_ACTIVE<br>发出做空信号]
class State4 active

State4 -- "下一根K线开盘确认" --> State5[DONE<br>已持仓，监控止盈止损]
class State5 active

State5 -- "触发止盈/止损" --> State6[CLOSED<br>已平仓]
class State6 end

%% 定义重置/失败路径
State2 -- "形态失败/超时" --> State1
State3 -- "价格重新站上MA20 / 超时" --> State1
