import os
import falcon.asgi
import uvicorn

import uuid
import threading

# --- 1. 定义策略逻辑 (可以是 Cython 编写的类) ---
class BaseStrategy:
    def __init__(self, strategy_id):
        self.strategy_id = strategy_id
        self.data_history = []
        self.lock = threading.Lock()  # Python 3.14 无锁模式下，保护实例内部数据安全

    def on_bar(self, bar_data):
        # 这里的逻辑在 Python 3.14 中可以真正的并行运行
        with self.lock:
            self.data_history.append(bar_data)
            # 模拟策略计算：例如计算均线、下单判断等
            processed_result = f"Strategy {self.strategy_id} processed bar at {bar_data.get('time')}"
            return processed_result

# --- 2. 策略管理器：负责维护多个独立实例 ---
class StrategyManager:
    def __init__(self):
        self.instances = {}

    def create_instance(self):
        s_id = str(uuid.uuid4())[:8]
        self.instances[s_id] = BaseStrategy(s_id)
        return s_id

    def get_instance(self, s_id):
        return self.instances.get(s_id)

manager = StrategyManager()

# --- 3. Falcon 路由接口 ---
class StrategyResource:
    # 接口：创建策略实例
    async def on_post_create(self, req, resp):
        s_id = manager.create_instance()
        resp.media = {"status": "success", "strategy_id": s_id}

    # 接口：接收 K 线数据并触发运行
    async def on_post_data(self, req, resp, s_id):
        instance = manager.get_instance(s_id)
        if not instance:
            resp.status = falcon.HTTP_404
            resp.media = {"error": "Strategy instance not found"}
            return

        # 获取请求体中的 K 线数据
        bar_data = await req.get_media()
        
        # 触发策略运行
        result = instance.on_bar(bar_data)
        
        resp.media = {"status": "executed", "result": result}

# --- 4. App 实例化 ---
app = falcon.asgi.App()
service = StrategyResource()

app.add_route('/instance/create', service, suffix='create')
app.add_route('/instance/{s_id}/on_bar', service, suffix='data')

# ... 你的路由和策略逻辑 ...

if __name__ == "__main__":
    print("检测到调试模式：启动原生 ASGI 测试服务器...")
    # 调试模式下使用简单的服务器，或者减小 Granian 的线程数
    uvicorn.run("demo_server:app", host="127.0.0.1", port=8000, reload=True)
