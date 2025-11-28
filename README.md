# 跨交易所套利机器人

本项目是一个加密货币期货跨所套利的框架，仅为分享交流目的，不能直接用于生产环境，实际交易需谨慎使用。

## 项目简介

本项目实现了一个跨交易所套利交易机器人，目前主要在 **edgeX** 和 **Lighter** 两个交易所之间进行价差套利。机器人通过在 edgeX 上挂 post-only 限价单（做市单），在 Lighter 上执行市价单来完成套利交易。

## 功能特性

- 🔄 **跨交易所套利**：自动检测并利用两个交易所之间的价差
- 📊 **实时订单簿管理**：通过 WebSocket 实时监控订单簿变化
- 📈 **仓位跟踪**：实时跟踪和管理交易仓位
- 🛡️ **风险控制**：支持最大仓位限制和超时控制
- 📝 **数据记录**：记录交易数据和统计信息
- ⚡ **异步执行**：基于 asyncio 的高性能异步架构

## 系统要求

- Python 3.8+
- edgeX 和 Lighter 交易所账户
- API 密钥和访问权限

## 安装步骤

### 1. 克隆仓库

```bash
git clone <repository-url>
cd cross-exchange-arbitrage
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置环境变量

复制 `env_example.txt` 为 `.env` 并填写您的 API 凭证：

```bash
cp env_example.txt .env
```

编辑 `.env` 文件，填入你的 API 信息：

```env
# edgeX 账户凭证（必需）
EDGEX_ACCOUNT_ID=your_account_id_here
EDGEX_STARK_PRIVATE_KEY=your_stark_private_key_here

# EdgeX API 端点
EDGEX_BASE_URL=https://pro.edgex.exchange
EDGEX_WS_URL=wss://quote.edgex.exchange

# Lighter 配置（必需）
API_KEY_PRIVATE_KEY=your_api_key_private_key_here
LIGHTER_ACCOUNT_INDEX=your_account_index
LIGHTER_API_KEY_INDEX=your_api_key_index
```

## 使用方法

### 基本用法

```bash
python arbitrage.py --ticker BTC --size 0.002
```

### 命令行参数

- `--exchange`：交易所名称（默认：edgex）
- `--ticker`：交易对符号（默认：BTC）
- `--size`：每笔订单的交易数量（必需）
- `--max-position`：最大持仓限制（必需）
- `--long-threshold`：做多套利触发阈值（Lighter 买一价高于 edgeX 卖一价超过多少即做多 edgeX 套利，默认：10）
- `--short-threshold`：做空套利触发阈值（edgeX 买一价高于 Lighter 卖一价超过多少即做空 edgeX 套利，默认：10）
- `--fill-timeout`：限价单成交超时时间（秒，默认：5）

### 使用示例

```bash
# 交易 ETH，每笔订单 0.01 ETH，设置 5 秒超时
python arbitrage.py --ticker ETH --size 0.01 --long-threshold 10 --short-threshold 10 --max-position 0.1 --fill-timeout 5

# 交易 BTC，限制最大持仓为 0.1 BTC
python arbitrage.py --ticker BTC --size 0.002 --long-threshold 1 --short-threshold 20 --max-position 0.1
```

## 项目结构

```
cross-exchange-arbitrage/
├── arbitrage.py              # 主程序入口
├── exchanges/                # 交易所接口实现
│   ├── base.py              # 基础交易所接口
│   ├── edgex.py             # edgeX 交易所实现
│   ├── lighter.py           # Lighter 交易所实现
│   └── lighter_custom_websocket.py  # Lighter WebSocket 管理
├── strategy/                 # 交易策略模块
│   ├── edgex_arb.py         # 主要套利策略
│   ├── order_book_manager.py    # 订单簿管理
│   ├── order_manager.py     # 订单管理
│   ├── position_tracker.py  # 仓位跟踪
│   ├── websocket_manager.py # WebSocket 管理
│   └── data_logger.py       # 数据记录
├── requirements.txt         # Python 依赖
├── env_example.txt          # 环境变量示例
└── README.md               # 项目说明文档
```

## 工作原理

1. **订单簿监控**：通过 WebSocket 实时接收两个交易所的订单簿更新
2. **价差检测**：计算两个交易所之间的价差
3. **套利机会识别**：当价差超过阈值时，识别套利机会
4. **订单执行**：
   - 在 edgeX 上挂 post-only 限价单（做市单，赚取手续费）
   - 在 Lighter 上执行市价单完成对冲
5. **仓位管理**：实时跟踪仓位，确保不超过最大持仓限制
6. **风险控制**：监控订单成交状态，超时未成交则取消订单

## 注意事项

⚠️ **风险提示**：

- 套利交易存在市场风险，请确保充分理解交易机制
- 建议先在测试环境或小额资金下测试
- 注意网络延迟和交易所 API 限制
- 定期检查仓位和资金状况

## 依赖说明

主要依赖包括：

- `python-dotenv`：环境变量管理
- `asyncio`：异步编程支持
- `requests`：HTTP 请求
- `tenacity`：重试机制
- `edgex-python-sdk`：edgeX 官方 Python SDK（fork 版本，支持 post-only 限价单）
- `lighter-python`：Lighter 交易所 SDK

## 许可证

请查看 [LICENSE](LICENSE) 文件了解详情。

## 贡献

欢迎提交 Issue 和 Pull Request！

## 联系方式

如有问题或建议，请通过 Issue 联系。
