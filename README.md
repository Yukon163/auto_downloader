# Intelligent Download Agent

智能下载代理 - 使用类 TCP 拥塞控制算法的多线程下载器

## 架构

```
Chrome Extension  →  Python Agent (Native Messaging)  →  Go MCP Server
   (拦截下载)           (决策逻辑)                        (分片下载)
```

## 核心算法：类 TCP 拥塞控制

### 二叉树分片 (Binary Tree Segmentation)
- **初始分片** = 完整文件大小
- 每轮下载时，将分片**二分**为更小的分片
- 分片只需记录**起始位置**（结束位置由下一分片计算）

### 拥塞窗口 (cwnd)
| 阶段 | 行为 |
|------|------|
| **慢启动 (Slow Start)** | cwnd 翻倍: 1→2→4→8... 直到达到 ssthresh |
| **拥塞避免 (Congestion Avoidance)** | cwnd 线性增长: +1/轮 |
| **快速恢复 (Fast Recovery)** | 失败时: ssthresh = cwnd/2, cwnd = cwnd/2 |

## 快速开始

### 1. 编译 Go 下载器

```bash
cd G-Downloader
go build -o go-downloader.exe .
```

### 2. 安装 Chrome 扩展

1. 打开 Chrome，访问 `chrome://extensions`
2. 启用「开发者模式」
3. 点击「加载已解压的扩展程序」选择 `chorme_donlodIntercepter` 文件夹
4. 复制扩展的 **Extension ID**

### 3. 配置 Native Messaging

1. 编辑 `local_agent/com.autodownloader.agent.json`
2. 将 `EXTENSION_ID_PLACEHOLDER` 替换为你的扩展 ID
3. 以管理员身份运行 `local_agent/install_host.bat`

### 4. 测试

下载任意 > 50MB 的文件，扩展会：
- 拦截下载
- 通过 Python Agent 分析
- 使用自适应拥塞控制下载（如支持 Range 请求）
- 完成后显示通知（包含分片数和最终 cwnd）

## 组件

| 组件 | 路径 | 说明 |
|------|------|------|
| Go MCP Server | `G-Downloader/` | 提供 `detect_resource` 和 `adaptive_download` 工具 |
| Python Agent | `local_agent/` | Native Messaging Host + 决策逻辑 |
| Chrome Extension | `chorme_donlodIntercepter/` | 下载拦截器 (Manifest V3) |

## 配置参数

编辑 `local_agent/host.py`:
- `SIZE_THRESHOLD_MB` - 触发加速下载的最小文件大小 (默认: 50MB)
- `MAX_CWND` - 最大拥塞窗口/并发连接数 (默认: 16)

## 环境要求

- Go 1.21+
- Python 3.8+
- Chrome/Chromium
