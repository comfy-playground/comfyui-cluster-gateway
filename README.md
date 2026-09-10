# ComfyUI Gateway TS

一个面向多台独立 ComfyUI worker 的 TypeScript 网关。客户端继续请求
ComfyUI 风格的 HTTP API；网关负责全局排队、按预估完成时间调度、保存历史与
输出，并可在严格等价时把多个请求合成一次物理执行。

> 这是部署组件，不是 ComfyUI 本体、模型包或通用集群编排系统。每个 worker
> 仍由使用者自行部署、维护模型与工作流兼容性。

## 能力

- 提供 `/prompt`、`/history/:id`、`/view`、`/queue`、`/interrupt`、
  `/system_stats` 和基础 `/ws`，让现有 ComfyUI 客户端可以接入网关。
- 使用 SQLite 持久化全局队列、worker 执行映射、幂等键、历史和输出索引；worker
  离线后已完成的图片和 history 仍可由网关读取。
- 每块 GPU 一个执行槽；初始按配置的每图耗时调度，样本足够后使用 EWMA 实测值。
- 可选的模型注册表按完整 loader 依赖（扩散模型、文本编码器、VAE）识别请求；worker
  可通过 `model_ids` 白名单差异化分配模型，状态可由 `/gateway/v1/models` 查询。
- 可选合批：支持 SDXL `euler/normal`、Anima `euler/sgm_uniform`，以及安装
  专用节点后的 Anima `er_sde`。每个逻辑请求保持独立 `prompt_id`、history 和
  输出；不支持时自动单请求执行。
- 主 worker 是唯一 LoRA catalog 控制面。网关会在 catalog 变更时 drain、递增
  revision 并对在线 worker 执行完整重建。
- LoRA Manager MCP 作为独立 `mcp/` Git submodule 管理，避免在 Gateway 中维护副本。

## 架构

```text
ComfyUI clients
       |
       v
Gateway :19189 ---- SQLite + copied output files
       |
       +---- primary ComfyUI worker (required, LoRA catalog control)
       |
       +---- optional ComfyUI workers (one visible GPU each)

```

## 使用条件

- Docker Engine 和 Docker Compose；或本地 Node.js `>= 22.18.0`。项目使用
  Node 内置的 `node:sqlite`，Node 20 不受支持。
- 每个上游 worker 必须运行可用的 ComfyUI HTTP 服务，且 `/system_stats` 只能
  返回一块可见 GPU。网关会精确校验设备名，防止错误地向另一块 GPU 调度。
- 主 worker 必须提供项目所使用的 LoRA Manager HTTP 路由，例如
  `/api/lm/loras/list`、`/api/lm/loras/scan`、`/api/lm/download-model`。所有
  准备接收带 LoRA 工作流的 worker 应使用一致的 LoRA 目录和 catalog 内容。
- 合批不是默认要求。启用前，将
  [`worker/ComfyUI-Gateway-Batch`](worker/ComfyUI-Gateway-Batch) 复制到每个
  worker 的 `custom_nodes/ComfyUI-Gateway-Batch` 并重启 ComfyUI。缺少节点时
  网关会安全回退到 batch size 1。
- 可选的 MCP 由 [`mcp/`](mcp) 子模块独立部署。其网络 LoRA 查询与下载需要访问
  Civitai；`CIVITAI_API_KEY` 仅在 Civitai 需要或建议认证时配置，绝不能提交。

## 快速开始

1. 准备一台主 ComfyUI worker，并记录 `GET /system_stats` 中唯一 GPU 的 `name`。
   在 Docker for Linux 场景，示例的 `host.docker.internal` 已映射到宿主机；若
   worker 位于其他网络，请在配置中改为实际可达 URL。
2. 建立本机配置并填写主 worker 的 URL、精确设备名和预估单图耗时：

   ```sh
   cp config.example.yaml config.yaml
   cp .env.example .env
   ```

   `config.yaml` 和 `.env` 被 Git 忽略。初次启动只配置一个主 worker 即可；需要
   横向扩容时，复制 `workers` 项并增加一个 `primary: false`、`required: false`
   的 worker，再为其增加对应的 `batching.workers` 配置。

   配置 `models` 后，模型请求必须匹配注册条目的完整文件组合，并且目标 worker 的
   `model_ids` 必须包含该模型。没有 `models` 配置时保留旧版兼容行为。
3. 构建并启动 Gateway：

   ```sh
   docker compose build
   docker compose up -d gateway
   curl http://127.0.0.1:19189/readyz
   ```

   默认只监听 `127.0.0.1`。只有在反向代理、身份认证和网络访问控制都已就绪时，
   才将 `GATEWAY_BIND_ADDRESS` 改为局域网或公网地址。
4. 将 ComfyUI 客户端的服务地址指向 `http://<gateway-host>:19189`。可使用下列
   端点查看状态：

   ```sh
   curl http://127.0.0.1:19189/healthz
   curl http://127.0.0.1:19189/gateway/v1/status
   curl http://127.0.0.1:19189/gateway/v1/workers
   ```

`/healthz` 只表示 HTTP 进程存活；`/readyz` 会在 required worker、设备校验和
初始 catalog 未就绪时返回 `503`。

## 配置与认证

`config.example.yaml` 是完整的 schema 示例，解析器会拒绝未知字段。主要字段：

| 配置 | 作用 |
| --- | --- |
| `workers` | 上游 ComfyUI URL、精确 GPU 名、预估速度、能力、required/primary 状态 |
| `limits` | Prompt、输出、队列的上限，避免单一客户端耗尽网关资源 |
| `scheduler` | EWMA 与排队老化参数 |
| `batching` | 合批开关、等待窗口、每个 worker/profile 的最大 batch size |
| `retention` | Gateway SQLite 和已复制图片的容量与保留时间 |
| `auth` | 指向环境变量名的 generation/management Bearer token 配置 |

默认没有 HTTP token，以保持原生客户端兼容。若需认证，在 `config.yaml` 中写入
环境变量名称，例如：

```yaml
auth:
  generation_token_env: COMFYUI_GATEWAY_GENERATION_TOKEN
  management_token_env: COMFYUI_GATEWAY_MANAGEMENT_TOKEN
```

再在本机 `.env` 中配置值。generation token 保护除 `/healthz` 外的 Gateway
路由；management token 保护 `/api/lm/*` 和 catalog 管理路由。即使启用 token，
也应将管理路由放在反向代理访问控制之后。

## 合批

合批默认关闭。它只会处理网关判定为同一 adapter、同一 merge key 的请求，在
`merge_window_ms` 内等待可合并成员。支持的 adapter 为：

- `sdxl-euler-normal`：需要 worker 中的 `GatewayMultiSeedNoise` 节点。
- `anima-euler-sgm`：需要 worker 中的 `GatewayMultiSeedNoise` 节点。
- `anima-er-sde`：需要 `GatewayMultiSeedStochasticSampler`；该节点会为每个成员
  单独调用 sampler，以确保随机状态独立，不能将其视为线性吞吐提升。

启用步骤：安装 worker 节点、在各 worker 的 `batching.workers` 中设定大于 1 的
上限、设置 `batching.enabled: true`，然后通过 `/gateway/v1/status` 确认
`batch_capable`。若一次物理 batch 失败，网关会按成员回退重试。

## MCP 子模块

MCP 是独立仓库
[`comfyui-lora-manager-mcp`](https://github.com/comfy-playground/comfyui-lora-manager-mcp)，
以 Git submodule 固定版本引用。它不再共享 Gateway 的依赖树、镜像、Compose 项目
或故障域，Gateway 本身也不依赖 MCP 才能启动。

克隆 Gateway 后初始化子模块：

```sh
git submodule update --init --recursive
cd mcp
cp .env.example .env
# 编辑 COMFYUI_URL、可选 COMFYUI_GATEWAY_URL、LORA_ROOT 与 CIVITAI_API_KEY
docker compose up -d
curl http://127.0.0.1:19173/healthz
```

在单节点模式，仅设置 `COMFYUI_URL`；状态工具会返回
`gateway_status_available: false`，其他 MCP 工具照常可用。设置
`COMFYUI_GATEWAY_URL` 后，MCP 才会返回 Gateway 的真实集群状态。详见子模块的
README。`LORA_ROOT` 必须是目标 ComfyUI 看到的目录路径。MCP 默认绑定
`127.0.0.1`；远程访问时还需设置 `MCP_BIND_ADDRESS`、`MCP_ALLOWED_HOSTS`，并在
外层提供 TLS 和认证。

## 开发与验证

```sh
npm ci
npm run typecheck
npm test
python3 -m unittest discover -s worker/tests
npm run build
docker build --tag comfyui-gateway-ts:local .
```

若主机未安装 Node 22.18，可使用容器运行测试：

```sh
docker run --rm --network none -e NODE_ENV=test \
  -v "$PWD:/app" -w /app node:22.18-alpine npm test
```

`scripts/smoke-load.ts`、`scripts/live-batch-smoke.ts` 和
`scripts/live-worker-batch-benchmark.ts` 会向真实 ComfyUI worker 发送任务，仅应
在隔离或明确授权的测试环境使用。MCP 的测试与 `contract` 工具在 `mcp/` 子模块中
独立运行。

## 外部依赖与其他仓库

运行 Gateway 本身不需要 MCP 进程；合批 adapter 和 worker 自定义节点仍在本仓库。
部署时存在以下外部边界：

| 依赖 | 是否必须 | 用途 |
| --- | --- | --- |
| ComfyUI worker | 是 | 原生 HTTP 推理服务、模型和工作流执行 |
| LoRA Manager ComfyUI 扩展 | 主 worker 必须；有 LoRA/catalog 功能时 worker 需一致 | LoRA inventory、扫描、下载与 catalog 同步 API |
| `worker/ComfyUI-Gateway-Batch` | 仅启用合批时 | 提供多 seed noise 与随机 sampler 节点 |
| `mcp/` Git submodule | 仅部署 MCP 时 | 独立的 LoRA Manager MCP 源码、镜像和 Compose 项目 |
| Civitai API | 仅 MCP LoRA 检索/下载时 | 模型元数据与下载链接 |
| Docker / Node 22.18 | 是 | 运行 Gateway；MCP 自行声明其运行时 |

模型、LoRA、ComfyUI 自定义节点和 Civitai 内容分别受其自身许可证约束；使用者应在
下载和部署前核对许可证、模型许可和网络访问政策。

## 发布说明

本仓库不会提交本机 worker 地址、GPU 名、token、SQLite、输出图片或模型数据。GitHub
Actions 会在 Node 22.18 上执行类型检查、测试、构建和 Docker 镜像构建。MCP 的
版本更新需先在其独立仓库发布，再由本仓库显式更新 submodule 指针。

当前仓库尚未包含 `LICENSE`。公开发布前，版权持有人需要选择并添加许可证；在此之前，
代码默认不授予第三方复制、修改或分发许可。
