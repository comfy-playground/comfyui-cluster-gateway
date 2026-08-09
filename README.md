# ComfyUI Gateway TS

独立的 ComfyUI 横向扩容网关。外部仍使用原生 ComfyUI HTTP 接口；A3000 是 required 主节点和唯一 LoRA control，3090 是可移除的高权重 worker。

## 模块

- 生成面：`/prompt`、`/history/:id`、`/view`、`/queue`、`/interrupt`、`/system_stats`、基础 `/ws`。
- 调度面：每张 GPU 一个执行槽，SQLite 全局队列，按预计最早完成时间分配，达到样本阈值后采用实际耗时 EWMA。
- 输出面：结果从 worker 收集到网关目录，worker 离线后历史和图片仍可读取。
- 管理面：`/api/lm/*` allowlist 固定命中 A3000 control；mutation 在 drain、revision 和所有在线 worker full rebuild 完成后才返回成功。
- 恢复面：持久化 global/backend prompt 映射；提交结果不确定或网关重启时先 reconcile，禁止盲目重提。

## 部署边界

仓库、镜像、SQLite 和输出均独立，不依赖 `/opt/docker/animagine/.env`。Compose 只创建 `comfyui-gateway-ts`，监听宿主 `127.0.0.1:19189`，不修改 AstrBot、MCP、ComfyUI 源码或已有容器。

默认不启用 HTTP token，以保持原生 ComfyUI 客户端兼容；需要鉴权时，在 `config.yaml` 的两个 `*_token_env` 中填入 `.env.example` 对应变量名。对外暴露时仍应在反向代理层限制管理路由。

```sh
npm run typecheck
docker run --rm --network none -e NODE_ENV=test -v "$PWD:/app" -w /app node:22.18-alpine npm test
docker compose build
docker compose up -d
curl http://127.0.0.1:19189/readyz
```

测试使用进程内 mock ComfyUI；所有被测请求都从网关监听端口进入，不直接请求 worker。
