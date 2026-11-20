# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + 清除入口 + 构建租约 API，均在 `/docs`）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

## 普通下载与清除

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
curl -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status
```

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数与当前包 sha256
curl http://localhost:8081/control/stats

# 更换包内容（重新生成随机字节，sha256 改变；代理在清除前仍发旧包）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 构建租约 API

一次构建的编译与打包可能分别取依赖；缓存即使在中途被清除并重新填充，构建
也必须始终拿到开工那一刻的版本。租约把一组路径各自**当前完整代际**钉住，
在租期内反复下载同一字节。

### 1. 申请租约（`POST /leases`，可在 `/docs` 直接试）

请求体：1～16 个**互不重复**的包路径，`ttl_seconds` 取 1～300 秒。整组必须
在同一时点全部命中**完整的本地缓存**，否则原子失败，不钉住任何路径：

```bash
# 先确保包已入缓存（租约只认完整缓存，不会触发回源）
curl -s -o /dev/null http://localhost:8080/packages/pkg-1m.bin
curl -s -o /dev/null http://localhost:8080/packages/pkg-32m.bin

# 申请：两包同一时点完整缓存 -> 201
curl -s -X POST http://localhost:8080/leases \
  -H 'Content-Type: application/json' \
  -d '{"paths":["pkg-1m.bin","pkg-32m.bin"],"ttl_seconds":120}'
```

返回 `201` 与租约编号、到期时间、每个路径钉住的代际（`gen`/`size`/`sha256`）。
失败语义：

* 任一缺包或仍在填充 → `409`，响应体 `unavailable` 列出**全部**不可用路径及
  原因（`not_cached` / `still_filling`）；
* 路径重复、数量越界、TTL 越界 → `422`。

### 2. 凭租约逐块取包（可反复取）

```bash
# 同一租约编号 + 路径，可重复下载，字节始终是申请时钉住的版本
curl -o dep1.bin http://localhost:8080/leases/<lease_id>/packages/pkg-1m.bin
curl -o dep2.bin http://localhost:8080/leases/<lease_id>/packages/pkg-1m.bin
```

响应头带 `X-Cache: LEASE`、`X-Lease`、`X-Lease-State` 与钉住版本的
`X-Package-Sha256`。错误码：租约编号不存在 `404`；路径不在租约内 `403`；
租约已到期 `410`。

### 3. 清除与租约并行

```bash
# 源站换内容 + 代理清除：普通下载照常回源填充新版本
curl -X POST http://localhost:8081/control/content/pkg-1m.bin
curl -X POST http://localhost:8080/purge/pkg-1m.bin
curl -o fresh.bin http://localhost:8080/packages/pkg-1m.bin      # 新版本

# 租约下载仍是申请时的旧字节（钉住文件改名 .keep-<gen> 保留）
curl -o pinned.bin http://localhost:8080/leases/<lease_id>/packages/pkg-1m.bin
```

### 4. 到期两段式释放

* 到期瞬间租约转为 `expired`，**新的租约下载一律 `410`**；到期前已受理的
  在途下载可以读完整包（客户端断连也算下载结束）。
* 在途下载全部结束后，租约自动转为 `released`，释放钉住；若该路径已被清除，
  旧代际文件随即回收。后台 reaper 每 0.2s 巡检，下载结束时也会即时触发。

### 5. 查询

```bash
curl http://localhost:8080/leases                      # 全部租约
curl http://localhost:8080/leases/<lease_id>           # 单个：状态/剩余秒数/在途数/钉住代际
```

### 6. 进程重启

租约只存内存：重启后租约表为空，旧编号返回 `404`；启动时清理所有
`.part-*`、`.keep-*` 残件与孤儿 meta，完整包缓存仍然保留并继续 HIT。

## 行为要点

* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者（及租约 holder）结束后回收。
* **租约钉版**：清除时被租约钉住的完整文件改名为 `.keep-<gen>`，新填充可
  正常发布同名正式文件而不覆盖旧字节；租约释放后保留文件才删除。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*`、`.keep-*`
  残件与孤儿 meta，租约全部失效。404 不做负缓存。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install -r tests/requirements.txt
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越，以及租约的全组原子申请与 409 清单、填充中拒绝、参数校验、
反复取包、清除/重填交错下的钉版、到期拒绝新请求但在途读完、断连后自动
释放、重启失效与残件清理。`tests/run_server.py` 是测试专用启动器（本沙箱
禁止进程间信号，用控制文件代替 SIGTERM 停服），交付应用本身不含测试钩子。
