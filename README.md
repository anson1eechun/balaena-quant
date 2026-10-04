# Balaena Mission Control

本機量化研究系統的即時控制台。單一 Python 檔、只用標準庫，畫面上的每個數字都直接讀機器的真實狀態：systemd、nvidia-smi、journalctl、SQLite、Obsidian vault。沒有假的 agent 數字，也沒有寫死的 log。

這是 [factor-lab](https://github.com/anson1eechun/factor-lab) 與 Hermes Agent 的控制面板，靈感來自 Balaena Quant 的 Mission Control 介面。

## 功能

| 頁面 / API | 內容 | 資料來源 |
|---|---|---|
| 健康列 | 2 個 Hermes gateway 與本機 llama.cpp 服務的狀態、GPU 記憶體與使用率、文獻攝取是否在 24 小時內跑過 | `systemctl --user is-active`、`nvidia-smi`、攝取追蹤檔 |
| Agent Team | 每個 Hermes profile 的模型、provider、是否正在跑，以及 factor-lab 角色的真實辯論步數 | `~/.hermes/profiles/*/config.yaml`、`pgrep`、`index.db` |
| Strategy Pipeline | 因子依 status 對應到階段，附最新一輪 IC、Sharpe、裁決；含美股 EQ 因子 | `index.db` 的 `factors`、`debate_rounds` |
| 一鍵回測 | 從 `core/signals/` 選模組、資產（BTC / ETH / XAU / OIL）、週期、成本，背景執行回測 | `scripts/backtest_timeseries.py`（`uv run`） |
| Processes | 相關行程、Hermes cron 排程、Kanban 任務 | `ps`、`~/.hermes/cron/jobs.json`、`kanban.db` |
| Log Stream | 三個服務合併後的最近 log | `journalctl --user` |
| KB Explorer | Obsidian vault「量化交易」的領域分類、檔案搜尋、Markdown 預覽 | vault 內的 `.md` |
| `/graph` | 因子、論文、概念的 typed 知識圖譜 | factor-lab 的 `core/kb_graph.py` |

## 執行

```bash
python3 mission_control.py                    # 唯讀，http://127.0.0.1:8900
python3 mission_control.py --enable-dispatch  # 另外開放「對 agent 下指令」
```

常駐執行用 `deploy/balaena-mission-control.service`（systemd user unit，記憶體上限 1G）：

```bash
cp deploy/balaena-mission-control.service ~/.config/systemd/user/
systemctl --user enable --now balaena-mission-control
```

## 安全

- 只綁 `127.0.0.1`，沒有任何認證。
- 指令派發預設關閉。開啟後 `POST /api/dispatch` 會在背景執行 `hermes chat --profile <p> -q <cmd>`，profile 必須存在於 `~/.hermes/profiles/`。
- KB 檔案讀取用 `realpath` 擋路徑穿越，只允許 vault 內的 `.md`。
- 回測 API 只接受 `core/signals/` 裡實際存在的模組名，資產與週期都走白名單。

## 設定

路徑寫在檔案開頭的常數，換機器要改：

```python
DB_PATH    = "/home/anson/factor-lab/index.db"
VAULT_ROOT = "/home/anson/Documents/Obsidian Vault/量化交易"
PROFILES_DIR = "/home/anson/.hermes/profiles"
SERVICES = [("hermes-gateway.service", ...), ("hermes-gateway-deepseek.service", ...), ("llama-qwen.service", ...)]
```

任何資料來源讀不到時，對應區塊會顯示空值或 `UNKNOWN`，不會造假。
