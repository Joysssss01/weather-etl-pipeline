#  Weather Data Pipelines with Apache Airflow, BigQuery and Grafana
<img width="1280" height="720" alt="data_engineering_project" src="https://github.com/user-attachments/assets/697ba2eb-c600-4f69-aea3-d40db50f3942" />

## 🚀 快速開始與環境配置 (Setup & Configuration)

本專案使用 Docker Compose 同步啟動 Airflow 與 Grafana。為了確保資料安全，GCP 的金鑰檔案與敏感設定不會包含在程式碼倉庫中。

### 1. 準備 GCP 服務帳戶金鑰

要讓 Grafana 從 BigQuery 讀取資料，請遵循以下步驟：

* 在 GCP Console 建立一個 **Service Account** 並給予 `BigQuery Data Viewer` 權限。
* 下載 JSON 格式的金鑰檔。
* 將該檔案重新命名為 `gcp-key.json`。
* 將檔案放置於專案根目錄的 `config/` 資料夾中：
```bash
# 路徑應如下：
airflow-docker/config/gcp-key.json

```





> **注意：** `config/gcp-key.json` 已被加入 `.gitignore`，請確保它不會被上傳至 GitHub。

### 2. 配置環境變數

請在專案根目錄建立 `.env` 檔案，並參考以下設定：

```env
# Airflow 設定
AIRFLOW_UID=50000

# Grafana 設定
GRAFANA_PASSWORD=admin # 預設登入密碼
GCP_PROJECT_ID=你的GCP專案ID
GCP_SERVICE_ACCOUNT_EMAIL=你的服務帳號Email

```

### 3. 啟動專案

在終端機執行以下指令：

```bash
docker-compose up -d

```

啟動後，你可以透過以下網址存取服務：

* **Airflow:** `http://localhost:8080` (預設帳密: airflow/airflow)
* **Grafana:** `http://localhost:3000` (預設帳密: admin/admin)

---

## 📊 視覺化自動化 (Grafana Provisioning)

本專案採用 **"Configuration as Code"** 的概念。當 Docker 容器啟動時，系統會自動執行以下動作：

1. **自動連線 BigQuery:** 透過 `grafana/provisioning/datasources/` 中的 YAML 設定，自動讀取 `config/gcp-key.json` 並建立 Data Source。
2. **自動載入儀表板:** 系統會掃描 `grafana/dashboards/` 資料夾，並將其中的 JSON 檔案自動匯入為 Grafana 儀表板。

---

## 🛡️ 安全性說明 (Security)

* **敏感檔案保護:** 所有金鑰 (`*.json`)、環境變數 (`.env`) 以及運行數據 (`logs/`, `grafana-data/`) 均已透過 `.gitignore` 排除。
* **權限控管:** 建議在 GCP 上僅給予服務帳戶唯讀權限，以符合最小權限原則。

---

