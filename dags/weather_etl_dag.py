from airflow import DAG
from airflow.sdk import task  # 消除 Deprecation Warning 的修改點
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowSkipException  # === 新增：用來優雅跳過任務的模組 ===
from datetime import datetime, timedelta
import pendulum  # <--- 新增這一行
import pandas as pd
import requests
import io
import os

# === 設定區 ===
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("GCP_DATASET_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/config/gcp-key.json"

# 定義 Table ID
TABLE_DIM_CITIES = "dim_cities"
TABLE_FACT_FORECAST = "fact_forecasts"
TABLE_FACT_ACTUAL = "fact_actuals"

# 城市列表 (自動生成 ID)
CITIES_RAW = [
    # --- 亞洲 (Asia) ---
    {"name": "Taipei", "country": "Taiwan", "continent": "Asia", "lat": 25.0330, "long": 121.5654},
    {"name": "Seoul", "country": "South Korea", "continent": "Asia", "lat": 37.5665, "long": 126.9780},
    {"name": "Tokyo", "country": "Japan", "continent": "Asia", "lat": 35.6895, "long": 139.6917},
    {"name": "Singapore", "country": "Singapore", "continent": "Asia", "lat": 1.3521, "long": 103.8198},
    {"name": "Mumbai", "country": "India", "continent": "Asia", "lat": 19.0760, "long": 72.8777},
    {"name": "Dubai", "country": "UAE", "continent": "Asia", "lat": 25.2048, "long": 55.2708},
    {"name": "Bangkok", "country": "Thailand", "continent": "Asia", "lat": 13.7563, "long": 100.5018},
    {"name": "Jakarta", "country": "Indonesia", "continent": "Asia", "lat": -6.2088, "long": 106.8456},
    {"name": "Beijing", "country": "China", "continent": "Asia", "lat": 39.9042, "long": 116.4074},
    {"name": "Lhasa", "country": "China", "continent": "Asia", "lat": 29.6469, "long": 91.1172},
    {"name": "Almaty", "country": "Kazakhstan", "continent": "Asia", "lat": 43.2389, "long": 76.8897},

    # --- 歐洲 (Europe) ---
    {"name": "Berlin", "country": "Germany", "continent": "Europe", "lat": 52.5244, "long": 13.4105},
    {"name": "Paris", "country": "France", "continent": "Europe", "lat": 48.8566, "long": 2.3522},
    {"name": "London", "country": "UK", "continent": "Europe", "lat": 51.5074, "long": -0.1278},
    {"name": "Palermo", "country": "Italy", "continent": "Europe", "lat": 38.1157, "long": 13.3615},
    {"name": "Barcelona", "country": "Spain", "continent": "Europe", "lat": 41.3851, "long": 2.1734},
    {"name": "Reykjavik", "country": "Iceland", "continent": "Europe", "lat": 64.1466, "long": -21.9426},
    {"name": "Moscow", "country": "Russia", "continent": "Europe", "lat": 55.7558, "long": 37.6173},
    {"name": "Istanbul", "country": "Turkey", "continent": "Europe", "lat": 41.0082, "long": 28.9784},
    {"name": "Athens", "country": "Greece", "continent": "Europe", "lat": 37.9838, "long": 23.7275},
    {"name": "Warsaw", "country": "Poland", "continent": "Europe", "lat": 52.2297, "long": 21.0122},

    # --- 北美洲 (North America) ---
    {"name": "New York", "country": "USA", "continent": "North America", "lat": 40.7128, "long": -74.0060},
    {"name": "Los Angeles", "country": "USA", "continent": "North America", "lat": 34.0522, "long": -118.2437},
    {"name": "Toronto", "country": "Canada", "continent": "North America", "lat": 43.6532, "long": -79.3832},
    {"name": "Mexico City", "country": "Mexico", "continent": "North America", "lat": 19.4326, "long": -99.1332},
    {"name": "Vancouver", "country": "Canada", "continent": "North America", "lat": 49.2827, "long": -123.1207},
    {"name": "Honolulu", "country": "USA", "continent": "North America", "lat": 21.3069, "long": -157.8583},
    {"name": "Havana", "country": "Cuba", "continent": "North America", "lat": 23.1136, "long": -82.3666},

    # --- 南美洲 (South America) ---
    {"name": "São Paulo", "country": "Brazil", "continent": "South America", "lat": -23.5505, "long": -46.6333},
    {"name": "Buenos Aires", "country": "Argentina", "continent": "South America", "lat": -34.6037, "long": -58.3816},
    {"name": "Ushuaia", "country": "Argentina", "continent": "South America", "lat": -54.8019, "long": -68.3030},
    {"name": "Lima", "country": "Peru", "continent": "South America", "lat": -12.0464, "long": -77.0428},
    {"name": "Bogotá", "country": "Colombia", "continent": "South America", "lat": 4.7110, "long": -74.0721},
    {"name": "Santiago", "country": "Chile", "continent": "South America", "lat": -33.4489, "long": -70.6693},

    # --- 非洲 (Africa) ---
    {"name": "Cairo", "country": "Egypt", "continent": "Africa", "lat": 30.0444, "long": 31.2357},
    {"name": "Cape Town", "country": "South Africa", "continent": "Africa", "lat": -33.9249, "long": 18.4241},
    {"name": "Lagos", "country": "Nigeria", "continent": "Africa", "lat": 6.5244, "long": 3.3792},
    {"name": "Accra", "country": "Ghana", "continent": "Africa", "lat": 5.6037, "long": -0.1870},
    {"name": "Nairobi", "country": "Kenya", "continent": "Africa", "lat": -1.2921, "long": 36.8219},
    {"name": "Casablanca", "country": "Morocco", "continent": "Africa", "lat": 33.5731, "long": -7.5898},
    {"name": "Addis Ababa", "country": "Ethiopia", "continent": "Africa", "lat": 9.0192, "long": 38.7469},

    # --- 大洋洲 (Oceania) ---
    {"name": "Sydney", "country": "Australia", "continent": "Oceania", "lat": -33.8688, "long": 151.2093},
    {"name": "Auckland", "country": "New Zealand", "continent": "Oceania", "lat": -36.8485, "long": 174.7633},
    {"name": "Perth", "country": "Australia", "continent": "Oceania", "lat": -31.9505, "long": 115.8605},
    {"name": "Suva", "country": "Fiji", "continent": "Oceania", "lat": -18.1248, "long": 178.4501},

    # --- 特殊極點 (Special Points) ---
    {"name": "North Pole", "country": "None", "continent": "Arctic", "lat": 90.0000, "long": 0.0000},
    {"name": "South Pole", "country": "None", "continent": "Antarctica", "lat": -90.0000, "long": 0.0000},
    {"name": "McMurdo Station", "country": "Antarctica", "continent": "Antarctica", "lat": -77.8419, "long": 166.6863},
    {"name": "Svalbard", "country": "Norway", "continent": "Europe", "lat": 78.2232, "long": 15.6267},
    {"name": "Easter Island", "country": "Chile", "continent": "Oceania", "lat": -27.1127, "long": -109.3497},
]

# 為城市加上 ID (從 1 開始)
CITIES = []
for idx, city in enumerate(CITIES_RAW):
    city['city_id'] = idx + 1
    CITIES.append(city)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_etl_v6',
    default_args=default_args,
    schedule='@daily',
    start_date=pendulum.datetime(2026, 2, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=['weather', 'etl', 'forecast', 'actuals'],
    max_active_runs=1
) as dag:

    # =================================================================
    # 共用函數: 呼叫 API 並合併 Weather + Air Quality
    # =================================================================
    def fetch_meteo_data(cities, params_weather, params_air):
        url_weather = "https://api.open-meteo.com/v1/forecast"
        url_air = "https://air-quality-api.open-meteo.com/v1/air-quality"

        lats = ",".join([str(c['lat']) for c in cities])
        longs = ",".join([str(c['long']) for c in cities])
        
        params_weather['latitude'] = lats
        params_weather['longitude'] = longs
        params_air['latitude'] = lats
        params_air['longitude'] = longs

        r_w = requests.get(url_weather, params=params_weather, timeout=60)
        r_a = requests.get(url_air, params=params_air, timeout=60)
        r_w.raise_for_status()
        r_a.raise_for_status()

        data_w = r_w.json()
        data_a = r_a.json()

        if not isinstance(data_w, list): data_w = [data_w]
        if not isinstance(data_a, list): data_a = [data_a]

        final_rows = []

        for city, w, a in zip(cities, data_w, data_a):
            df_w = pd.DataFrame(w['hourly'])
            df_a = pd.DataFrame(a['hourly'])
            
            df_w['time'] = pd.to_datetime(df_w['time'])
            df_a['time'] = pd.to_datetime(df_a['time'])
            
            df_merged = pd.merge(df_w, df_a, on='time', how='left')
            df_merged['city_id'] = city['city_id']
            
            final_rows.extend(df_merged.to_dict('records'))
            
        return final_rows

    # =================================================================
    # Task 1: 更新維度表 (Dim Cities)
    # =================================================================
    @task
    def update_dim_cities():
        df = pd.DataFrame(CITIES)
        cols = ['city_id', 'name', 'country', 'continent', 'lat', 'long']
        df = df[cols].rename(columns={'name': 'city_name', 'long': 'lon'})
        
        df.to_gbq(
            destination_table=f"{DATASET_ID}.{TABLE_DIM_CITIES}",
            project_id=PROJECT_ID,
            if_exists='replace',
            table_schema=[
                {'name': 'city_id', 'type': 'INTEGER'},
                {'name': 'city_name', 'type': 'STRING'},
                {'name': 'country', 'type': 'STRING'},
                {'name': 'continent', 'type': 'STRING'},
                {'name': 'lat', 'type': 'FLOAT'},
                {'name': 'lon', 'type': 'FLOAT'},
            ]
        )
        print("Updated dim_cities table.")

    # =================================================================
    # Stream A: Forecast Data (未來 7 天) -> 直接存 GCS
    # =================================================================
    # =================================================================
    # Stream A: Forecast Data (未來 7 天) -> 直接存 GCS
    # =================================================================
    @task
    def extract_forecast_data(**context): # <--- 改成接收 context
        # === 修改點：取得資料區間的結束點 (例如 3/2)，並轉為台灣時區 ===
        run_date = context['data_interval_end'].in_timezone('Asia/Taipei')
        current_date_str = run_date.strftime('%Y-%m-%d')
        
        params_w = {
            "hourly": "temperature_2m,precipitation,relative_humidity_2m,apparent_temperature,uv_index,cloud_cover,surface_pressure,wind_speed_10m,wind_direction_10m",
            "forecast_days": 7,
            "timezone": "Asia/Taipei"
        }
        params_a = {
            "hourly": "us_aqi",
            "timezone": "Asia/Taipei"
        }
        
        raw_data = fetch_meteo_data(CITIES, params_w, params_a)
        df = pd.DataFrame(raw_data)
        
        # === 修改點：使用 current_date_str 替代 ds ===
        df['forecast_date'] = current_date_str
        df.rename(columns={'time': 'target_time'}, inplace=True)
        
        rename_map = {
            'temperature_2m': 'pred_temp',
            'precipitation': 'pred_rain',
            'relative_humidity_2m': 'pred_humidity',
            'apparent_temperature': 'pred_apparent_temp',
            'uv_index': 'pred_uv',
            'cloud_cover': 'pred_cloud_cover',
            'surface_pressure': 'pred_pressure',
            'wind_speed_10m': 'pred_wind_speed',
            'wind_direction_10m': 'pred_wind_dir',
            'us_aqi': 'pred_aqi'
        }
        df.rename(columns=rename_map, inplace=True)
        df['target_time'] = pd.to_datetime(df['target_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        final_cols = ['forecast_date', 'target_time', 'city_id'] + list(rename_map.values())
        df = df[final_cols]
        
        # --- 寫入 GCS (新結構) ---
        file_date_str = run_date.strftime('%Y_%m_%d')
        # 目錄結構: forecast/年/月/檔名
        gcs_path = f"forecast/{run_date.year}/{run_date.month:02d}/{file_date_str}_forecast_data.csv"
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        
        hook = GCSHook(gcp_conn_id='google_cloud_default')
        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=gcs_path,
            data=csv_buffer.getvalue(),
            mime_type='text/csv; charset=utf-8'
        )
        
        return gcs_path

    # =================================================================
    # Stream B: Actual Data (Yesterday) -> 直接存 GCS
    # =================================================================
    @task
    def extract_actual_data(**context):  # <--- 修改點 1: 改接收 context
        # 取得資料區間的結束時間 (即代表實際運作的「這一天」)
        run_date = context['data_interval_end'].in_timezone('Asia/Taipei')
        
        # === 核心修改：我們的目標永遠是「運作日的昨天」 ===
        target_date_dt = run_date.subtract(days=1)
        target_date_str = target_date_dt.strftime('%Y-%m-%d')

        params_w = {
            "hourly": "temperature_2m,precipitation,relative_humidity_2m,apparent_temperature,uv_index,cloud_cover,surface_pressure,wind_speed_10m,wind_direction_10m",
            "past_days": 1,
            "forecast_days": 0,
            "timezone": "Asia/Taipei"
        }
        params_a = {
            "hourly": "us_aqi",
            "past_days": 1,
            "forecast_days": 0,
            "timezone": "Asia/Taipei"
        }
        
        raw_data = fetch_meteo_data(CITIES, params_w, params_a)
        df = pd.DataFrame(raw_data)
        
        df['time'] = pd.to_datetime(df['time'])
        # 確保只篩選出「昨天」的資料
        df = df[df['time'].dt.strftime('%Y-%m-%d') == target_date_str]

        if df.empty:
            print(f"Warning: No data found for target date ({target_date_str})")
            raise AirflowSkipException(f"No data to process for {target_date_str}, skipping downstream tasks.")

        rename_map = {
            'temperature_2m': 'actual_temp',
            'precipitation': 'actual_rain',
            'relative_humidity_2m': 'actual_humidity',
            'apparent_temperature': 'actual_apparent_temp',
            'uv_index': 'actual_uv',
            'cloud_cover': 'actual_cloud_cover',
            'surface_pressure': 'actual_pressure',
            'wind_speed_10m': 'actual_wind_speed',
            'wind_direction_10m': 'actual_wind_dir',
            'us_aqi': 'actual_aqi'
        }
        df.rename(columns=rename_map, inplace=True)
        
        df['date'] = df['time'].dt.date
        df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['date'] = df['date'].astype(str)
        
        cols = ['date', 'time', 'city_id'] + list(rename_map.values())
        df = df[cols]
        
        # --- 寫入 GCS ---
        file_date_str = target_date_dt.strftime('%Y_%m_%d')
        gcs_path = f"actual/{target_date_dt.year}/{target_date_dt.month:02d}/{file_date_str}_actual_data.csv"
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        
        hook = GCSHook(gcp_conn_id='google_cloud_default')
        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=gcs_path,
            data=csv_buffer.getvalue(),
            mime_type='text/csv; charset=utf-8'
        )
        
        return gcs_path

    # =================================================================
    # BigQuery Load Operators
    # =================================================================
    
    create_forecast_table = BigQueryInsertJobOperator(
        task_id="create_forecast_table",
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{TABLE_FACT_FORECAST}` (
                    forecast_date DATE,
                    target_time DATETIME,
                    city_id INT64,
                    pred_temp FLOAT64,
                    pred_rain FLOAT64,
                    pred_humidity INT64,
                    pred_apparent_temp FLOAT64,
                    pred_uv FLOAT64,
                    pred_cloud_cover INT64,
                    pred_pressure FLOAT64,
                    pred_wind_speed FLOAT64,
                    pred_wind_dir INT64,
                    pred_aqi FLOAT64
                )
                PARTITION BY DATE_TRUNC(forecast_date, MONTH)
                OPTIONS(description="7-day weather forecast snapshot");
                """,
                "useLegacySql": False,
            }
        },
    )

    create_actual_table = BigQueryInsertJobOperator(
        task_id="create_actual_table",
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{TABLE_FACT_ACTUAL}` (
                    date DATE,
                    time DATETIME,
                    city_id INT64,
                    actual_temp FLOAT64,
                    actual_rain FLOAT64,
                    actual_humidity INT64,
                    actual_apparent_temp FLOAT64,
                    actual_uv FLOAT64,
                    actual_cloud_cover INT64,
                    actual_pressure FLOAT64,
                    actual_wind_speed FLOAT64,
                    actual_wind_dir INT64,
                    actual_aqi FLOAT64
                )
                PARTITION BY DATE_TRUNC(date, MONTH)
                OPTIONS(description="Historical weather ground truth");
                """,
                "useLegacySql": False,
            }
        },
    )

    load_forecast_bq = GCSToBigQueryOperator(
        task_id='load_forecast_bq',
        bucket=BUCKET_NAME,
        source_objects=["{{ ti.xcom_pull(task_ids='extract_forecast_data') }}"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_FACT_FORECAST}",
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        autodetect=False, 
        schema_fields=[    
            {'name': 'forecast_date', 'type': 'DATE'},
            {'name': 'target_time', 'type': 'DATETIME'},
            {'name': 'city_id', 'type': 'INTEGER'},
            {'name': 'pred_temp', 'type': 'FLOAT'},
            {'name': 'pred_rain', 'type': 'FLOAT'},
            {'name': 'pred_humidity', 'type': 'INTEGER'},
            {'name': 'pred_apparent_temp', 'type': 'FLOAT'},
            {'name': 'pred_uv', 'type': 'FLOAT'},
            {'name': 'pred_cloud_cover', 'type': 'INTEGER'},
            {'name': 'pred_pressure', 'type': 'FLOAT'},
            {'name': 'pred_wind_speed', 'type': 'FLOAT'},
            {'name': 'pred_wind_dir', 'type': 'INTEGER'},
            {'name': 'pred_aqi', 'type': 'FLOAT'}
        ],
        gcp_conn_id='google_cloud_default',
    )

    load_actuals_bq = GCSToBigQueryOperator(
        task_id='load_actuals_bq',
        bucket=BUCKET_NAME,
        source_objects=["{{ ti.xcom_pull(task_ids='extract_actual_data') }}"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_FACT_ACTUAL}",
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND', 
        autodetect=False,
        schema_fields=[    
            {'name': 'date', 'type': 'DATE'},
            {'name': 'time', 'type': 'DATETIME'},
            {'name': 'city_id', 'type': 'INTEGER'},
            {'name': 'actual_temp', 'type': 'FLOAT'},
            {'name': 'actual_rain', 'type': 'FLOAT'},
            {'name': 'actual_humidity', 'type': 'INTEGER'},
            {'name': 'actual_apparent_temp', 'type': 'FLOAT'},
            {'name': 'actual_uv', 'type': 'FLOAT'},
            {'name': 'actual_cloud_cover', 'type': 'INTEGER'},
            {'name': 'actual_pressure', 'type': 'FLOAT'},
            {'name': 'actual_wind_speed', 'type': 'FLOAT'},
            {'name': 'actual_wind_dir', 'type': 'INTEGER'},
            {'name': 'actual_aqi', 'type': 'FLOAT'}
        ],
        gcp_conn_id='google_cloud_default',
    )

    # =================================================================
    # 執行流程
    # =================================================================
    
    # 1. 更新維度表
    task_dim = update_dim_cities()

    # 2. 處理 Forecast (資料流 A)
    gcs_f_path = extract_forecast_data()
    
    # 3. 處理 Actuals (資料流 B)
    gcs_a_path = extract_actual_data()

    # 定義相依性
    task_dim >> create_forecast_table >> gcs_f_path >> load_forecast_bq
    task_dim >> create_actual_table >> gcs_a_path >> load_actuals_bq