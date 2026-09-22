# Taxi Demand

Pipeline xử lý và dự báo nhu cầu taxi theo không gian–thời gian bằng Apache Spark.

## Phạm vi

- Dữ liệu: NYC Yellow Taxi Trip Records năm 2023 và 2024.
- Quy mô: 24 file Parquet, khoảng 80 triệu chuyến.
- Đơn vị phân tích: `zone_id × pickup_hour`.
- Múi giờ xử lý: `America/New_York`.

Pipeline gồm các bước:

```text
Raw Parquet → kiểm tra dữ liệu → làm sạch → EDA → aggregate demand
→ feature engineering → baseline/ML → đánh giá
```

## Công nghệ

- Python 3.11+
- PySpark 3.5.1
- Apache Spark 3.5.1
- Docker Compose
- Parquet
- pandas, PyArrow, Matplotlib và pytest

## Cấu trúc thư mục

```text
taxi-demand/
├── data/
│   ├── cleaned/       # Parquet sau làm sạch, sinh khi chạy pipeline
│   └── processed/     # Dữ liệu aggregate/features, sinh khi chạy pipeline
├── output/            # Báo cáo JSON và biểu đồ, sinh khi chạy pipeline
├── src/
│   ├── 01_check_data.py
│   ├── 02_clean_data.py
│   ├── 03_eda.py
│   ├── 04_aggregate_demand.py
│   ├── 05_features.py
│   ├── 06_train.py
│   └── 07_evaluate.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

Dữ liệu raw và các output Parquet không được commit vào Git. Đặt dữ liệu raw bên ngoài repository, ví dụ:

```text
taxi-demand-data/
└── raw/
    ├── 2023/
    │   ├── yellow_tripdata_2023-01.parquet
    │   └── ...
    └── 2024/
        ├── yellow_tripdata_2024-01.parquet
        └── ...
```

## Cài đặt local

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Nếu raw data không nằm ở vị trí mặc định, đặt biến môi trường cho các script local:

```powershell
$env:TAXI_DATA_DIR = "C:\path\to\taxi-demand-data\raw"
```

## Chạy Spark bằng Docker

Docker Compose tạo một Spark Master và hai Spark Worker.

```powershell
docker compose up -d
docker compose ps
```

Giao diện kiểm tra:

- Spark Master: <http://localhost:8080>
- Worker 1: <http://localhost:8081>
- Worker 2: <http://localhost:8082>

Compose mặc định mount raw data từ `../taxi-demand-data/raw`. Có thể thay đổi bằng:

```powershell
$env:TAXI_RAW_DIR = "C:\path\to\taxi-demand-data\raw"
docker compose up -d
```

## Kiểm tra dữ liệu

Chạy trên toàn bộ raw data bằng local Spark:

```powershell
python src\01_check_data.py --data-dir "$env:TAXI_DATA_DIR"
```

Báo cáo mặc định được ghi tại `output/data_quality_report.json`.

## Làm sạch dữ liệu

Chạy trên Spark cluster Docker:

```powershell
docker compose exec spark-master spark-submit --master spark://spark-master:7077 /app/src/02_clean_data.py --data-dir /data/raw --output /app/data/cleaned/taxi_cleaned.parquet --sample-output /app/data/cleaned/sample_2023_01.parquet --report /app/output/cleaning_report.json
```

Quy tắc làm sạch:

- Giữ pickup time trong khoảng `2023-01-01` đến trước `2025-01-01`.
- Giữ `zone_id` trong khoảng `1–265`.
- Loại pickup time null.
- Loại dropoff trước pickup.
- Loại chuyến dài hơn 24 giờ.
- Loại bản ghi trùng hoàn toàn.
- Giữ các cột nghiệp vụ có null nếu không ảnh hưởng đến việc đếm pickup.

Các cột chuẩn cho các bước sau:

```text
pickup_datetime
zone_id
pickup_year
pickup_month
```

Output hiện tại:

| Output | Phạm vi | Số dòng |
|---|---|---:|
| `data/cleaned/taxi_cleaned.parquet` | Toàn bộ 2023–2024 | 79.475.304 |
| `data/cleaned/sample_2023_01.parquet` | Tháng 01/2023 | 3.066.698 |

Spark ghi Parquet dưới dạng thư mục chứa các file `part-*.parquet`, dù tên thư mục có hậu tố `.parquet`. Đọc bằng:

```python
df = spark.read.parquet("data/cleaned/taxi_cleaned.parquet")
```

Báo cáo chạy bằng Docker: `output/cleaning_report_docker.json`.

## Các bước tiếp theo

1. `src/03_eda.py`: phân tích nhu cầu theo giờ, ngày trong tuần và zone.
2. `src/04_aggregate_demand.py`: tạo bảng `zone_id × pickup_hour × demand`, bao gồm các khung giờ có `demand = 0`.
3. `src/05_features.py`: tạo feature lịch, lag và rolling theo từng zone.
4. `src/06_train.py`: huấn luyện baseline và mô hình Spark MLlib.
5. `src/07_evaluate.py`: đánh giá bằng MAE, RMSE và R².

Khi phát triển các bước này, có thể dùng sample tháng 01/2023 để kiểm thử nhanh. Kết quả cuối cùng phải chạy trên `taxi_cleaned.parquet` toàn bộ hai năm.

## Quy tắc chia dữ liệu

Đây là bài toán chuỗi thời gian, không dùng random split:

| Tập | Thời gian |
|---|---|
| Train | 01/2023–09/2024 |
| Validation | 10/2024 |
| Test | 11/2024–12/2024 |

## Git workflow

Không commit trực tiếp vào `main`. Tạo branch cho từng thay đổi, mở Pull Request, kiểm tra và merge sau khi branch đã được push:

```powershell
git switch -c codex/<ten-thay-doi>
git add .
git commit -m "Mo ta thay doi"
git push -u origin codex/<ten-thay-doi>
```

Sau khi Pull Request được duyệt, merge vào `main` trên GitHub.
