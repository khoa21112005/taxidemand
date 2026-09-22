# TV2 — EDA và Demand Grid

Branch: `feature/tv2-eda-demand`

## Yêu cầu đã triển khai

- `src/03_eda.py`
  - biểu đồ nhu cầu theo 24 giờ;
  - so sánh ngày thường và cuối tuần;
  - Top 15 zone đón khách;
  - tự động ghép `taxi_zone_lookup.csv` nếu có;
  - dữ liệu trip-level được xử lý bằng Spark; chỉ các bảng aggregate nhỏ mới chuyển sang pandas để vẽ.
- `src/04_aggregate_demand.py`
  - group theo `zone_id × pickup_hour`;
  - tạo đủ 17.544 giờ wall-clock từ 2023-01-01 00:00 đến 2024-12-31 23:00;
  - dùng toàn bộ zone_id 1–265;
  - cross join thành grid đầy đủ rồi fill `demand = 0`;
  - đọc lại Parquet bằng Spark và kiểm tra row count, min/max, số zone, số giờ, duplicate pair, missing pair và null demand.

Với 265 zone và 17.544 giờ, số dòng kỳ vọng của demand grid là:

`265 × 17.544 = 4.649.160`.

## Output

Sau khi pipeline chạy thành công:

```text
output/
├── charts/
│   ├── 01_hourly_demand_24h.png
│   ├── 02_weekday_vs_weekend.png
│   └── 03_top_zones.png
├── eda_tables/
│   ├── hourly_demand_24h.csv
│   ├── weekday_vs_weekend.csv
│   └── top_zones.csv
└── reports/
    └── demand_hourly_summary.json

data/processed/
└── demand_hourly.parquet/
```

## Chạy với cleaned data có sẵn

```powershell
python src\03_eda.py --input data\cleaned\taxi_cleaned.parquet --zone-lookup data\raw\taxi_zone_lookup.csv
python src\04_aggregate_demand.py --input data\cleaned\taxi_cleaned.parquet
```

## CI tái tạo full 2023–2024

Workflow `.github/workflows/tv2-build.yml` tải 24 file NYC TLC Yellow Taxi chính thức, tạo cleaned tối giản cho TV2, chạy EDA và Demand Grid, sau đó commit các output đã sinh về branch này.

`src/02b_prepare_tv2_minimal.py` chỉ phục vụ CI/reproducibility khi không có sẵn full cleaned dataset. Pipeline chính vẫn dùng contract `pickup_datetime, zone_id` do bước cleaning bàn giao.


## Ghi chú timestamp

Các timestamp TLC được xử lý như giờ wall-clock, không áp dụng quy đổi DST khi dựng lưới giờ. Spark session dùng `UTC` để bảo toàn đúng 17.544 nhãn giờ từ `2023-01-01 00:00:00` đến `2024-12-31 23:00:00`; điều này tránh giờ bị trùng/mất tại các mốc DST.
