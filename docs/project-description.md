# Mô tả dự án Taxi Demand

## 1. Tổng quan

Taxi Demand là pipeline xử lý dữ liệu lớn và dự báo nhu cầu taxi theo không gian và thời gian. Dự án sử dụng NYC Yellow Taxi Trip Records trong giai đoạn 2023–2024, xử lý bằng Apache Spark và lưu trữ ở định dạng Parquet.

Đơn vị phân tích là số chuyến bắt đầu tại một `zone_id` trong một giờ (`pickup_hour`). Kết quả được dùng cho phân tích khám phá, xây dựng đặc trưng và so sánh các mô hình dự báo.

## 2. Mục tiêu

- Chuẩn hóa và làm sạch 24 file dữ liệu taxi.
- Tổng hợp nhu cầu theo khu vực đón khách và từng giờ.
- Phân tích quy luật nhu cầu theo giờ, ngày trong tuần và khu vực.
- Xây dựng baseline và mô hình dự báo theo chuỗi thời gian.
- Đánh giá mô hình bằng MAE, RMSE và R².

## 3. Phạm vi dữ liệu

- Nguồn: NYC Yellow Taxi Trip Records.
- Thời gian: 01/2023 đến 12/2024.
- Số file raw: 24 file Parquet.
- Dữ liệu đầu vào: khoảng 79,5 triệu bản ghi.
- Múi giờ xử lý: `America/New_York`.

Dữ liệu raw và các output lớn được lưu ngoài Git repository. Repository chỉ chứa mã nguồn, cấu hình và tài liệu.

## 4. Pipeline

```text
Raw Parquet
    ↓
Kiểm tra schema và chất lượng dữ liệu
    ↓
Làm sạch dữ liệu bằng Spark
    ↓
EDA và trực quan hóa
    ↓
Tổng hợp zone_id × pickup_hour
    ↓
Tạo feature lịch sử và feature thời gian
    ↓
Baseline và mô hình ML
    ↓
Đánh giá trên tập dữ liệu theo thời gian
```

## 5. Output chính

| Output | Mô tả |
|---|---|
| `data/cleaned/taxi_cleaned.parquet` | Dữ liệu sạch toàn bộ hai năm |
| `data/cleaned/sample_2023_01.parquet` | Sample tháng 01/2023 để kiểm thử nhanh |
| `data/processed/demand_hourly.parquet` | Nhu cầu theo `zone_id × pickup_hour` |
| `output/charts/` | Biểu đồ EDA và đánh giá |
| `output/*.json` | Báo cáo chất lượng và metric |

## 6. Phân công

### Thành viên 1 Data Engineering

- Chuẩn hóa môi trường và Docker Compose.
- Kiểm tra schema và chất lượng dữ liệu.
- Làm sạch dữ liệu bằng PySpark.
- Bàn giao dữ liệu sạch và báo cáo xử lý.

### Thành viên 2 EDA và Demand Grid

- Phân tích nhu cầu theo giờ, ngày và khu vực.
- Tạo bảng `zone_id × pickup_hour × demand`.
- Bổ sung các khung giờ không có chuyến với `demand = 0`.

### Thành viên 3 Evaluation và Baseline

- Chia train, validation và test theo thời gian.
- Xây dựng các baseline `lag_1`, `lag_24` và `lag_168`.
- Tính MAE, RMSE, R² và tạo biểu đồ Actual vs Predicted.

## 7. Quy tắc chia dữ liệu

| Tập | Thời gian |
|---|---|
| Train | 01/2023–09/2024 |
| Validation | 10/2024 |
| Test | 11/2024–12/2024 |

Không sử dụng random split vì đây là bài toán chuỗi thời gian.

## 8. Quy ước GitHub

### 8.1. Đặt tên branch

Tên branch dùng chữ thường, không dấu, không khoảng trắng và theo mẫu:

```text
<loai>/<pham-vi>-<mo-ta-ngan>
```

Các loại branch được sử dụng:

| Loại | Mục đích | Ví dụ |
|---|---|---|
| `feature/` | Thêm chức năng | `feature/tv2-eda-demand` |
| `fix/` | Sửa lỗi | `fix/cleaning-timezone` |
| `docs/` | Cập nhật tài liệu | `docs/project-description` |
| `refactor/` | Cải tổ mã nguồn | `refactor/spark-reader` |
| `chore/` | Công việc cấu hình/bảo trì | `chore/update-dependencies` |

Branch hiện tại của phần Data Engineering là:

```text
codex/week1-data-engineering
```

### 8.2. Quy trình làm việc

Không commit hoặc push trực tiếp vào `main`. Mỗi thay đổi phải được thực hiện trên branch riêng:

```powershell
git switch -c feature/<ten-cong-viec>
git add .
git commit -m "Mo ta thay doi"
git push -u origin feature/<ten-cong-viec>
```

Sau khi kiểm tra, tạo Pull Request vào `main`. Chỉ merge khi code đã được review và các kiểm tra cần thiết đều đạt.

### 8.3. Quy tắc commit

Commit message ngắn gọn, dùng động từ ở dạng mệnh lệnh và nêu đúng phạm vi thay đổi:

```text
feat: add demand aggregation
fix: handle missing pickup zone
docs: update project description
chore: align Spark dependencies
```
