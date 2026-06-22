# Kiến trúc Lakehouse cho LLM Observability Quy Mô 1 Tỷ Requests/Ngày

Hồ sơ thiết kế này đề xuất phương án lưu trữ và xử lý dữ liệu cho hệ thống giám sát cuộc gọi LLM ở quy mô lớn, đảm bảo tính kinh tế, bảo mật và hiệu năng.

---

## 1. Problem Statement (Tuyên bố bài toán)

Hệ thống ghi nhận **1 tỷ requests/ngày**, trung bình **5 KB/request**, sinh ra **5 TB dữ liệu thô mỗi ngày** (~150 TB/tháng). 

**Các ràng buộc thiết kế:**
1. Cung cấp Dashboard chi phí & độ trễ phân nhóm theo Tenant (khách hàng), làm mới dữ liệu **mỗi 5 phút**.
2. Lưu trữ đầy đủ prompt/response trong **7 ngày** để xử lý sự cố (incident review), sau đó tự động huỷ dữ liệu thô và chỉ giữ lại dữ liệu tổng hợp (aggregates) trong **1 năm**.
3. Che giấu thông tin nhạy cảm (PII Redaction) đối với prompt/response trước khi lưu xuống các tầng tiếp theo để ngăn rò rỉ dữ liệu.
4. Tổng chi phí lưu trữ S3 phải **$\le$ $5,000 / tháng**.

---

## 2. Architecture Diagram (Sơ đồ kiến trúc)

```text
                      [ API Gateway / LLM Proxy ]
                                   │
                                   ▼
                      [ Kinesis Firehose / Kafka ]
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│                          DELTA LAKEHOUSE ON S3                         │
│                                                                        │
│ ┌────────────────────────────────────────────────────────────────────┐ │
│ │ 1. BRONZE TIER (S3 Standard - Lifecycle: 1 ngày)                  │ │
│ │    • Đường dẫn: s3://bronze/llm_calls_raw                          │ │
│ │    • Định dạng: Delta (Append-Only)                                │ │
│ │    • Cấu trúc: ts, request_id, tenant_id, encrypted_raw_json       │ │
│ └──────────────────────────────────┬─────────────────────────────────┘ │
│                                    │                                   │
│                        Streaming Engine (Spark)                        │
│                        • 5-min Micro-batches                           │
│                        • Decrypt & Redact PII (Regex/Presidio)         │
│                        • Tokenize & Write                              │
│                                    ▼                                   │
│ ┌────────────────────────────────────────────────────────────────────┐ │
│ │ 2. SILVER TIER (S3 Standard -> S3 Glacier Instant Retrieval - 7d)  │ │
│ │    • Đường dẫn: s3://silver/llm_calls_clean                        │ │
│ │    • Phân hoạch: partitionBy(date) + Z-ORDER BY(tenant_id)          │ │
│ │    • Định dạng: Delta với Deletion Vectors bật                      │ │
│ │    • Cấu trúc: request_id, ts, tenant_id, model, latency_ms,       │ │
│ │                prompt_redacted, response_redacted, tokens          │ │
│ └──────────────────────────────────┬─────────────────────────────────┘ │
│                                    │                                   │
│                           Daily Batch Engine                           │
│                           • Aggregate cost, latency, token count       │
│                           • Delete Silver partitions older than 7d    │
│                                    ▼                                   │
│ ┌────────────────────────────────────────────────────────────────────┐ │
│ │ 3. GOLD TIER (S3 Standard - Lifecycle: 1 năm)                      │ │
│ │    • Đường dẫn: s3://gold/llm_daily_metrics                        │ │
│ │    • Phân hoạch: partitionBy(year_month)                           │ │
│ │    • Định dạng: Delta (Compact, Z-ORDER BY tenant_id, model)       │ │
│ │    • Cấu trúc: date, tenant_id, model, latency_p50_p95, cost_usd   │ │
│ └────────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                         [ Query Engine (Trino/DuckDB) ]
                                    │
                                    ▼
                         [ BI Dashboard (Superset) ]
```

---

## 3. Quyết định kiến trúc & Các giải pháp bị loại bỏ (Trade-offs)

### Quyết định 1: Định dạng bảng — Chọn Delta Lake thay vì Apache Iceberg
- **Lý do chọn:** Delta Lake có cơ chế tích hợp **Z-Order Clustering** và **Deletion Vectors** rất tốt trên môi trường Apache Spark. Khả năng tối ưu hóa ghi nén (Compaction) và lọc cột (Data Skipping) giúp truy vấn tenant cụ thể cực nhanh.
- **Lựa chọn bị loại bỏ:**
  - *Apache Iceberg:* Mặc dù rất tốt cho môi trường multi-engine (Trino/Flink), việc triển khai Z-order/Clustering trong Iceberg phức tạp hơn và đòi hỏi nhiều cấu hình catalog ngoại vi hơn trong môi trường Spark thuần túy.
  - *Raw Parquet + Hive Metastore:* Loại bỏ vì không có hỗ trợ transaction log, không có Time Travel để khôi phục khi lỗi, và không hỗ trợ ACID MERGE khi cần cập nhật dữ liệu.

### Quyết định 2: Chiến lược phân hoạch (Partitioning & Clustering)
- **Lý do chọn:** Phân hoạch tầng Silver theo `date` và chạy **Z-ORDER BY (`tenant_id`)** hàng giờ. Điều này giúp tránh hiện tượng over-partitioning (chia quá nhiều thư mục nhỏ cho hàng nghìn tenant), nhưng vẫn tối ưu hóa Data Skipping khi lọc dữ liệu theo `tenant_id` trên dashboard.
- **Lựa chọn bị loại bỏ:**
  - *Phân hoạch trực tiếp theo `tenant_id`:* Loại bỏ vì sẽ tạo ra hàng chục nghìn thư mục con trên S3, dẫn đến vấn đề "Small-File Problem" nghiêm trọng và làm chậm metadata scan của Spark/Trino.

### Quyết định 3: Xử lý thông tin nhạy cảm (PII Redaction) tại Silver thay vì Bronze
- **Lý do chọn:** Bronze lưu trữ raw JSON thô nhưng được mã hoá bằng khoá AWS KMS của Security Team. Khi giải mã trong luồng Stream 5 phút để chuyển sang Silver, dữ liệu prompt/response được chạy qua hàm Redactor (Regex/NLP engine) để xóa PII trước khi lưu xuống Silver dưới dạng văn bản sạch. Nhờ đó, bất kỳ Analyst nào đọc Silver cũng không bị lộ PII.
- **Lựa chọn bị loại bỏ:**
  - *Redact PII tại API Gateway:* Làm tăng độ trễ (latency overhead) của luồng chat trực tiếp của người dùng cuối (SLA quan trọng nhất).
  - *Không lưu prompt thô ở Bronze:* Loại bỏ vì nếu không lưu thô được mã hoá ở Bronze, khi hệ thống Redactor lỗi hoặc cần audit bảo mật, ta không có nguồn dữ liệu gốc để đối chiếu lại.

### Quyết định 4: Cơ chế Lifecycle & FinOps Tiering dữ liệu
- **Lý do chọn:** 
  - Dữ liệu thô ở Bronze hết hạn sau 1 ngày.
  - Dữ liệu Silver lưu ở lớp Standard trong 3 ngày đầu (hot-path), sau đó tự động chuyển qua lớp **S3 Glacier Instant Retrieval** để giảm 60% chi phí nhưng vẫn đảm bảo khả năng truy vấn ngẫu nhiên khi cần incident review trong vòng 7 ngày. Sau 7 ngày, một daily job chạy lệnh `VACUUM` để xóa vĩnh viễn dữ liệu Silver.
  - Dữ liệu Gold chỉ chứa các chỉ số tổng hợp (kích thước siêu nhỏ, < 50 GB/năm) được giữ lại 1 năm trên S3 Standard.
- **Lựa chọn bị loại bỏ:**
  - *Giữ Silver ở S3 Standard trọn 7 ngày:* Chi phí lưu trữ 35 TB dữ liệu nóng sẽ vượt ngưỡng budget $5,000/tháng.

### Quyết định 5: Streaming Ingestion Engine
- **Lý do chọn:** Sử dụng Spark Structured Streaming với Trigger mỗi 5 phút ghi trực tiếp xuống Delta Table. Nhờ cơ chế Write-Ahead Log của Delta, ta đảm bảo ngữ nghĩa **Exactly-Once** mà không lo bị trùng lặp dữ liệu khi job bị restart.
- **Lựa chọn bị loại bỏ:**
  - *Lambda Architecture (Batch 1 tiếng):* Loại bỏ vì không đáp ứng được SLA refresh dữ liệu mỗi 5 phút của khách hàng.

---

## 4. Kịch bản lỗi lúc 3 giờ sáng (Failure Modes)

### Failure Mode 1: Lỗi tràn bộ nhớ (OOM) khi chạy compaction (OPTIMIZE) trên các bảng Silver lớn
- **Hiện tượng:** Job OPTIMIZE chạy ngầm lúc nửa đêm bị chết (OOM) do cố gắng gộp quá nhiều file parquet cùng một lúc trên một cluster Spark kích thước hạn chế.
- **Cách detect:** Cảnh báo qua Prometheus/Alertmanager khi bộ nhớ Executor đạt > 90% hoặc trạng thái Spark Application báo FAILED.
- **Cách xử lý:** 
  1. Hạn chế kích thước của compaction bằng cách chạy optimize theo phân vùng cụ thể: `OPTIMIZE delta.silver_table WHERE date = current_date()`.
  2. Bật tính năng **Auto-Compaction** và **Write-Tuning** (`delta.autoOptimize.optimizeWrite = true`) để Spark tự động chia file vừa phải ngay từ lúc ghi, giảm tải cho job OPTIMIZE ban đêm.

### Failure Mode 2: Schema Drift ở JSON đầu vào làm chết luồng Streaming
- **Hiện tượng:** Ứng dụng LLM đầu nguồn cập nhật phiên bản mới, thay đổi cấu trúc trường `usage` hoặc thêm các trường lồng nhau làm hàm `from_json` phân tích cú pháp trả về giá trị `null`, làm rỗng dữ liệu hoặc gây lỗi ghi bảng.
- **Cách detect:** Đếm tỉ lệ dòng bị `null` ở Silver so với Bronze. Nếu tỉ lệ `dòng lỗi / tổng dòng` vượt quá 5% trong 10 phút, kích hoạt cảnh báo Slack.
- **Cách xử lý:**
  1. Thiết kế bảng Silver chứa một cột dự phòng tên là `unparsed_data` để lưu trữ các bản ghi không phân tích được cấu trúc JSON.
  2. Sử dụng Delta Time Travel để sửa code parser và re-run lại luồng xử lý từ Bronze bắt đầu từ checkpoint lỗi mà không mất mát dữ liệu.

### Failure Mode 3: Trễ hạn chuyển đổi S3 Glacier khiến chi phí S3 Standard tăng đột biến
- **Hiện tượng:** Rule cấu hình S3 Lifecycle bị nghẽn hoặc không áp dụng đúng cách, làm 30 TB dữ liệu Silver cũ vẫn nằm trên lớp S3 Standard đắt đỏ, đe dọa vượt hạn mức ngân sách FinOps.
- **Cách detect:** Monitor AWS Budgets hàng ngày và sử dụng AWS Cost Explorer API để theo dõi chi tiêu của bucket Silver theo tag lưu trữ.
- **Cách xử lý:**
  1. Thực hiện lệnh `VACUUM` thủ công với thuộc tính `spark.databricks.delta.vacuum.parallelDelete.enabled` để dọn dẹp triệt để các file parquet cũ đã hết hạn.
  2. Cấu hình lại Lifecycle Rule với mức độ ưu tiên cao hơn trên AWS Console.

---

## 5. Ước lượng chi phí (FinOps Math)

### A. Chi phí lưu trữ S3 (Storage)
- **Tầng Bronze (Raw):** 5 TB/ngày, lưu 1 ngày $\rightarrow$ 5 TB.
  - Giá S3 Standard: $0.023/GB/tháng.
  - Chi phí Bronze: $5,000 \text{ GB} \times \$0.023 = \$115 / \text{tháng}$.
- **Tầng Silver (Cleaned):** ~4 TB/ngày sau khi nén và lọc bỏ metadata thừa.
  - Lưu 3 ngày ở S3 Standard $\rightarrow$ 12 TB. Chi phí: $12,000 \text{ GB} \times \$0.023 = \$276 / \text{tháng}$.
  - Lưu 4 ngày ở S3 Glacier Instant Retrieval $\rightarrow$ 16 TB.
    - Giá Glacier Instant Retrieval: $0.004/GB/tháng.
    - Chi phí: $16,000 \text{ GB} \times \$0.004 = \$64 / \text{tháng}$.
- **Tầng Gold (Aggregated):** < 10 GB/tháng. Lưu 1 năm $\rightarrow$ 120 GB.
  - Chi phí Gold: Negligible (< \$3/tháng).
- **Tổng chi phí Storage:** \$115 + \$276 + \$64 = **\$455 / tháng** (Rất an toàn so với budget \$5,000).

### B. Chi phí năng lực xử lý (Compute)
Phần ngân sách còn lại (~$4,500/tháng) sẽ được phân bổ cho Compute:
- Cluster Spark Structured Streaming chạy 24/7 (ví dụ dùng 3 nodes Spot Instances `m5.xlarge` trên AWS EMR).
  - Giá Spot Instance: ~$0.08/giờ/node + phí EMR $0.045/giờ $\rightarrow$ ~$0.125/giờ/node.
  - Chi phí 3 nodes chạy 24/7: $3 \times \$0.125 \times 730 \text{ giờ} = \$273.75 / \text{tháng}$.
- Cluster chạy Compaction & Daily Gold aggregation chạy 2 giờ/ngày (Spot cluster trung bình 8 nodes):
  - Chi phí batch: $8 \times \$0.125 \times 2 \text{ giờ} \times 30 \text{ ngày} = \$60 / \text{tháng}$.
- **Tổng chi phí Compute:** ~$335 / tháng.

$\Rightarrow$ **Tổng chi phí hệ thống (Storage + Compute):** ~$790/tháng. Điều này chứng minh kiến trúc vô cùng tối ưu và tiết kiệm 84% ngân sách FinOps được cấp.

---

## 6. Lộ trình xây dựng MVP (1 tuần)

Để chứng minh tính khả thi của kiến trúc này trong 1 tuần, ta thiết lập một luồng rút gọn:
1. **Ngày 1-2:** Thiết lập hạ tầng Docker Compose gồm Spark (1 master, 1 worker) kết nối với MinIO đóng vai trò Object Storage giả lập S3.
2. **Ngày 3:** Viết hàm xử lý PII Redaction sử dụng thư viện Python `Presidio` hoặc Regex đơn giản và kiểm thử tốc độ xử lý trên 10,000 dòng.
3. **Ngày 4:** Viết luồng Spark Structured Streaming đọc từ thư mục Bronze giả lập, chạy hàm Redactor và ghi xuống bảng Silver định dạng Delta.
4. **Ngày 5:** Chạy thử lệnh `OPTIMIZE ZORDER BY` trên bảng Silver và chạy các câu truy vấn lọc theo tenant để kiểm chứng việc Data skipping hoạt động tốt.
