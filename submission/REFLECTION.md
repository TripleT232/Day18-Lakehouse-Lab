# Reflection — Lakehouse Anti-Patterns

Trong 5 anti-patterns của Data Lakehouse:
1. **treating lakehouse as a dump (Data Swamp):** Ghi đè dữ liệu vô tội vạ không qua phân tầng Bronze/Silver/Gold.
2. **Ignoring the Small-File Problem:** Không chạy OPTIMIZE/Z-ORDER định kỳ khiến hiệu năng giảm mạnh do quá nhiều file nhỏ.
3. **No Schema Governance:** Không dùng Schema Enforcement khiến dữ liệu sai lệch kiểu làm hỏng các báo cáo hạ nguồn.
4. **Bypassing the Transaction Log:** Tự ý sửa/xoá file parquet trực tiếp trong storage thay vì thông qua Delta Engine API.
5. **Lack of Partition Strategy:** Không phân hoạch bảng hoặc phân hoạch quá đà (over-partitioning) theo các cột có độ chọn lọc quá cao.

---

### Anti-pattern dễ vướng nhất và lý do:

Đối với dự án của chúng tôi, **"Ignoring the Small-File Problem"** và **"Bypassing the Transaction Log"** là hai nguy cơ lớn nhất. 

* **Ignoring the Small-File Problem:** Do hệ thống liên tục nhận các luồng logs cuộc gọi LLM ở dạng streaming (như giả lập ghi 200 batches ở NB2), dữ liệu đổ về liên tục dưới dạng các file parquet siêu nhỏ. Nếu không thiết lập cơ chế tự động chạy `OPTIMIZE` định kỳ (ví dụ qua Spark jobs hàng ngày hoặc Delta Auto-Optimize), hiệu năng đọc bảng Gold sẽ suy giảm nghiêm trọng theo thời gian.
* **Bypassing the Transaction Log:** Các kỹ sư dữ liệu thỉnh thoảng có thói quen vào trực tiếp hệ thống lưu trữ (MinIO/S3) để xoá bớt file parquet vật lý nhằm giải phóng dung lượng. Hành động này làm lệch catalog và phá vỡ tính toàn vẹn của Transaction Log của Delta Table, dẫn đến lỗi đọc dữ liệu.
