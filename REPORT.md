# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyen Hoang Cuong |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602473 |
| GitHub | 5cuong |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-07900 |

## Kết quả cuối

- `dataset_version` của bạn: `ea0aed65a2e3`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `748 / 142 / 129`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Ở Bronze, A dùng `ts_utc` ISO-8601 UTC, `speed_mps` (m/s), `heading_deg`, `lat/lon`, `alt_m`, `acc_x/acc_y`, `gas` và `steer`; B dùng `timestamp_ms` epoch-millisecond, `spd_kmh` (km/h), `hdg`, `latitude/longitude`, `altitude`, `ax/ay`, `throttle_pct`, `brake_pct` và `steering`. Giữ riêng hai bảng và để giá trị thô dạng chữ giúp bảo toàn biểu diễn nguồn, truy dấu lỗi và tránh ép kiểu theo một schema sai trước khi quy tắc chuẩn hóa được áp dụng.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging A parse `ts_utc` thành timestamp và giữ tốc độ m/s; staging B đổi epoch-ms sang timestamp, đổi km/h thành m/s bằng phép chia 3.6, ánh xạ tên cột và trừ clock offset 1.500 ms. Bắt đầu B sau chỉnh là `2025-10-08 14:30:00.009+00:00`, nằm trong sai lệch cho phép 0,5 giây so với video. A nhận 3.021 dòng, sạch 2.968 và reject 53; B nhận 3.234, sạch 3.181 và reject 53. Mỗi drive có 20 duplicate, 30 `speed_missing`, 3 `speed_out_of_range`. Giữ rejects kèm lý do giúp kiểm toán và sửa nguồn, thay vì làm mất dữ liệu âm thầm.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` chỉ báo warn vì mất mẫu cảm biến làm giảm độ phủ/độ chính xác ghép thời gian nhưng dữ liệu còn lại vẫn dùng được và cần điều tra. `in_range` là error vì tốc độ ngoài 0–70 m/s có thể làm hỏng dữ liệu huấn luyện. Trong `break_gate.json`, nới max lên 1.000 m/s làm `in_range_silver_telemetry_speed_mps__70__0` FAIL với 6 dòng; kết quả là PASS=5, WARN=1, ERROR=1. Khôi phục luật gốc trả về PASS=6, WARN=1, ERROR=0. Như vậy warn giữ tín hiệu để theo dõi, còn error chặn dữ liệu sai đi tiếp.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

`idempotency.json` ghi 1.246 dòng trước và sau hai lần chạy lại, fingerprint `6c7480dfde7e` giữ nguyên. Mô phỏng append cho trip `trip-00003` có 60 frame tạo 120 dòng sau hai lần ghi (2x). Delta overwrite theo predicate `trip_id` thay partition đó trong một commit, nên retry không nhân đôi; cách xóa rồi ghi riêng có thể để partition trống nếu tiến trình lỗi ở giữa.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Frame đen có cường độ gần bằng 0 trên toàn ảnh nên không mang tín hiệu cảnh; frame tối do hầm/bóng râm vẫn có nội dung nên chỉ gắn cờ và giữ trong Silver. Bước 3 thấy 6 frame đen ở A, 4 ở B và 61 frame tối ở B; Gold loại frame đen. Tôi chọn ngưỡng dHash 2 làm điểm cân bằng: ngưỡng 0 giữ gần như mọi biến thể, còn 6 loại nhiều frame khác nhau thật hơn. Xe chạy chậm tạo nhiều frame liền nhau gần giống nhau; khi xe chạy nhanh hơn, cảnh thay đổi nhiều nên ít frame bị coi là gần trùng hơn.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Dữ liệu trong `leakage.json` có 1.019 frame thuộc 26 trip. Chia ngẫu nhiên theo frame làm cả 26/26 trip xuất hiện ở nhiều split và có frame ở cả train lẫn test; chia theo trip làm 0 trip bị cắt. Vì các frame liền nhau tương tự nhau, mô hình có thể thấy cùng cảnh ở train rồi gặp lại ở test, khiến điểm đánh giá cao giả tạo.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là hash nội dung để nhận diện chính xác dữ liệu và cấu hình tạo dataset, không phụ thuộc ngày chạy hoặc số lượng dòng có thể giống nhau. Hai lần export với cùng dữ liệu/MSSV cho cùng version `ea0aed65a2e3`. MSSV tham gia quy tắc chia trip, nên đổi MSSV có thể đổi split, tập frame và hash version dù đầu vào gốc giống nhau.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest cho version `ea0aed65a2e3` cần được đối chiếu trực tiếp sau lần chạy cuối: phần `lineage` liệt kê version của các bảng Bronze/Silver và SHA-256 từng video. Cùng với MSSV, luật chia trip, luật lọc frame, hash manifest và các bảng upstream có version, các thông tin này cho phép kiểm tra và tái tạo chính xác dataset thay vì chỉ dựa vào ngày tạo.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Run Airflow dùng trip `trip-07900`, map index 21, và log có `simulated crash while extracting trip-07900`. Lúc lỗi có 1 task `silver_frames` đỏ và 7 task downstream `upstream_failed`. Tôi clear đúng task đỏ; map index 21 chạy lại thành công, không phải chạy lại 26 task trích frame. Tuy nhiên run hiện vẫn `failed` vì 7 task downstream chưa tự tiếp tục; tôi đang giữ chúng nguyên trạng theo chỉ dẫn clear task đỏ. Version Gold trước lỗi là `ea0aed65a2e3`; chưa thể kết luận DAG đã publish lại version sau retry cho tới khi downstream chạy xong.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi tăng lên 500 xe/ngày, nút thắt đầu tiên nhiều khả năng là CPU/IO trích video và ghi frame vào một MinIO đơn, tiếp theo là số mapped task/metadata của Airflow, rồi truy vấn/ghi mart vào Postgres đơn. Tôi sẽ tăng worker trích frame theo hàng đợi có giới hạn song song, phân vùng object theo ngày/xe và đặt lake trên object storage co giãn; sau đó chuyển warehouse sang hệ phục vụ phân tích có khả năng scale-out. Đây là suy luận từ các chặng hiện có; cần đối chiếu nội dung cụ thể của slide 12, 22, 23 nếu chúng nêu công nghệ bắt buộc.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

SQL đã thêm vào dashboard: `select split, frames, trips, drives, avg_speed_kmh, round(frames::numeric / nullif(trips,0), 1) as frames_per_trip from marts.mart_split_balance order by split`. Kết quả: train 748 frame/18 trip (41,6 frame mỗi trip), val 142/4 (35,5), test 129/4 (32,3). Nó giúp vận hành nhìn cả quy mô split lẫn mức frame trung bình trên mỗi trip; train nhiều hơn vì có nhiều trip hơn, còn phép chia theo trip vẫn giữ nhóm đánh giá độc lập.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều bất ngờ nhất là telemetry B lệch đồng hồ 1,5 giây nhưng sau chuẩn hóa mẫu đầu vẫn khớp video trong 9 ms. Cổng chất lượng giải thích rõ dữ liệu bị cách ly thay vì làm mất. Một lỗi còn có thể lọt là cảm biến ghi sai nhưng vẫn nằm trong miền hợp lệ, chẳng hạn tốc độ hợp lý nhưng lệch đồng hồ biến thiên theo thời gian; cần thêm kiểm tra drift/đối chiếu với video hoặc cảm biến độc lập.
