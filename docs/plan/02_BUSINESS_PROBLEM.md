# 02. Bài toán của đơn vị và sự cần thiết nghiên cứu


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Bối cảnh đơn vị giả định

Một hệ thống thương mại điện tử dùng PostgreSQL để lưu dữ liệu giao dịch đơn hàng. Các thao tác đặt hàng, thanh toán, cập nhật trạng thái giao hàng và hủy đơn diễn ra liên tục.

Đội vận hành và kinh doanh cần theo dõi các chỉ số gần thời gian thực như:

- Số lượng đơn hàng mới.
- Doanh thu theo thời gian.
- Số đơn theo trạng thái.
- Tỷ lệ hủy đơn.
- Các thay đổi bất thường trong vận hành.

## Vấn đề

PostgreSQL là DBMS phù hợp cho OLTP vì xử lý giao dịch nhanh và nhất quán. Tuy nhiên, các truy vấn phân tích như `COUNT`, `SUM`, `GROUP BY`, thống kê theo thời gian có thể quét nhiều dòng và tiêu tốn CPU/RAM/I/O.

Nếu chạy trực tiếp báo cáo phân tích trên PostgreSQL production, hệ thống có thể bị ảnh hưởng:

- Tăng latency cho giao dịch người dùng.
- Làm chậm thao tác insert/update/delete.
- Tăng tải database OLTP.
- Khó mở rộng khi dữ liệu lớn.

## Giải pháp đề xuất

Xây dựng pipeline CDC:

```text
PostgreSQL OLTP → Flink CDC → ClickHouse OLAP
```

Flink CDC đọc thay đổi từ WAL của PostgreSQL và ghi sang ClickHouse. ClickHouse xử lý truy vấn phân tích, giúp tách tải OLAP khỏi hệ thống giao dịch.

## Người dùng phân tích

| Nhóm | Nhu cầu |
|---|---|
| Vận hành | Theo dõi đơn mới, đơn hủy, đơn lỗi |
| Kinh doanh | Doanh thu, trạng thái đơn, tỷ lệ hủy |
| Quản lý | Dashboard tổng quan |
| Kỹ thuật dữ liệu | Kiểm tra pipeline, mất/trùng/chậm dữ liệu |

## Yêu cầu độ trễ

Vì đề bài chưa nêu con số cụ thể, v1 đặt mục tiêu thực nghiệm: **dữ liệu xuất hiện ở ClickHouse trong vòng dưới 10 giây** sau khi thay đổi ở PostgreSQL.

## Đoạn văn có thể đưa vào báo cáo

Trong hệ thống thương mại điện tử, PostgreSQL được sử dụng để lưu trữ dữ liệu giao dịch đơn hàng. Tuy nhiên, các truy vấn phân tích trực tiếp trên PostgreSQL có thể gây tải lớn và ảnh hưởng đến giao dịch người dùng. Vì vậy, đề tài xây dựng pipeline CDC bằng Apache Flink CDC để đồng bộ dữ liệu từ PostgreSQL sang ClickHouse theo thời gian gần thực. ClickHouse đóng vai trò OLAP sink, hỗ trợ truy vấn phân tích nhanh và tách tải phân tích khỏi hệ thống OLTP.
