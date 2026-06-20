# 03. Lựa chọn nguồn dữ liệu


> Phiên bản v1 đã chốt: chỉ dùng bảng `orders`; DELETE xử lý theo soft delete bằng `deleted`; ClickHouse lưu trạng thái mới nhất theo `order_id`; mục tiêu latency dưới 10 giây; tài liệu vừa để triển khai vừa để viết báo cáo.


## Tiêu chí chọn dữ liệu

Dữ liệu phải phù hợp với CDC và dễ triển khai:

- Có `INSERT`, `UPDATE`, `DELETE`.
- Có khóa chính để đảm bảo idempotent.
- Có timestamp để xử lý event-time.
- Dễ sinh dữ liệu test nhiều mức tải.
- Dễ giải thích nghiệp vụ.
- Không phụ thuộc dữ liệu nhạy cảm.

## Các phương án cân nhắc

| Phương án | Ưu điểm | Nhược điểm | Đánh giá |
|---|---|---|---|
| E-commerce orders | Có transaction rõ, dễ I/U/D, dễ phân tích | Cần tự sinh dữ liệu | Chọn |
| Banking/payment | Rất hợp CDC | Nhạy cảm, khó lấy dữ liệu | Không chọn |
| Logistics | Trạng thái thay đổi liên tục | Phức tạp hơn orders | Dự phòng |
| Real estate listing | Có dữ liệu public | Ít update/delete tự nhiên | Không tối ưu |
| IoT sensor | Dễ stream | Thường insert-only | Không hợp yêu cầu DELETE/UPDATE |
| Dataset public tĩnh | Có sẵn | Không tự tạo CDC event | Chỉ dùng làm seed |

## Quyết định v1

Chọn **synthetic e-commerce orders**, tức tự sinh dữ liệu đơn hàng vào PostgreSQL.

Lý do:

- Chủ động tạo `INSERT`, `UPDATE`, soft delete.
- Dễ đo latency/throughput.
- Dễ tái lập test.
- Không phụ thuộc API bên ngoài.
- Phù hợp với bảng `orders` đã có.

## Nguồn mở rộng sau này

Có thể dùng API/dataset sau để seed dữ liệu mở rộng:

- DummyJSON: products, users, carts.
- FakeStoreAPI: products, users, carts.
- Northwind: customers, orders, products.
- Online Retail: dữ liệu bán lẻ tĩnh.
- TPC-H: benchmark OLAP, phù hợp mở rộng phần phân tích.

## Kết luận

Dữ liệu đơn hàng thương mại điện tử là lựa chọn phù hợp nhất vì vừa có tính giao dịch, vừa dễ phát sinh thay đổi, vừa dễ phân tích trong OLAP. Phiên bản v1 dùng bảng `orders`; các bảng khác sẽ đưa vào phần mở rộng.
