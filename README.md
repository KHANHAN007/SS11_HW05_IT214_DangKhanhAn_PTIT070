# SS11 HW05 - WebClient và Kafka trong luồng thông báo

## 1. Tổng quan

Notification Service lắng nghe sự kiện `order.created` từ topic `storex-order-events`. Sau khi nhận sự kiện, service gọi User Preference API để lấy kênh thông báo, sau đó gọi Email API hoặc Zalo API bằng WebClient.

Toàn bộ luồng HTTP là reactive và không sử dụng `.block()`.

```text
Kafka: storex-order-events
          |
          v
Notification Consumer
          |
          |-- kiểm tra orderId trùng
          |
          v
GET Preference API ---- lỗi sau retry ----> fallback EMAIL
          |
          v
POST Email/Zalo API --- lỗi sau retry ----> storex-order-events.DLQ
```

## 2. WebClient Non-blocking

Consumer khởi động pipeline bằng `subscribe()`. Các bước phụ thuộc nhau được nối bằng `flatMap()`:

```java
preferenceClient.getPreferredChannel(event.userId())
    .flatMap(channel -> notificationClient.send(channel, event))
```

Không có lời gọi `.block()` trong Consumer hoặc các client. Trong thời gian chờ HTTP, thread không bị giữ để chờ phản hồi, nhờ đó Notification Service có thể tiếp nhận nhiều sự kiện đồng thời.

## 3. Timeout và Retry

WebClient sử dụng Reactor Netty với read timeout và response timeout là 3 giây. Mỗi lời gọi còn có toán tử:

```java
.timeout(Duration.ofSeconds(3))
.retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(1)))
```

`Retry.fixedDelay(2, ...)` nghĩa là tối đa 2 lần retry sau lần gọi ban đầu, mỗi lần cách nhau 1 giây. Chỉ lỗi timeout, lỗi kết nối và HTTP 5xx được retry. Lỗi HTTP 4xx không được retry vì thường là lỗi dữ liệu đầu vào.

## 4. BUG-05 - Preference API bị lỗi

Khi Preference API từ chối kết nối, timeout hoặc trả HTTP 5xx, WebClient retry tối đa 2 lần. Nếu vẫn không thành công, `onErrorResume` ghi log mức `WARN` và trả về kênh mặc định `EMAIL`:

```text
Không lấy được kênh của user ..., fallback sang EMAIL
```

Lỗi Preference API không làm gián đoạn luồng gửi thông báo.

## 5. BUG-06 - Chống xử lý trùng

`IdempotencyService` dùng `ConcurrentHashMap` với hai trạng thái:

- `PROCESSING`: order đang gọi API.
- `PROCESSED`: order đã gửi thông báo thành công.

`putIfAbsent` là thao tác nguyên tử. Nếu Kafka gửi lại `ORD-123` trong lúc bản đầu đang xử lý hoặc sau khi đã thành công, consumer bỏ qua ngay và không gọi API bên ngoài.

Theo phạm vi bài tập, dữ liệu được lưu trong RAM và sẽ mất khi service khởi động lại. Trong production nên dùng Redis hoặc database có unique constraint theo `orderId` để bảo đảm lũy đẳng giữa nhiều instance và qua các lần restart.

## 6. BUG-07 - Gửi thông báo thất bại

Email/Zalo API được retry tối đa 2 lần. Nếu vẫn thất bại, `NotificationWorkflow` không ném lỗi ra Kafka Consumer mà gọi `DlqPublisher` để gửi toàn bộ `OrderCreatedEvent` sang:

```text
storex-order-events.DLQ
```

Sau khi publish DLQ thành công, hệ thống ghi log mức `ERROR` đúng yêu cầu:

```text
Đã đẩy order {orderId} vào DLQ do lỗi gửi thông báo
```

Message lỗi không làm consumer chết cứng và các message khác vẫn tiếp tục được nhận.

## 7. Cấu hình Kafka

- Topic chính: `storex-order-events`.
- Consumer group: `notification-group`.
- DLQ: `storex-order-events.DLQ`.
- Topic chính và DLQ đều có 3 partition.
- `ErrorHandlingDeserializer` xử lý lỗi JSON trước khi listener được gọi.
- `trusted.packages: "*"` cho phép giải mã DTO gửi từ service khác.

`DefaultErrorHandler` được giữ để xử lý lỗi giải mã JSON hoặc lỗi đồng bộ ngoài dự kiến. Lỗi gửi Email/Zalo được xử lý trong reactive pipeline và publish chủ động sang cùng DLQ.

## 8. API bên ngoài

Service gọi các endpoint:

```text
GET  http://localhost:8081/api/preferences/{userId}
POST http://localhost:8082/api/notify/email
POST http://localhost:8082/api/notify/zalo
```

Ví dụ Preference API trả về:

```json
{"channel":"EMAIL"}
```

Ví dụ sự kiện Kafka:

```json
{
  "eventType": "order.created",
  "orderId": "ORD-123",
  "userId": "USER-01",
  "message": "Đơn hàng của bạn đã được tạo thành công"
}
```

## 9. Cách kiểm thử

1. Chạy Kafka tại `localhost:9092`.
2. Chạy API giả lập Preference tại port `8081`.
3. Chạy API giả lập Email/Zalo tại port `8082`.
4. Khởi động Notification Service.
5. Gửi sự kiện `order.created` vào topic `storex-order-events`.
6. Gửi lại cùng `orderId` để kiểm tra log bỏ qua message trùng.
7. Cho Preference API trả 500 để kiểm tra fallback Email.
8. Cho Email API liên tục trả 500 để kiểm tra retry và DLQ.

## 10. Build và test

```bash
./gradlew clean build
```

Test tự động xác nhận message trùng bị bỏ qua khi đang xử lý, sau khi thành công và có thể xử lý lại sau khi workflow thất bại.
