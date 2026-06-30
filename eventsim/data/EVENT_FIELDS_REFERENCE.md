# Eventsim - Danh sách trường dữ liệu

Eventsim sinh ra 4 loại event, mỗi loại ghi vào một Kafka topic riêng. Tất cả event đều ở định dạng JSON một dòng (single-line JSON).

---

## I. Base Fields (15 trường chung - có mặt trong tất cả event)

| # | Tên trường | Kiểu dữ liệu | Mô tả |
|:-:|:-----------|:-------------|:------|
| 1 | `ts` | Long (epoch ms) | Timestamp của event |
| 2 | `sessionId` | Long | ID của phiên làm việc |
| 3 | `level` | String | Loại tài khoản: `"free"` hoặc `"paid"` |
| 4 | `itemInSession` | Integer | Số thứ tự event trong phiên |
| 5 | `city` | String | Thành phố |
| 6 | `zip` | String | Mã ZIP |
| 7 | `state` | String | Mã bang (ví dụ: "CA", "NY") |
| 8 | `userAgent` | String | Chuỗi User-Agent của trình duyệt |
| 9 | `lon` | Double | Kinh độ (longitude) |
| 10 | `lat` | Double | Vĩ độ (latitude) |
| 11 | `userId` | Long | ID người dùng |
| 12 | `lastName` | String | Họ |
| 13 | `firstName` | String | Tên |
| 14 | `gender` | String | Giới tính: `"M"` hoặc `"F"` |
| 15 | `registration` | Long (epoch ms) | Thời điểm đăng ký tài khoản |

---

## II. 1. page_view_events — Sự kiện xem trang

**Kafka topic:** `page_view_events`

Bao gồm tất cả 15 base fields + 4 trường riêng:

| # | Tên trường | Kiểu dữ liệu | Mô tả / Giá trị |
|:-:|:-----------|:-------------|:----------------|
| 16 | `page` | String | Tên trang được xem |
| 17 | `auth` | String | Trạng thái xác thực |
| 18 | `method` | String | HTTP method: `"GET"`, `"PUT"` |
| 19 | `status` | Integer | HTTP status: `200`, `307`, `404` |

### Các trang (`page`) trong hệ thống:

| Trang | Mô tả |
|:------|:------|
| `Home` | Trang chủ |
| `About` | Trang giới thiệu |
| `Help` | Trang trợ giúp |
| `Settings` | Trang cài đặt |
| `NextSong` | Phát bài hát tiếp theo |
| `Login` | Trang đăng nhập |
| `Register` | Trang đăng ký |
| `Submit Registration` | Xác nhận đăng ký |
| `Logout` | Đăng xuất |
| `Upgrade` | Nâng cấp tài khoản |
| `Submit Upgrade` | Xác nhận nâng cấp |
| `Downgrade` | Hạ cấp tài khoản |
| `Submit Downgrade` | Xác nhận hạ cấp |
| `Cancel` | Hủy tài khoản |
| `Cancellation Confirmation` | Xác nhận hủy |
| `Save Settings` | Lưu cài đặt |
| `Error` | Trang lỗi |

### LƯU Ý: Trang `NextSong`

Khi `page = "NextSong"`, event **còn chứa thêm 3 trường nhạc** (thừa hưởng từ listen_events):

| Tên trường thêm | Kiểu | Mô tả |
|:----------------|:-----|:------|
| `artist` | String | Tên nghệ sĩ |
| `song` | String | Tên bài hát |
| `duration` | Double | Độ dài bài hát (giây) |

### Ví dụ NextSong page view:

```json
{
  "ts": 1767225705000,
  "sessionId": 264,
  "page": "NextSong",
  "auth": "Logged In",
  "method": "PUT",
  "status": 200,
  "level": "paid",
  "itemInSession": 19,
  "city": "North Smithfield",
  "zip": "02896",
  "state": "RI",
  "userAgent": "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 ...\"",
  "lon": -71.544069,
  "lat": 41.975186,
  "userId": 265,
  "lastName": "Compton",
  "firstName": "Ashton",
  "gender": "M",
  "registration": 1767225600000,
  "artist": "Jimmy Cliff",
  "song": "Million Teardrops",
  "duration": 216.45016
}
```

---

## III. 2. listen_events — Sự kiện nghe nhạc

**Kafka topic:** `listen_events`

Bao gồm tất cả 15 base fields + 4 trường riêng:

| # | Tên trường | Kiểu dữ liệu | Mô tả |
|:-:|:-----------|:-------------|:------|
| 16 | `artist` | String | Tên nghệ sĩ |
| 17 | `song` | String | Tên bài hát |
| 18 | `duration` | Double | Độ dài bài hát (giây) |
| 19 | `auth` | String | Trạng thái xác thực (luôn là `"Logged In"`) |

### Ví dụ:

```json
{
  "artist": "Dixie Chicks",
  "song": "Lullaby",
  "duration": 351.8428,
  "ts": 1767225784000,
  "sessionId": 1081,
  "auth": "Logged In",
  "level": "paid",
  "itemInSession": 12,
  "city": "Berkeley",
  "zip": "94702",
  "state": "CA",
  "userAgent": "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 ...\"",
  "lon": -122.286326,
  "lat": 37.865784,
  "userId": 562,
  "lastName": "Gonzalez",
  "firstName": "Ava",
  "gender": "F",
  "registration": 1767225600000
}
```

---

## IV. 3. auth_events — Sự kiện xác thực (đăng nhập/đăng ký)

**Kafka topic:** `auth_events`

Bao gồm tất cả 15 base fields + 1 trường riêng:

| # | Tên trường | Kiểu dữ liệu | Mô tả |
|:-:|:-----------|:-------------|:------|
| 16 | `success` | Boolean | `true` = thành công, `false` = thất bại |

### LƯU Ý QUAN TRỌNG: Auth thất bại

Khi `success = false`, các trường sau **sẽ bị thiếu** (null):
- `userId`
- `firstName`
- `lastName`
- `gender`
- `registration`

### Ví dụ thành công:

```json
{
  "ts": 1767226483000,
  "sessionId": 311,
  "level": "free",
  "itemInSession": 1,
  "city": "New London",
  "zip": "63459",
  "state": "MO",
  "userAgent": "\"Mozilla/5.0 (Windows NT 6.1) ...\"",
  "lon": -91.369252,
  "lat": 39.579374,
  "userId": 312,
  "lastName": "Moore",
  "firstName": "Lydia",
  "gender": "F",
  "registration": 1767225600000,
  "success": true
}
```

### Ví dụ thất bại:

```json
{
  "ts": 1767243001000,
  "sessionId": 608,
  "level": "free",
  "itemInSession": 1,
  "city": "Poughkeepsie",
  "zip": "12601",
  "state": "NY",
  "userAgent": "\"Mozilla/5.0 (iPad; ...) ...\"",
  "lon": -73.911521,
  "lat": 41.701908,
  "success": false
}
```

---

## V. 4. status_change_events — Sự kiện thay đổi trạng thái

**Kafka topic:** `status_change_events`

Bao gồm tất cả 15 base fields + 1 trường riêng:

| # | Tên trường | Kiểu dữ liệu | Mô tả |
|:-:|:-----------|:-------------|:------|
| 16 | `auth` | String | Trạng thái xác thực (luôn là `"Logged In"`) |

Các trường hợp thay đổi:
- **Upgrade:** `level` chuyển từ `"free"` → `"paid"` (qua trang Submit Upgrade)
- **Downgrade:** `level` chuyển từ `"paid"` → `"free"` (qua trang Submit Downgrade)
- **Cancel:** `auth` chuyển thành `"Cancelled"` (qua trang Cancel)

### Ví dụ:

```json
{
  "ts": 1767366951000,
  "sessionId": 1702,
  "auth": "Logged In",
  "level": "free",
  "itemInSession": 4,
  "city": "Leesville",
  "zip": "71446",
  "state": "LA",
  "userAgent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
  "lon": -93.186912,
  "lat": 31.166711,
  "userId": 765,
  "lastName": "Green",
  "firstName": "David",
  "gender": "M",
  "registration": 1767225600000
}
```

---

## VI. Trạng thái xác thực (`auth`)

| Giá trị | Ý nghĩa |
|:--------|:--------|
| `Guest` | Khách chưa đăng nhập |
| `Logged In` | Đã đăng nhập |
| `Logged Out` | Đã đăng xuất |
| `Cancelled` | Tài khoản đã bị hủy |

## VII. Loại tài khoản (`level`)

| Giá trị | Ý nghĩa |
|:--------|:--------|
| `free` | Tài khoản miễn phí |
| `paid` | Tài khoản trả phí |

---

## VIII. Tổng quan schema map

| Kafka Topic | Event Type | Base Fields | Extra Fields | Tổng |
|:------------|:-----------|:-----------:|:------------:|:----:|
| `page_view_events` | Page view | 15 | 4 (`page`, `auth`, `method`, `status`) + 3 nhạc nếu là NextSong | 19/22 |
| `listen_events` | Listen | 15 | 4 (`artist`, `song`, `duration`, `auth`) | 19 |
| `auth_events` | Auth | 15 | 1 (`success`) | 16 |
| `status_change_events` | Status change | 15 | 1 (`auth`) | 16 |
