1. Nhóm Mở Vị Thế: LONG / SHORT
Đây là tín hiệu bắt đầu một chu kỳ giao dịch mới. TradingView chỉ gửi tín hiệu này khi trạng thái đang là rỗng (Flat).

Logic xử lý tại MT5:

Kiểm tra trạng thái: Đảm bảo symbol này hiện không có vị thế mở nào. Nếu có lệnh cũ đang kẹt, cần đóng ngay.

Tính toán Volume: Gọi hàm convert_quantity_to_lots để dịch quantity từ payload sang số Lot của MT5 (như đã thảo luận, dùng math.floor để làm tròn xuống).

Vào lệnh: Bắn lệnh ORDER_TYPE_BUY (với LONG) hoặc ORDER_TYPE_SELL (với SHORT) theo Market Price.

Gắn Stop Loss: Lấy giá trị sl từ payload và set cứng (Hard SL) lên server MT5 cho vị thế vừa mở để phòng ngừa đứt kết nối mạng.

Lưu State (Tùy chọn): Lưu lại Ticket ID của lệnh này vào biến bộ nhớ/database để dễ dàng truy xuất cho các bước chốt lời sau.

2. Nhóm Chốt Lời Từng Phần: TP1
Tín hiệu này báo hiệu giá đã chạm mốc an toàn đầu tiên. TV muốn đóng một phần vị thế (ví dụ 30%) và giữ lại phần còn lại (Runner).

Logic xử lý tại MT5:

Kiểm tra vị thế: Query MT5 xem vị thế của symbol này còn tồn tại không. (Nếu giá giật quá nhanh cắn SL cứng trước đó thì bỏ qua webhook này).

Tính toán Volume cắt: Dùng convert_quantity_to_lots để tính số Lot cần cắt từ quantity của webhook.

Đóng lệnh từng phần (Partial Close): Bắn một Market Order ngược chiều.

Nếu đang có vị thế BUY: Bắn lệnh SELL với khối lượng vừa tính.

Nếu đang có vị thế SELL: Bắn lệnh BUY với khối lượng vừa tính.

Lưu ý MT5: Phải truyền đúng position (Ticket ID của lệnh gốc) vào thuộc tính position của request để MT5 hiểu đây là lệnh đóng 1 phần, không phải mở lệnh đối xung (hedging).

Cập nhật SL (Quan trọng): Dù TradingView có tự dời SL nội bộ, bạn nên gửi một request TRADE_ACTION_SLTP lên MT5 để dời mức Hard SL của phần lệnh còn lại về điểm hòa vốn (Breakeven) để đảm bảo an toàn tuyệt đối.

3. Nhóm Kết Thúc Toàn Bộ: TP2 / SL / R_SL
Dù tên gọi khác nhau (Chốt phần còn lại, Cắt lỗ toàn bộ, Cắt lỗ dời), bản chất của 3 action này đối với SDK MT5 là hoàn toàn giống nhau: Xóa sổ toàn bộ những gì đang còn mở của mã đó.

Logic xử lý tại MT5:

Bỏ qua số Quantity: TUYỆT ĐỐI KHÔNG dùng quantity từ webhook để tính Lot cho bước này nhằm tránh lỗi sót lot lẻ (dust lot) do làm tròn.

Lấy Volume thực tế: Query trực tiếp API MT5 để lấy chính xác số Lot hiện đang còn mở của vị thế đó (position.volume).

Đóng toàn bộ (Full Close): Bắn lệnh Market ngược chiều với khối lượng bằng đúng position.volume vừa lấy được.

Dọn dẹp State: Xóa Ticket ID khỏi bộ nhớ/database. Sẵn sàng đón chu kỳ LONG/SHORT tiếp theo.

Tóm tắt Luồng thực thi (Execution Flow)
Webhook Action	Loại Lệnh MT5	Khối lượng (Volume) MT5	Cập nhật Stop Loss
LONG / SHORT	Market Order (Mở mới)	Dùng hàm convert_quantity_to_lots tính từ Webhook	Set SL ban đầu từ Payload
TP1	Market Order (Đóng 1 phần)	Dùng hàm convert_quantity_to_lots tính từ Webhook	Cập nhật dời SL về hòa vốn
TP2 / SL / R_SL	Market Order (Đóng toàn bộ)	Lấy 100% Volume đang mở trên MT5, bỏ qua Webhook	Lệnh bị xóa, không cần set