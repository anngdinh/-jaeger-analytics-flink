## To do

- [x] xóa bớt hashmap lưu SpanToService khi ko dùng nhiều nữa, tràn heap
- [x] xử lí ghi 5s 1 lần, ghi nhiều có thể es chịu ko nổi
- [ ] xử lí những trường hợp null khi span cha vào trước span con
- [ ] ý của anh Tấn: dùng window event time để chọn trace trong vòng 1 phút, group by traceID sort bởi end timestamp để tìm được span con trước span cha.
- [ ] luồng ci/cd





