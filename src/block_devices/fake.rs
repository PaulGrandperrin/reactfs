use super::BlockDevice;
use reactor::*;

struct FakeBlockDevice {

}

impl BlockDevice for FakeBlockDevice {
	fn send_read(req: ReadRequest) {}
	fn send_write(req: WriteRequest) {}
	fn send_flush(req: FlushRequest) {}
	fn recv_event() -> Option<FutureEvent> {None}
}