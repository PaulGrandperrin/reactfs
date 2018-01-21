use reactor::*;
mod fake;

pub trait BlockDevice {
	fn send_read(req: ReadRequest);
	fn send_write(req: WriteRequest);
	fn send_flush(req: FlushRequest);
	fn recv_event() -> Option<FutureEvent>;
}