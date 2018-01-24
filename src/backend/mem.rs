use std::sync::mpsc::{Sender, Receiver};
use std::ptr;

use reactor::*;
use reactor::{Event};

/* TODO
 - enforce block semantics (4k)
 - use logger
*/

/// This function implements a memory backend.
///
/// The function body is an infinite loop and therefor never returns.
/// It should then be called from inside a thread.
pub fn mem_backend_loop(sender: Sender<Event>, receiver: Receiver<BDRequest>, size: usize) {

    let mut mem = Vec::with_capacity(size);
    unsafe {
        mem.set_len(size);
    } 

    loop {
        let event = receiver.recv();

        match event {
            Ok(BDRequest::Read(r)) => {

                let result = if r.offset + r.length <= size as u64 {
                    let mut data = Vec::with_capacity(r.length as usize);
                    unsafe {
                        data.set_len(r.length as usize);
                        ptr::copy_nonoverlapping(mem[r.offset as usize..].as_ptr(), data.as_mut_ptr(), r.length as usize);
                    }
                    
                    Ok(FutureEvent::ReadResponse(ReadResponse {
                        data
                    }))
                } else {
                    Err(format_err!("mem backend: read operation ouside of limit: {} + {} > {}", r.offset, r.length, size))
                };

                let event = Event::ToFuture {
                    event_id: r.event_id,
                    task_id: r.task_id,
                    result
                };

                sender.send(event).unwrap();
            },
            Ok(BDRequest::Write(w)) => {
                
                let result = if w.offset + w.data.len() as u64 <= size as u64 {
                    unsafe {
                        ptr::copy_nonoverlapping(w.data.as_ptr(), mem[w.offset as usize..].as_mut_ptr(), w.data.len() as usize);
                    }
                    
                    Ok(FutureEvent::WriteResponse(WriteResponse{}))
                } else {
                    Err(format_err!("mem backend: write operation ouside of limit: {} + {} > {}", w.offset, w.data.len(), size))
                };

                let event = Event::ToFuture {
                    event_id: w.event_id,
                    task_id: w.task_id,
                    result
                };

                sender.send(event).unwrap();
            },
            Ok(BDRequest::Flush(f)) => {

                // no-op

                let event = 
                    Event::ToFuture {
                        event_id: f.event_id,
                        task_id: f.task_id,
                        result: Ok(FutureEvent::FlushResponse(FlushResponse{}))
                    };

                sender.send(event).unwrap();
            },
            Err(_) => {
                // the channel is closed, exit loop
                break;
            }
        }
    }
}
