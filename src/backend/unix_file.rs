use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::sync::mpsc::{Sender, Receiver};

use reactor::*;
use reactor::{Event};

/* TODO
 - enforce block semantics (4k)
 - use logger
*/

/// This function implements a unix file backend.
///
/// The function body is an infinite loop and therefor never returns.
/// It should then be called from inside a thread.
///
/// All data is written to the file `bd.raw`.
///
/// All operations are recorded in the file `bd.log`.
pub fn unix_file_backend_loop(sender: Sender<Event>, receiver: Receiver<BDRequest>) {
    
    let mut bd = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("bd.raw").expect("failed to open bd.raw");

    let mut log = OpenOptions::new()
        .read(false)
        .append(true)
        .create(true)
        .open("bd.log").expect("failed to open bd.log");

    loop {
        let event = receiver.recv();
        write!(log, "received : {:?}\n", event).unwrap();

        match event {
            Ok(BDRequest::Read(r)) => {

                bd.seek(SeekFrom::Start(r.offset)).unwrap();
                let mut data = vec![0;r.length as usize]; // maybe do not initialize it
                let res = bd.read_exact(&mut data);

                let result = match res {
                    Ok(()) => 
                        Ok(FutureEvent::ReadResponse(ReadResponse {
                                data
                        }))
                    ,
                    Err(e) => 
                        Err(e.into())
                };

                let event = Event::ToFuture {
                    event_id: r.event_id,
                    task_id: r.task_id,
                    result
                };

                write!(log, "sent: {:?}\n", event).unwrap();

                sender.send(event).unwrap();
            },
            Ok(BDRequest::Write(w)) => {
                
                bd.seek(SeekFrom::Start(w.offset)).unwrap();
                let res = bd.write_all(&w.data);

                let result = match res {
                    Ok(()) => 
                        Ok(FutureEvent::WriteResponse(WriteResponse{len: w.data.len() as u64}))
                    ,
                    Err(e) => 
                        Err(e.into())
                };

                let event = Event::ToFuture {
                    event_id: w.event_id,
                    task_id: w.task_id,
                    result
                };

                write!(log, "sent: {:?}\n", event).unwrap();

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

                write!(log, "sent: {:?}\n", event).unwrap();

                sender.send(event).unwrap();
            },
            Err(_) => {
                // the channel is closed, exit loop
                break;
            }
        }
    }
}
