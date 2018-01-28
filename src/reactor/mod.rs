//! This module implements the reactor pattern to allow asynchronous processing
//! of requests.
//!
//! I tried to write this module from first principle to better understand the
//! design decision behind `tokio`.
//! I did however stole many design ideas and tried to use as best as I could
//! the same terminology.
//!
//! This design is at the same time:
//!
//! * More specific and simpler because it is tailored for the block device use case
//!
//! * More generic and more complex because it is intented to be generic over the environment
//!   (kernel/nostd or userland/std) and the IO system.

/* TODO
 - factorize Future{Read,Write,Flush}::poll()
 - check ids boundaries
 - communicate to the outsite StreamIds
 - use slab instead of hashmap
 - maybe use future::try_ready! macro
 - implenent other fscalls
 - implement end of stream of fscalls
 - use a proper logger
 - write tests (quickcheck)
*/

#[cfg(test)]
mod tests;

extern crate futures;
//extern crate slab;

use std::fmt;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::sync::mpsc::{Sender, Receiver};
use std::collections::HashMap;

use futures::prelude::*;

use failure;
//use slab::Slab;


/// The ID of a `Stream`
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub u64);

/// The ID of an `Event`
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub u64);

/// The ID of a `Task`
///
/// The main task's ID is 0 (```reactor::Core::run()```).
///
/// `SpawnedTask`'s IDs starts at 1. 
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(pub u64);

/// Represents a spawned task.
///
/// Holds the `Future` it's running
pub struct SpawnedTask {
    future: Box<Future<Item=(), Error=()>>
}

impl fmt::Debug for SpawnedTask {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Box<Future>")
    }
}

/// Represents an event which destination is a `Future` 
#[derive(Debug)]
pub enum FutureEvent {
    ReadResponse(ReadResponse),
    WriteResponse(WriteResponse),
    FlushResponse(FlushResponse)
    /*
    ...
    */
}

/// Represents an event which destination is a `Stream` 
#[derive(Debug)]
pub enum StreamEvent {
    FSRequest(FSRequest)
    /*
    ...
    */
}

/// Represents an event to be sent to the `reactor`'s `Core`'s main loop
#[derive(Debug)]
pub enum Event {
    ToFuture {
        event_id: EventId,
        task_id: TaskId,
        result: Result<FutureEvent, failure::Error>
    },
    ToStream {
        stream_id: StreamId,
        task_id: TaskId,
        result: Result<StreamEvent, failure::Error>
    }
}

/// A block device read request
#[derive(Debug)]
pub struct ReadRequest {
    pub event_id: EventId,
    pub task_id: TaskId,
    pub offset: u64,
    pub length: u64
}


/// A block device write request
#[derive(Debug)]
pub struct WriteRequest {
    pub event_id: EventId,
    pub task_id: TaskId,
    pub offset: u64,
    pub data: Vec<u8>
}

/// A block device flush request
#[derive(Debug)]
pub struct FlushRequest {
    pub event_id: EventId,
    pub task_id: TaskId
}

/// A block device request
#[derive(Debug)]
pub enum BDRequest {
    Read(ReadRequest),
    Write(WriteRequest),
    Flush(FlushRequest)
}

/// A block device read response
#[derive(Debug)]
pub struct ReadResponse {
    pub data: Vec<u8>
}

/// A block device write response
#[derive(Debug)]
pub struct WriteResponse {
    pub len: u64
}

/// A block device flush response
#[derive(Debug)]
pub struct FlushResponse {
}

/// A filesystem request
#[derive(Debug)]
pub struct FSRequest {
}

/// A filesystem response
#[derive(Debug)]
pub struct FSResponse {
}

/// The `Inner` struct hold the `reactor` state which needs to be shared
/// between `Core`, `Handle` and the `Future`s and `Stream`s.
///
/// It's shared as a ```Rc<RefCell<Inner>>``` in `Core` and as
/// ```Weak<RefCell<Inner>>``` elsewhere.
#[derive(Debug)]
struct Inner {
    id_counter: u64, // incrementing counter of events and streams
    task_id_counter: u64,
    events_to_future: HashMap<EventId, Result<FutureEvent, failure::Error>>,
    events_to_streams: HashMap<StreamId, Vec<Result<StreamEvent, failure::Error>>>,
    newly_spawned_tasks: Vec<(TaskId, SpawnedTask)>,
    current_task_id: Option<TaskId>,
    
    // channels to which send block device requests and filesystem responses
    bd_sender: Sender<BDRequest>,
    fs_sender: Sender<FSResponse>,
}

impl Inner {
    fn new(bd_sender: Sender<BDRequest>, fs_sender: Sender<FSResponse>) -> Inner {
        Inner {
            id_counter: 0,
            task_id_counter: 1, // 0 is reserved to the main task_id
            events_to_future: HashMap::new(),
            events_to_streams: HashMap::new(),
            newly_spawned_tasks: Vec::new(),
            current_task_id: None,
            bd_sender,
            fs_sender
        }
    }
}


/// A handle to the `reactor::Inner` structure of `reactor::Core` (```Weak<RefCell<Inner>>```)
///
/// It's used to create new I/O `Future`s and `Stream`s and spawn new `SpawnedTask`s.
#[derive(Clone)]
pub struct Handle {
    inner: Weak<RefCell<Inner>>
}

impl Handle {
    /// Reads `length` bytes at `offset` and returns a `ReadFuture` which resolves to `Vec<u8>`.
    pub fn read(&self, offset: u64, length: u64) -> FutureRead {
        FutureRead {
            state: FutureReadState::NotYet {
                offset,
                length
            },
            inner: self.inner.clone()
        }
    }

    /// Writes `data` bytes at `offset` and returns a `WriteFuture` which resolves when the write is done`.
    pub fn write(&self, data: Vec<u8>, offset: u64) -> FutureWrite {
        FutureWrite {
            state: FutureWriteState::NotYet {
                data,
                offset
            },
            inner: self.inner.clone()
        }
    }

    /// Flush block device queue and returns a `FlushFuture` which resolves when the flush is done`.
    pub fn flush(&self) -> FutureFlush {
        FutureFlush {
            state: FutureFlushState::NotYet {
            },
            inner: self.inner.clone()
        }
    }

    /// Return a `FSCallStream` which resolves to `FSRequest`s.
    pub fn recv_fs_request(&self) -> FSCallStream {
        FSCallStream {
            state: FSCallStreamState::NotYet,
            inner: self.inner.clone()
        }
    }

    /// Sends `fs_response` back through the filesystem channel
    pub fn send_fs_response(&self, fs_response: FSResponse) {
        // get mut ref to inner
        let inner = self.inner.upgrade().unwrap();
        let inner = inner.borrow();

        inner.fs_sender.send(fs_response)
            .expect("reactor::Handle::send_fs_response: filesystem channel has been closed");
    }

    /// Spawns a new `SpawnedTask` in the event loop from a `Future` or a `Stream`
    ///
    /// # Return value
    ///
    /// This function returns the `TaskId` to which the `Future` as been assigned.
    /// It's useful to then send `Event`s to this `Future`.
    /// 
    /// We don't yet provide a way to retreive the `StreamId` of `Stream`s.
    /// 
    /// # About panics
    ///
    /// `SpawnedTask`s **do not** catch panics, it is the responsability of the `Future`
    /// to do so if needed. 
    pub fn spawn<F>(&self, mut f: F) -> TaskId
    where F: Future<Item=(), Error=()> + 'static {
        println!("handle: spawning new task");
        let saved_current_task_id;
        let task_id;
        {
            // get mut ref to inner
            let inner = self.inner.upgrade().unwrap();
            let mut inner = inner.borrow_mut();

            // check state
            assert_ne!(inner.current_task_id, None, "trying to spawn a task when the reactor is not running") ;

            // create task_id and make it current,
            // saving former current task_id
            task_id = TaskId(inner.task_id_counter);
            inner.task_id_counter += 1;
            saved_current_task_id = inner.current_task_id;
            inner.current_task_id = Some(task_id);
        }

        // poll the future which will register itself with the current_task_id
        match f.poll() {
            Ok(Async::NotReady) => {}, // nothing to do
            Ok(Async::Ready(())) |
            Err(()) => {return task_id}, // that task_id didn't have time to be instantiated but that's not a problem
        }

        // get mut ref to inner
        let inner = self.inner.upgrade().unwrap();
        let mut inner = inner.borrow_mut();

        // restore saved former current_task_id
        inner.current_task_id = saved_current_task_id;

        // create and push new task to the newly_spawned_tasks list
        let task = SpawnedTask{future: Box::new(f)};
        inner.newly_spawned_tasks.push((task_id, task));

        return task_id;
    }
}

/// The `Core` of the `reactor` containing the event loop.
///
/// Use ```Core::run()``` to start the `reactor` with a `Future`.
///
/// It holds a reference counter reference to `Inner` as ```Rc<RefCell<Inner>>```.
pub struct Core {
    receiver: Receiver<Event>,
    inner: Rc<RefCell<Inner>>,
}

impl Core {
    /// Creates a new `reactor`.
    ///
    /// Needs:
    ///
    /// * a channel to send block device requests
    ///
    /// * a channel to send filesystem responses
    ///
    /// * a channel to receive `Event`s: block device responses or filesystem requests
    pub fn new(bd_sender: Sender<BDRequest>, fs_sender: Sender<FSResponse>, receiver: Receiver<Event>) -> Core {
        Core {
            receiver,
            inner: Rc::new(RefCell::new(Inner::new(bd_sender, fs_sender)))
        }
    }

    /// Returns a `Handle` to the `Inner` state of the `reactor`.
    pub fn handle(&mut self) -> Handle {
        Handle {inner: Rc::downgrade(&self.inner)}
    }

    /// Runs a `future` until completion.
    ///
    /// # Return value
    ///
    /// Returns the `Result` of `future` if any.
    ///
    /// If `future` is never resolved, this function never returns
    pub fn run<F>(self, mut future: F) -> Result<F::Item, F::Error>
    where F: Future {

        // list of active tasks
        let mut tasks: HashMap<TaskId, SpawnedTask> = HashMap::new();
        {
            // borrow inner
            let mut inner = self.inner.borrow_mut();
            
            // initialize current_task_id to main task special id 0
            inner.current_task_id = Some(TaskId(0));
        }

        println!("reactor: polling future");
        match future.poll() {
                Ok(Async::Ready(r)) => return Ok(r),
                Err(e) => return Err(e),
                Ok(Async::NotReady) => {}
        }

        // event loop
        loop {
            let task_id_to_poll;
            {
                // borrow inner
                let mut inner = self.inner.borrow_mut();

                // move the new tasks to our map of tasks
                for (task_id, task) in inner.newly_spawned_tasks.drain(0..) {
                    tasks.insert(task_id, task);
                }

                // read event from channel
                println!("reactor: waiting on channel");
                let event = self.receiver.recv().unwrap();    
                //println!("reactor: received event {:?}", event);

                // process event and extract the task_id we need to poll
                task_id_to_poll = match event {
                    Event::ToFuture{event_id, task_id, result} => {
                        inner.events_to_future.insert(event_id, result);
                        // return extracted task_id
                        task_id
                    },
                    Event::ToStream{stream_id, task_id, result} => {
                        if let Some(vec) = inner.events_to_streams.get_mut(&stream_id) {
                            vec.push(result);
                        } else {
                            unreachable!("logic error in reactor: trying to add event to non-existing stream");
                        }
                        // return extracted task_id
                        task_id
                    }
                };

                // set current_task_id so that the future about to be polled knows
                // from which task it's called
                inner.current_task_id = Some(task_id_to_poll);
            }

            // if the task we're about to poll finishes, we'll remove it
            let mut task_finished = false;

            println!("reactor: polling future");
            match task_id_to_poll {
                // main task
                TaskId(0) => {
                    match future.poll() {
                        Ok(Async::Ready(r)) => return Ok(r),
                        Err(e) => return Err(e),
                        Ok(Async::NotReady) => {}
                    }
                },
                // spawned task
                TaskId(_) => {
                    match tasks.get_mut(&task_id_to_poll)
                            .expect("reactor: received event with unknown task_id").future.poll() {
                        Ok(Async::NotReady) => {},
                        Ok(Async::Ready(())) | 
                        Err(()) => {task_finished = true;}
                    }
                }
            }
            if task_finished {
                // the task is finished, remove it
                tasks.remove(&task_id_to_poll);
            }
        }
    }
}

#[derive(Clone, Debug)]
enum FutureReadState {
    NotYet{
        offset: u64,
        length: u64
    },
    Pending{
        event_id: EventId
    },
    Done
}

/// `Future` returned by `Handle::read()` which will resolve to a `Vec<u8>` on completion.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct FutureRead {
    state: FutureReadState,
    inner: Weak<RefCell<Inner>>
}

impl Future for FutureRead {
    type Item=Vec<u8>;
    type Error=failure::Error;
    
    fn poll(&mut self) -> futures::prelude::Poll<Self::Item, Self::Error> {
        // get mut ref to inner
        let inner = self.inner.upgrade().unwrap();
        let mut inner = inner.borrow_mut();

        let task_id = inner.current_task_id
            .expect("trying to poll a future when the reactor is not running");

        println!("FutureRead is polled with task_id={:?}", task_id);

        match self.state {
            // first time the future is polled, push command to queue
            FutureReadState::NotYet{offset, length} => {
                
                
                let event_id = EventId(inner.id_counter);
                inner.id_counter+=1;

                inner.bd_sender.send(BDRequest::Read(ReadRequest{event_id, task_id, offset, length}))
                    .expect("FutureRead::poll: block device channel has been closed");
                
                // update state
                self.state = FutureReadState::Pending{event_id};
                
                Ok(Async::NotReady)
            },
            // we are waiting for the result of the command
            FutureReadState::Pending{event_id} => {
                // if we have the result
                match inner.events_to_future.remove(&event_id) {
                    Some(Ok(FutureEvent::ReadResponse(ReadResponse{data}))) => {
                        // update state
                        self.state = FutureReadState::Done;
                        
                        Ok(Async::Ready(data))
                    },
                    Some(Err(e)) => {
                        // update state
                        self.state = FutureReadState::Done;

                        Err(e)
                    }
                    None => {
                        Ok(Async::NotReady)
                    },
                    _ => {
                        unreachable!("logic error in reactor: mismatch of event type");
                    }
                }
            },
            FutureReadState::Done => {
                panic!("FutureRead polled but already done");
            }
        }
    }
}

#[derive(Clone, Debug)]
enum FutureWriteState {
    NotYet{
        offset: u64,
        data: Vec<u8>
    },
    Pending{
        event_id: EventId
    },
    Done
}

impl FutureWriteState {
    // this is a hack to be able to change to state in-place while moving its content out
    #[inline]
    pub fn replace<T, F: FnOnce(Self) -> (Self, T)>(&mut self, f: F) -> T {
        use std::mem;
        let mut tmp: Self = unsafe{mem::uninitialized()};
        mem::swap(&mut tmp, self);
        let r = f(tmp);
        tmp = r.0;
        mem::swap(&mut tmp, self);
        mem::forget(tmp);
        return r.1;
    }
}

/// `Future` returned by `Handle::write()` which will resolve to `()` when completed.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct FutureWrite {
    state: FutureWriteState,
    inner: Weak<RefCell<Inner>>
}

impl Future for FutureWrite {
    type Item=u64;
    type Error=failure::Error;
    
    fn poll(&mut self) -> futures::prelude::Poll<Self::Item, Self::Error> {
        // get mut ref to inner
        let inner = self.inner.upgrade().unwrap();
        let mut inner = inner.borrow_mut();

        let task_id = inner.current_task_id
            .expect("trying to poll a future when the reactor is not running");

        println!("FutureWrite is polled with task_id={:?}", task_id);

        self.state.replace(|state| {
            match state {
                // first time the future is polled, push command to queue
                FutureWriteState::NotYet{offset, data} => {
                    
                    
                    let event_id = EventId(inner.id_counter);
                    inner.id_counter+=1;

                    inner.bd_sender.send(BDRequest::Write(WriteRequest{event_id, task_id, offset, data}))
                        .expect("FutureWrite::poll: block device channel has been closed");
                    
                    // update state and return status
                    (FutureWriteState::Pending{event_id}, Ok(Async::NotReady))
                },
                // we are waiting for the result of the command
                FutureWriteState::Pending{event_id} => {
                    // if we have the result
                    match inner.events_to_future.remove(&event_id) {
                        Some(Ok(FutureEvent::WriteResponse(WriteResponse{len}))) => {
                            // update state and return status
                            (FutureWriteState::Done, Ok(Async::Ready(len)))
                        },
                        Some(Err(e)) => {
                            // update state and return status
                            (FutureWriteState::Done, Err(e))
                        }
                        None => {
                            (FutureWriteState::Pending{event_id}, Ok(Async::NotReady))
                        },
                        _ => {
                            unreachable!("logic error in reactor: mismatch of event type");
                        }
                    }
                },
                FutureWriteState::Done => {
                    panic!("FutureWrite polled but already done");
                }
            }
        })
    }
}

#[derive(Clone, Debug)]
enum FutureFlushState {
    NotYet{
    },
    Pending{
        event_id: EventId
    },
    Done
}

/// `Future` returned by `Handle::flush()` which will resolve to `()` when completed.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct FutureFlush {
    state: FutureFlushState,
    inner: Weak<RefCell<Inner>>
}

impl Future for FutureFlush {
    type Item=();
    type Error=failure::Error;
    
    fn poll(&mut self) -> futures::prelude::Poll<Self::Item, Self::Error> {
        // get mut ref to inner
        let inner = self.inner.upgrade().unwrap();
        let mut inner = inner.borrow_mut();

        let task_id = inner.current_task_id
            .expect("trying to poll a future when the reactor is not running");

        println!("FutureFlush is polled with task_id={:?}", task_id);

        match self.state {
            // first time the future is polled, push command to queue
            FutureFlushState::NotYet{} => {
                
                
                let event_id = EventId(inner.id_counter);
                inner.id_counter+=1;

                inner.bd_sender.send(BDRequest::Flush(FlushRequest{event_id, task_id}))
                    .expect("FutureFlush::poll: block device channel has been closed");
                
                // update state
                self.state = FutureFlushState::Pending{event_id};
                
                Ok(Async::NotReady)
            },
            // we are waiting for the result of the command
            FutureFlushState::Pending{event_id} => {
                // if we have the result
                match inner.events_to_future.remove(&event_id) {
                    Some(Ok(FutureEvent::FlushResponse(FlushResponse{}))) => {
                        // update state
                        self.state = FutureFlushState::Done;
                        
                        Ok(Async::Ready(()))
                    },
                    Some(Err(e)) => {
                        // update state
                        self.state = FutureFlushState::Done;

                        Err(e)
                    }
                    None => {
                        Ok(Async::NotReady)
                    },
                    _ => {
                        unreachable!("logic error in reactor: mismatch of event type");
                    }
                }
            },
            FutureFlushState::Done => {
                panic!("FutureFlush polled but already done");
            }
        }
    }
}


#[derive(Debug)]
enum FSCallStreamState {
    NotYet,
    Pending{
        stream_id: StreamId
    },
    _Done // TODO: implement done state
}

/// `Stream` returned by `Handle::get_fs_call()` which will resolve to `FSRequest`s.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct FSCallStream {
    state: FSCallStreamState,
    inner: Weak<RefCell<Inner>>
}

impl Stream for FSCallStream {
    type Item = FSRequest;
    type Error = failure::Error;

    fn poll(&mut self) -> futures::prelude::Poll<Option<Self::Item>, Self::Error> {
        println!("FSCallStream: got polled");
        // get mut ref to inner
        let inner = self.inner.upgrade().unwrap();
        let mut inner = inner.borrow_mut();

        inner.current_task_id.expect("trying to poll a future when the reactor is not running");

        match self.state {
            FSCallStreamState::NotYet => {
                // if this stream is being spawned, save its id
                let stream_id = StreamId(inner.id_counter);
                inner.id_counter+=1;
                
                inner.events_to_streams.insert(stream_id, Vec::new());

                // update state
                self.state = FSCallStreamState::Pending{stream_id};

                Ok(Async::NotReady)
            },
            FSCallStreamState::Pending{stream_id} => {
                // return one request if we have one
                if let Some(vec) = inner.events_to_streams.get_mut(&stream_id) {
                    match vec.pop() {
                        Some(Ok(StreamEvent::FSRequest(fs_request))) => {
                            Ok(Async::Ready(Some(fs_request)))
                        },
                        Some(Err(e)) => {
                            Err(e)
                        }
                        // TODO implement end-of-stream event
                        None => {
                            Ok(Async::NotReady)
                        }
                    } 
                } else {
                    unreachable!("logic error in reactor: trying to get event from non-existing stream");
                }
            },
            FSCallStreamState::_Done => {
                panic!("polled finished Stream");
            }
        }
        
    }
}

