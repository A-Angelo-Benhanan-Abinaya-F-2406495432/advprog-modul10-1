use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, mpsc::{self, Receiver, SyncSender}},
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    thread,
    time::Duration,
};

struct SharedState {
    completed: bool,
    waker: Option<Waker>,
}

pub struct TimerFuture {
    shared_state: Arc<Mutex<SharedState>>,
}

impl Future for TimerFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared_state = self.shared_state.lock().unwrap();
        if shared_state.completed {
            Poll::Ready(())
        } else {
            shared_state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl TimerFuture {
    pub fn new(duration: Duration) -> Self {
        let shared_state = Arc::new(Mutex::new(SharedState {
            completed: false,
            waker: None,
        }));
        let thread_shared_state = shared_state.clone();
        thread::spawn(move || {
            thread::sleep(duration);
            let mut shared_state = thread_shared_state.lock().unwrap();
            shared_state.completed = true;
            if let Some(waker) = shared_state.waker.take() {
                waker.wake();
            }
        });
        TimerFuture { shared_state }
    }
}

struct Task {
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>,
    task_sender: SyncSender<Arc<Task>>,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task")
    }
}

unsafe fn clone_task(data: *const ()) -> RawWaker {
    let arc = unsafe { Arc::from_raw(data as *const Task) };
    std::mem::forget(arc.clone());
    std::mem::forget(arc);
    make_raw_waker(data)
}

unsafe fn wake_task(data: *const ()) {
    let arc = unsafe { Arc::from_raw(data as *const Task) };
    arc.task_sender.send(arc.clone()).expect("too many tasks");
}

unsafe fn wake_task_by_ref(data: *const ()) {
    let arc = unsafe { Arc::from_raw(data as *const Task) };
    arc.task_sender.send(arc.clone()).expect("too many tasks");
    std::mem::forget(arc);
}

unsafe fn drop_task(data: *const ()) {
    drop(unsafe { Arc::from_raw(data as *const Task) });
}

fn make_raw_waker(data: *const ()) -> RawWaker {
    static VTABLE: RawWakerVTable = RawWakerVTable::new(
        |d| unsafe { clone_task(d) },
        |d| unsafe { wake_task(d) },
        |d| unsafe { wake_task_by_ref(d) },
        |d| unsafe { drop_task(d) },
    );
    RawWaker::new(data, &VTABLE)
}

struct Executor {
    ready_queue: Receiver<Arc<Task>>,
}

#[derive(Clone)]
struct Spawner {
    task_sender: SyncSender<Arc<Task>>,
}

fn new_executor_and_spawner() -> (Executor, Spawner) {
    const MAX_QUEUED_TASKS: usize = 10_000;
    let (task_sender, ready_queue) = mpsc::sync_channel(MAX_QUEUED_TASKS);
    (Executor { ready_queue }, Spawner { task_sender })
}

impl Spawner {
    fn spawn(&self, future: impl Future<Output = ()> + 'static + Send) {
        let future = Box::pin(future);
        let task = Arc::new(Task {
            future: Mutex::new(Some(future)),
            task_sender: self.task_sender.clone(),
        });
        self.task_sender.send(task).expect("too many tasks queued");
    }
}

impl Executor {
    fn run(&self) {
        while let Ok(task) = self.ready_queue.recv() {
            let mut future_slot = task.future.lock().unwrap();
            if let Some(mut future) = future_slot.take() {
                let raw = Arc::into_raw(task.clone()) as *const ();
                let raw_waker = make_raw_waker(raw);
                let waker = unsafe { Waker::from_raw(raw_waker) };
                let context = &mut Context::from_waker(&waker);
                if future.as_mut().poll(context).is_pending() {
                    *future_slot = Some(future);
                }
            }
        }
    }
}

fn main() {
    let (executor, spawner) = new_executor_and_spawner();

    spawner.spawn(async {
        println!("Angelo Benhanan Abinaya Fuun's Komputer: howdy!");
        TimerFuture::new(Duration::new(2, 0)).await;
        println!("Angelo Benhanan Abinaya Fuun's Komputer: done!");
    });
    spawner.spawn(async {
        println!("Angelo Benhanan Abinaya Fuun's Komputer: howdy2!");
        TimerFuture::new(Duration::new(2, 0)).await;
        println!("Angelo Benhanan Abinaya Fuun's Komputer: done2!");
    });
    spawner.spawn(async {
        println!("Angelo Benhanan Abinaya Fuun's Komputer: howdy3!");
        TimerFuture::new(Duration::new(2, 0)).await;
        println!("Angelo Benhanan Abinaya Fuun's Komputer: done3!");
    });

    println!("Angelo Benhanan Abinaya Fuun's Komputer: hey hey");

    // drop(spawner);
    executor.run();
}