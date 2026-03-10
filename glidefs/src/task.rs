use std::future::Future;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

pub fn spawn_named<T, F>(_name: &str, future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    {
        tokio::task::Builder::new()
            .name(_name)
            .spawn(future)
            .expect("failed to spawn task")
    }
    #[cfg(not(tokio_unstable))]
    {
        tokio::spawn(future)
    }
}

#[allow(dead_code)]
pub fn spawn_named_on<T, F>(_name: &str, future: F, handle: &Handle) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    {
        tokio::task::Builder::new()
            .name(_name)
            .spawn_on(future, handle)
            .expect("failed to spawn task")
    }
    #[cfg(not(tokio_unstable))]
    {
        handle.spawn(future)
    }
}

pub fn spawn_blocking_named<T, F>(_name: &str, f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    {
        tokio::task::Builder::new()
            .name(_name)
            .spawn_blocking(f)
            .expect("failed to spawn blocking task")
    }
    #[cfg(not(tokio_unstable))]
    {
        tokio::task::spawn_blocking(f)
    }
}
