use anyhow::anyhow;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio::time::Duration;

const STOP_GRACE_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) async fn wait_interval(
    cancel: &CancellationToken,
    interval: f32,
) -> anyhow::Result<(), ()> {
    tokio::select! {
        _ = cancel.cancelled() => Err(()),
        _ = tokio::time::sleep(Duration::from_secs_f32(interval)) => Ok(())
    }
}

pub(crate) async fn wait_and_abort(handle: JoinHandle<()>) -> anyhow::Result<bool> {
    if handle.is_finished() {
        return Ok(false);
    }

    let mut handle = handle;
    match tokio::time::timeout(STOP_GRACE_TIMEOUT, &mut handle).await {
        Ok(res) => {
            match res {
                Ok(_) => Ok(false),
                Err(e) => Err(anyhow!(e)),
            }
        }
        Err(_) => {
            handle.abort();
            let _ = handle.await;
            Ok(true)
        }
    }
}
