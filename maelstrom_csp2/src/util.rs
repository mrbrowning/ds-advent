#[macro_export]
macro_rules! send {
    ($channel:expr, $value:expr, $msg:expr) => {
        if let Err(e) = $channel.send($value) {
            return Err(MaelstromError::ChannelError(format!($msg, e)));
        }
    };
}
