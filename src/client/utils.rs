#[macro_export]
macro_rules! rpc_call {
    ($conn:expr, $message:expr, $matcher:pat $(=> $result:expr)?) => {
        async {
            match $conn.send_and_receive($message).await? {
                $matcher => $crate::Result::Ok(($($result),*)),
                msg => {
                    $crate::common::error::error(format!("Received an invalid message {:?}", msg))
                }
            }
        }
    };
}
