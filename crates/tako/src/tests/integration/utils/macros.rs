/// Helper macro that simplifies the usage of [`ServerHandle::recv_msg`]
#[macro_export]
macro_rules! wait_for_msg {
    ($handle: expr, $matcher:pat $(=> $result:expr)?) => {
        $handle.recv_msg(|msg| match msg {
            $matcher => $crate::tests::integration::utils::server::MessageFilter::Expected(($($result),*)),
            _ => $crate::tests::integration::utils::server::MessageFilter::Unexpected(msg),
        }).await
    };
    ($handle: expr, $matcher:pat if $guard:expr $(=> $result:expr)?) => {
        $handle.recv_msg(|msg| match msg {
            $matcher if $guard => $crate::tests::integration::utils::server::MessageFilter::Expected(($($result),*)),
            _ => $crate::tests::integration::utils::server::MessageFilter::Unexpected(msg),
        }).await
    }
}

/// Helper macro that simplifies the usage of [`ServerHandle::recv_msg_with_timeout`]
#[macro_export]
macro_rules! try_wait_for_msg {
    ($handle: expr, $timeout: expr, $matcher:pat $(=> $result:expr)?) => {
        $handle.recv_msg_with_timeout(|msg| match msg {
            $matcher => $crate::tests::integration::utils::server::MessageFilter::Expected(($($result),*)),
            _ => $crate::tests::integration::utils::server::MessageFilter::Unexpected(msg),
        }, $timeout).await
    };
    ($handle: expr, $matcher:pat if $guard:expr $(=> $result:expr)?) => {
        $handle.recv_msg_with_timeout(|msg| match msg {
            $matcher if $guard => $crate::tests::integration::utils::server::MessageFilter::Expected(($($result),*)),
            _ => $crate::tests::integration::utils::server::MessageFilter::Unexpected(msg),
        }, $timeout).await
    }
}
pub(in crate::tests::integration) use wait_for_msg;
