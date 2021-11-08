/// Helper macro that simplifies the usage of [`ServerHandle::recv_msg`]
macro_rules! wait_for_msg {
    ($handle: expr, $matcher:pat $(=> $result:expr)?) => {
        $handle.recv_msg(|msg| match msg {
            $matcher => ::std::option::Option::Some(($($result),*)),
            _ => None,
        }).await
    }
}
pub(in crate::tests::integration) use wait_for_msg;
