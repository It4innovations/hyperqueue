use crate::connection::Connection;
use crate::internal::datasrv::messages::{FromDataClientMessage, ToDataClientMessage};

pub(crate) type ToDataClientConnection = Connection<FromDataClientMessage, ToDataClientMessage>;
