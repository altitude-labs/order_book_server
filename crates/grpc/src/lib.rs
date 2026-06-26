pub mod orderbook {
    #![allow(warnings)]
    tonic::include_proto!("orderbook.v1");
}

mod server;

pub use server::run_grpc_server;
