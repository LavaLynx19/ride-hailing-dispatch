pub mod client {
    tonic::include_proto!("dispatch.client");
}

pub mod shard {
    tonic::include_proto!("dispatch.shard");
}

#[cfg(test)]
mod tests {
    use super::client::{DriverToServer, LatLng, LocationPing, driver_to_server};
    use prost::Message;

    #[test]
    fn location_ping_round_trips() {
        let message = DriverToServer {
            message: Some(driver_to_server::Message::LocationPing(LocationPing {
                driver_id: "driver-1".into(),
                position: Some(LatLng { lat: 40.75, lng: -73.98 }),
                reported_at_ms: 1_000,
            })),
        };

        let decoded = DriverToServer::decode(message.encode_to_vec().as_slice()).unwrap();

        assert_eq!(decoded, message);
    }
}
