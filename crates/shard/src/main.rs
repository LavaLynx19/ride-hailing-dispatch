use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let usage = || std::io::Error::other("usage: shard DRIVER_ADDR RIDER_ADDR");
    let driver_addr = args.next().ok_or_else(usage)?;
    let rider_addr = args.next().ok_or_else(usage)?;
    let drivers = TcpListener::bind(&driver_addr).await?;
    let riders = TcpListener::bind(&rider_addr).await?;
    eprintln!("listening: drivers {driver_addr}, riders {rider_addr}");
    shard::server::serve(drivers, riders).await;
    Ok(())
}
