#[tokio::main]
async fn main() -> anyhow::Result<()> {
    overlord_tools::minirupnpc::run().await
}
