use std::{path::PathBuf, process::Command, sync::Arc};

use anyhow::Result;
use clap::Parser;
use overlord_agent_common::IndexerService;
use overlord_agent_emule::{
    AgentExit, EmuleAgentConfig, OverlordAgentEmule, logging::init_file_logging,
};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "overlord-agent-emule", about = "Overlord Kad2/ED2K agent")]
struct Cli {
    #[arg(long, short)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(|| PathBuf::from("overlord.toml"));
    let config = EmuleAgentConfig::load(&config_path)?;
    let _logging_runtime = init_file_logging(&config)?;

    let agent = Arc::new(OverlordAgentEmule::new(config).await?);
    agent.start().await?;

    info!("starting overlord-agent-emule control server");
    match agent.serve().await? {
        AgentExit::Stopped => {}
        AgentExit::RestartRequested => {
            info!("restarting overlord-agent-emule process");
            restart_self(&config_path)?;
        }
    }
    Ok(())
}

fn restart_self(config_path: &PathBuf) -> Result<()> {
    let current_exe = std::env::current_exe()?;
    Command::new(current_exe)
        .arg("--config")
        .arg(config_path)
        .spawn()?;
    Ok(())
}
