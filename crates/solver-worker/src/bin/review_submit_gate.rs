#![allow(clippy::struct_excessive_bools)]

use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use solver_worker::review_submit_gate::{
    ReviewSubmitGateInput, ReviewSubmitGateStatus, verify_review_submit_gate,
};

#[derive(Debug, Parser)]
#[command(name = "review-submit-gate")]
#[command(about = "Verify the fast review-submit numerical stability gate.")]
struct Cli {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value_t = false)]
    fail_on_blocked: bool,
}

fn main() -> anyhow::Result<ExitCode> {
    let cli = Cli::parse();
    let input_bytes = fs::read(&cli.input)?;
    let input: ReviewSubmitGateInput = serde_json::from_slice(&input_bytes)?;
    let report = verify_review_submit_gate(&input);

    if let Some(parent) = cli.out.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&cli.out, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "[review_submit_gate] status={:?} blockers={} out={}",
        report.status,
        report.blockers.len(),
        cli.out.display()
    );

    if cli.fail_on_blocked && report.status == ReviewSubmitGateStatus::Blocked {
        Ok(ExitCode::from(2))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}
