#![allow(clippy::struct_excessive_bools)]

use std::fs;
use std::path::PathBuf;

use clap::Parser;
use solver_worker::readiness::{
    ComputeAnomalyPolicy, MatrixReadinessInput, MatrixReadinessPolicy, verify_matrix_readiness,
};

#[derive(Debug, Parser)]
#[command(name = "matrix-readiness")]
#[command(about = "Verify provider closure, graph readiness, and compute stability.")]
struct Cli {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value_t = false)]
    allow_equal_fallback: bool,
    #[arg(long, default_value_t = false)]
    allow_medium_singular_risk: bool,
    #[arg(long, default_value_t = false)]
    allow_high_singular_risk: bool,
    #[arg(long, default_value_t = false)]
    no_require_lcia_factors: bool,
    #[arg(long, default_value_t = false)]
    no_factorization: bool,
    #[arg(long)]
    sample_solve_unit_limit: Option<usize>,
    #[arg(long, value_enum)]
    negative_lcia_policy: Option<CliComputeAnomalyPolicy>,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum CliComputeAnomalyPolicy {
    Ignore,
    Warning,
    Blocker,
}

impl From<CliComputeAnomalyPolicy> for ComputeAnomalyPolicy {
    fn from(value: CliComputeAnomalyPolicy) -> Self {
        match value {
            CliComputeAnomalyPolicy::Ignore => Self::Ignore,
            CliComputeAnomalyPolicy::Warning => Self::Warning,
            CliComputeAnomalyPolicy::Blocker => Self::Blocker,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let input_bytes = fs::read(&cli.input)?;
    let mut input: MatrixReadinessInput = serde_json::from_slice(&input_bytes)?;
    input.policy = merge_policy_overrides(input.policy, &cli);
    let report = verify_matrix_readiness(&input);

    if let Some(parent) = cli.out.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&cli.out, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "[matrix_readiness] status={:?} next_action={} blockers={} findings={} out={}",
        report.status,
        report.next_action,
        report.blockers.len(),
        report.findings.len(),
        cli.out.display()
    );
    Ok(())
}

fn merge_policy_overrides(mut policy: MatrixReadinessPolicy, cli: &Cli) -> MatrixReadinessPolicy {
    if cli.allow_equal_fallback {
        policy.allow_equal_fallback = true;
    }
    if cli.allow_medium_singular_risk {
        policy.allow_medium_singular_risk = true;
    }
    if cli.allow_high_singular_risk {
        policy.allow_high_singular_risk = true;
    }
    if cli.no_require_lcia_factors {
        policy.require_lcia_factors = false;
    }
    if cli.no_factorization {
        policy.run_factorization = false;
    }
    if let Some(limit) = cli.sample_solve_unit_limit {
        policy.sample_solve_unit_limit = limit;
    }
    if let Some(negative_lcia_policy) = cli.negative_lcia_policy {
        policy.negative_lcia_policy = negative_lcia_policy.into();
    }
    policy
}
