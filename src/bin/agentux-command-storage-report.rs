//! Reports the Office event log's size and growth as JSON (read-only).

use std::process::ExitCode;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let url = std::env::var("AGENTUX_COMMAND_DATABASE_URL").unwrap_or_default();
    let code =
        agentux_command::report::run(&args, &url, &mut std::io::stdout(), &mut std::io::stderr())
            .await;
    ExitCode::from(code as u8)
}
