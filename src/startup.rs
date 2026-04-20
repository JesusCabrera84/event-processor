pub fn print_logo() {
    const LOGO: &str = include_str!("../assets/geminis-labs-logo.txt");
    // Structured logs must remain plain text without ANSI control sequences.
    const GRAY: &str = "\x1b[38;2;180;180;180m";
    const WHITE: &str = "\x1b[97m";
    const RESET: &str = "\x1b[0m";

    println!();
    println!("\t\t{WHITE}GeminiLabs :: Alert Processor{RESET}");
    println!("{GRAY}────────────────────────────────────────────────────────────────{RESET}");
    println!();

    use std::io::Write;
    if let Err(e) = std::io::stdout().write_all(LOGO.as_bytes()) {
        tracing::warn!(error = %e, "failed to print startup logo");
    }

    println!();
    println!("{GRAY}────────────────────────────────────────────────────────────────{RESET}");
    println!("\t\t{GRAY}alert-processor • @geminislabs{RESET}");
    println!();
    println!();
}

pub fn log_service_started(log_level: &str, health_bind_addr: &str) {
    let environment = std::env::var("APP_ENV")
        .or_else(|_| std::env::var("ENVIRONMENT"))
        .unwrap_or_else(|_| "unknown".to_string());

    tracing::info!(
        service = env!("CARGO_PKG_NAME"),
        version = env!("CARGO_PKG_VERSION"),
        environment = %environment,
        log_level = %log_level,
        health_bind_addr = %health_bind_addr,
        "service started"
    );
}
