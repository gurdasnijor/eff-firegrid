use anyhow::{Context, Result};
use s2_sdk::{
    S2,
    types::{ListBasinsInput, S2Config},
};

#[tokio::main]
async fn main() -> Result<()> {
    match std::env::var("S2_ACCESS_TOKEN").or_else(|_| std::env::var("S2_TOKEN")) {
        Ok(access_token) => {
            let client =
                S2::new(S2Config::new(access_token)).context("failed to create S2 client")?;
            let output = client
                .list_basins(ListBasinsInput::new())
                .await
                .context("failed to list S2 basins")?;

            println!(
                "firegrid-host: native s2 client listed {} basins",
                output.values.len()
            );
        }
        Err(_) => {
            println!("firegrid-host: set S2_ACCESS_TOKEN to run the native S2 smoke check");
        }
    }

    Ok(())
}
