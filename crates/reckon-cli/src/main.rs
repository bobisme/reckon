fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = asupersync::runtime::RuntimeBuilder::new().build()?;
    runtime.block_on(async {
        Ok(())
    })
}
