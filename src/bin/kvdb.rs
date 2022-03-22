use clap::app_from_crate;
use clap::crate_authors;
use clap::crate_description;
use clap::crate_name;
use clap::crate_version;
use kvdb::error::*;
use kvdb::server::sql_server::Server;
use kvdb::storage;
use serde_derive::Deserialize;

#[tokio::main]
async fn main() -> Result<()> {
    let opts = app_from_crate!()
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .help("Configuration file path")
                .takes_value(true)
                .default_value("example/kvdb.yaml"),
        )
        .get_matches();
    let cfg = Config::new(opts.value_of("config").unwrap())?;
    let loglevel = cfg.log_level.parse::<simplelog::LevelFilter>()?;
    let mut logconfig = simplelog::ConfigBuilder::new();
    if loglevel != simplelog::LevelFilter::Debug {
        logconfig.add_filter_allow_str("kvdb");
    }
    simplelog::SimpleLogger::init(loglevel, logconfig.build())?;

    // TODO. it should using a new sql_store
    let _path = std::path::Path::new(&cfg.data_dir);
    let _sync = &cfg.sync;

    let sql_store: Box<dyn storage::Store> = match cfg.storage_sql.as_str() {
        "memory" | "" => Box::new(storage::b_tree::Memory::new()),
        // TODO. a new sql_store on disk
        name => {
            return Err(Error::Config(format!(
                "can not support sql storage engine {}",
                name
            )))
        }
    };
    Server::new(&cfg.id, sql_store)
        .await?
        .listen(&cfg.listen_sql)
        .await?
        .server()
        .await
}

#[derive(Debug, Deserialize)]
struct Config {
    id: String,
    data_dir: String,
    sync: bool,
    listen_sql: String,
    log_level: String,
    storage_sql: String,
}

impl Config {
    pub fn new(file: &str) -> Result<Self> {
        let mut c = config::Config::new();
        c.set_default("id", "kvdb")?;
        c.set_default("data_dir", "/var/lib/kvdb")?;
        c.set_default("sync", true)?;
        c.set_default("listen_sql", "0.0.0.0:9605")?;
        c.set_default("log_level", "info")?;
        c.set_default("storage_sql", "memory")?;

        c.merge(config::File::with_name(file))?;
        c.merge(config::Environment::with_prefix("KVDB"))?;
        Ok(c.try_into()?)
    }
}
