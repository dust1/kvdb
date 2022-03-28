use clap::app_from_crate;
use clap::crate_authors;
use clap::crate_description;
use clap::crate_name;
use clap::crate_version;
use kvdb::client::Client;
use kvdb::error::Result;
use kvdb::storage::mvcc::TransactionMode;
use log::info;
use rustyline::validate::Validator;
use rustyline::Editor;
use rustyline::Modifiers;
use rustyline_derive::Completer;
use rustyline_derive::Helper;
use rustyline_derive::Highlighter;
use rustyline_derive::Hinter;

#[tokio::main]
async fn main() -> Result<()> {
    let opts = app_from_crate!()
        .arg(
            clap::Arg::with_name("host")
                .short("h")
                .long("host")
                .help("Host to connect to")
                .takes_value(true)
                .required(true)
                .default_value("127.0.0.1"),
        )
        .arg(
            clap::Arg::with_name("port")
                .short("p")
                .long("port")
                .help("Port number to connect to")
                .takes_value(true)
                .required(true)
                .default_value("9601"),
        )
        .get_matches();

    let mut kvsql = KVSQL::new(
        opts.value_of("host").unwrap(),
        opts.value_of("port").unwrap().parse()?,
    )
    .await?;

    kvsql.run().await
}

struct KVSQL {
    // conn client
    client: Client,
    // multiline editing
    editor: Editor<InputValidator>,
    history_path: Option<std::path::PathBuf>,
}

impl KVSQL {
    async fn new(host: &str, port: u16) -> Result<Self> {
        Ok(Self {
            client: Client::new((host, port)).await?,
            editor: Editor::new(),
            history_path: std::env::var_os("HOME")
                .map(|home| std::path::Path::new(&home).join(".kvsql.history")),
        })
    }

    /// run the kvsql REPL
    async fn run(&mut self) -> Result<()> {
        if let Some(path) = &self.history_path {
            match self.editor.load_history(path) {
                Ok(()) => {}
                Err(rustyline::error::ReadlineError::Io(ref err))
                    if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
        self.editor.set_helper(Some(InputValidator));
        // make sure multiline pastes are interpreted as normal inputs
        self.editor.bind_sequence(
            rustyline::KeyEvent(rustyline::KeyCode::BracketedPasteStart, Modifiers::NONE),
            rustyline::Cmd::Noop,
        );

        while let Some(input) = self.prompt()? {
            match self.execute(&input).await {
                Ok(()) => {}
                error @ Err(kvdb::error::Error::Internal(_)) => return error,
                Err(err) => println!("Error: {}", err.to_string()),
            }
        }

        // the operation record will not be saved until the client is closed
        if let Some(path) = &self.history_path {
            self.editor.save_history(path)?;
        }
        Ok(())
    }

    async fn execute(&mut self, command: &str) -> Result<()> {
        info!("kvsql: {}", command);
        todo!()
    }

    /// prompt the user for input
    fn prompt(&mut self) -> Result<Option<String>> {
        // read client transaction info, and reflected on the command line
        let prompt = match self.client.txn() {
            Some((id, TransactionMode::ReadWrite)) => format!("kvdb:{}> ", id),
            Some((id, TransactionMode::ReadOnly)) => format!("kvdb:{}> ", id),
            Some((_, TransactionMode::Snapshot { version })) => format!("kvdb@{}> ", version),
            None => "kvdb> ".into(),
        };
        match self.editor.readline(&prompt) {
            Ok(input) => {
                // save command to history file
                self.editor.add_history_entry(&input);
                Ok(Some(input.trim().to_string()))
            }
            Err(rustyline::error::ReadlineError::Eof)
            | Err(rustyline::error::ReadlineError::Interrupted) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator;

impl Validator for InputValidator {}
