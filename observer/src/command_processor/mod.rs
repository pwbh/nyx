mod command;

use self::command::{Command, CommandName};

#[derive(Debug)]
pub struct CommandProcessor {
    buf: String,
}

impl CommandProcessor {
    pub fn new() -> Self {
        Self { buf: String::new() }
    }

    pub fn process_raw_command(&mut self) -> Result<String, String> {
        std::io::stdin().read_line(&mut self.buf).unwrap();
        let command = Command::from(&self.buf)?;
        self.buf.clear();
        self.process_command(&command)
    }

    pub fn process_command(&self, command: &Command) -> Result<String, String> {
        match command {
            Command {
                name: CommandName::Connect,
                ..
            } => return self.handle_connect_command(command),

            _ => {
                return Err(
                    "unrecognized command has been passsed, please provide a relevant name."
                        .to_string(),
                )
            }
        }
    }

    fn handle_connect_command(&self, command: &Command) -> Result<String, String> {
        let hostname = match command.arguments.iter().next() {
            Some(hostname) => hostname,
            None => return Err("hostname was not provided.".to_string()),
        };

        // do some logic for connecting

        Ok("OK".to_string())
    }
}
