mod command;

use self::command::{Command, CommandName};
use std::str::SplitAsciiWhitespace;

pub struct CommandProcessor {}

impl CommandProcessor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn process_raw_command(&self, raw_command: &str) -> Result<String, String> {
        let command = Command::from(raw_command)?;
        self.process_command(&command)
    }

    pub fn process_command(&self, command: &Command) -> Result<String, String> {
        match command {
            Command {
                name: CommandName::Connect,
                ..
            } => return Ok("executed".to_string()),

            _ => {
                return Err(
                    "unrecognized command has been passsed, please provide a relevant name."
                        .to_string(),
                )
            }
        }
    }

    fn handle_connect_command(
        &self,
        tokens: &mut SplitAsciiWhitespace<'_>,
    ) -> Result<String, String> {
        let hostname = match tokens.next() {
            Some(hostname) => hostname,
            None => return Err("hostname was not provided.".to_string()),
        };

        // do some logic for connecting

        Ok("OK".to_string())
    }
}
