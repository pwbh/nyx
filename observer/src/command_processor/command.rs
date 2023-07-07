pub enum CommandName {
    Create,
    List,
}

pub struct Command {
    pub name: CommandName,
    pub arguments: Vec<String>,
}

impl Command {
    pub fn from(raw_command: &str) -> Result<Self, String> {
        let mut tokens = raw_command.split_ascii_whitespace();
        let command = tokens.next().unwrap();

        let name = match command {
            "CREATE" => CommandName::Create,
            "LIST" => CommandName::List,
            _ => return Err("unrecognized command has been passed.".to_string()),
        };

        Ok(Self {
            name,
            arguments: tokens.map(|s| s.to_string()).collect(),
        })
    }
}
