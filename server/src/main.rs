fn main() {
    print_colored(
        r"___ __  __ ____  
|_ _|  \/  |  _ \ 
 | || |\/| | |_) |
 | || |  | |  __/ 
|___|_|  |_|_|    
 
Message queue server",
        105,
    )
}

fn print_colored(text: &str, color: usize) {
    let t = format!("\x1b[38;5;{}m{}\x1b[0m", color, text);
    println!("{}", t)
}
