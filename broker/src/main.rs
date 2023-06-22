fn main() {
    print_colored(
        r" ___ __  __ ____    ____  _____ ______     _______ ____  
|_ _|  \/  |  _ \  / ___|| ____|  _ \ \   / / ____|  _ \ 
 | || |\/| | |_) | \___ \|  _| | |_) \ \ / /|  _| | |_) |
 | || |  | |  __/   ___) | |___|  _ < \ V / | |___|  _ < 
|___|_|  |_|_|     |____/|_____|_| \_\ \_/  |_____|_| \_\",
        105,
    )
}

fn print_colored(text: &str, color: usize) {
    let t = format!("\x1b[38;5;{}m{}\x1b[0m", color, text);
    // Should start trying to connect to the observer in intervals until success
    println!("{}", t)
}
