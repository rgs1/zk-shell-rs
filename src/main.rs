#![feature(duration)]

extern crate getopts;
extern crate zookeeper;

use std::env;

use getopts::Options;

mod shell;

use shell::Shell;


fn usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief[..]));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut hosts = "".to_string();
    let mut opts = Options::new();

    opts.optopt("", "hosts", "hosts string", "HOSTS");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(_) => {
            usage(&program[..], opts);
            return;
        }
    };

    if matches.opt_present("hosts") {
        hosts = matches.opt_str("hosts").unwrap();
    }

    let mut shell = Shell::new(&*hosts);
    shell.run();
}
