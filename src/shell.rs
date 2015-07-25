use std::io::stdin;
use std::io::stdout;
use std::io::Write;
use std::str;
use std::time::Duration;

use zookeeper::{Watcher, WatchedEvent, ZooKeeper};


struct MyWatcher;

impl Watcher for MyWatcher {
    fn handle(&self, _e: &WatchedEvent) {
        // println!("{:?}", e)
    }
}

pub struct Shell {
    hosts: String,
    zk: Option<ZooKeeper>,
    session_timeout: u64,
}

impl Shell {
    pub fn new(hosts: &str) -> Shell {
        Shell {
            hosts: hosts.to_string(),
            zk: None,
            session_timeout: 5
        }
    }

    pub fn run(&mut self) {
        if !self.hosts.is_empty() {
            println!("Connecting...");
            let zk = ZooKeeper::connect(
                &self.hosts,
                Duration::from_secs(self.session_timeout),
                MyWatcher).unwrap();
            self.zk = Some(zk);
        }

        loop {
            let mut line = String::new();

            print!("> ");
            let _ = stdout().flush();

            stdin()
                .read_line(&mut line)
                .ok()
                .expect("Failed to read line");

            let pieces: Vec<&str>  = line.trim().split_whitespace().collect();

            if pieces.len() == 0 {
                continue;
            }

            // dispatch the command
            match pieces[0] {
                "get" => self.get(pieces[1..].to_vec()),
                unknown => println!("Unknown command: {}", unknown)
            }
        }

    }

    fn get(&mut self, args: Vec<&str>) {
        if args.len() != 1 {
            return;
        }

        // are we connected?
        let zk = match self.zk {
            Some(ref __zk) => __zk,
            _ => {
                println!("Not connected.");
                return;
            }
        };

        let data = zk.get_data(args[0], false);
        if data.is_ok() {
            let (bytes, _) = data.unwrap();
            println!("{}", str::from_utf8(&bytes[..]).unwrap().to_string());
        }
    }
}
