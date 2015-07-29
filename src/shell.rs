use std::io::stdin;
use std::io::stdout;
use std::io::Write;
use std::str;
use std::time::Duration;

use zookeeper::{Watcher, WatchedEvent, ZkError, ZooKeeper};


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

// are we connected?
macro_rules! fetch_zk {
    ($e:expr) => (
        match $e {
            Some(ref __zk) => __zk,
            _ => {
                println!("Not connected.");
                return;
            }
        })
}

macro_rules! check_args {
    ($args:ident, $count:expr, $params:expr) => (
        if $args.len() != $count {
            println!("Wrong number of arguments, expected parameters: {}", $params);
            return;
        })
}

fn report_error(error: ZkError, path: &str) {
    match error {
        ZkError::NoNode => println!("Path {} does not exist.", path),
        unknown => println!("Unknown error: {:?}", unknown),
    }
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
            let args = pieces[1..].to_vec();
            match pieces[0] {
                "get" => self.get(args),
                "set" => self.set(args),
                "ls" => self.ls(args),
                unknown => println!("Unknown command: {}", unknown)
            }
        }

    }

    fn get(&mut self, args: Vec<&str>) {
        check_args!(args, 1, "<path>");
        let zk = fetch_zk!(self.zk);
        let path = args[0];
        let ret = zk.get_data(path, false);

        match ret {
            Ok(data_stat) =>  {
                let (bytes, _) = data_stat;
                let datastr = str::from_utf8(&bytes[..]).unwrap().to_string();
                println!("{}", datastr);
            },
            Err(err) => report_error(err, path),
        }
    }

    fn set(&mut self, args: Vec<&str>) {
        check_args!(args, 2, "<path> <data>");
        let zk = fetch_zk!(self.zk);
        let path = args[0];
        let data = args[1].as_bytes().to_vec();
        let ret = zk.set_data(path, data, -1);

        match ret {
            Ok(_) => (),
            Err(err) => report_error(err, path),
        }
    }

    fn ls(&mut self, args: Vec<&str>) {
        check_args!(args, 1, "<path>");
        let zk = fetch_zk!(self.zk);
        let path = args[0];
        let ret = zk.get_children(path, false);

        match ret {
            Ok(children) => println!("{}", children.join(" ")),
            Err(err) => report_error(err, path),
        }
    }
}
