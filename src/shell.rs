use std::io::stdin;
use std::io::stdout;
use std::io::Write;
use std::str;
use std::time::Duration;

use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZkError, ZooKeeper};
use zookeeper::acls;


struct MyWatcher;

impl Watcher for MyWatcher {
    fn handle(&self, e: &WatchedEvent) {
        println!("{:?}", e)
    }
}

pub struct Shell {
    hosts: String,
    zk: Option<ZooKeeper>,
    session_timeout: u64,
    default_acl: Vec<Acl>,
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
    ($args:ident, $min:expr, $max:expr, $params:expr) => (
        if $args.len() < $min || $args.len() > $max {
            println!("Wrong number of arguments, expected parameters: {}", $params);
            return;
        } else {
            $args.len()
        })
}

fn report_error(error: ZkError, path: &str) {
    match error {
        ZkError::NoNode => println!("Path {} does not exist.", path),
        ZkError::NotEmpty => println!("Path {} is not empty.", path),
        unknown => println!("Unknown error: {:?}", unknown),
    }
}

impl Shell {
    pub fn new(hosts: &str) -> Shell {
        Shell {
            hosts: hosts.to_string(),
            zk: None,
            session_timeout: 5,
            default_acl: acls::OPEN_ACL_UNSAFE.clone(),
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
                "create" => self.create(args),
                "rm" => self.rm(args),
                "exists" => self.exists(args),
                unknown => println!("Unknown command: {}", unknown)
            }
        }

    }

    fn get(&mut self, args: Vec<&str>) {
        let argc = check_args!(args, 1, 2, "<path> [watch]");
        let watch = match argc {
            1 => false,
            _ => args[1].to_lowercase() == "true"
        };

        let zk = fetch_zk!(self.zk);
        let path = args[0];
        let ret = zk.get_data(path, watch);

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
        let argc = check_args!(args, 2, 3, "<path> <data> [version]");
        let version = match argc {
            3 => match args[2].parse::<i32>() {
                Ok(version) => version,
                Err(_) => -1
            },
            _ => -1
        };

        let zk = fetch_zk!(self.zk);
        let path = args[0];
        let data = args[1].as_bytes().to_vec();
        let ret = zk.set_data(path, data, version);

        match ret {
            Ok(_) => (),
            Err(err) => report_error(err, path),
        }
    }

    fn ls(&mut self, args: Vec<&str>) {
        let argc = check_args!(args, 1, 2, "<path> [watch]");
        let watch = match argc {
            1 => false,
            _ => args[1].to_lowercase() == "true"
        };

        let zk = fetch_zk!(self.zk);
        let path = args[0];
        let ret = zk.get_children(path, watch);

        match ret {
            Ok(children) => println!("{}", children.join(" ")),
            Err(err) => report_error(err, path),
        }
    }

    fn create(&mut self, args: Vec<&str>) {
        let mut mode: CreateMode = CreateMode::Persistent;

        let argc = check_args!(args, 2, 4, "<path> <data> [ephemeral] [sequential]");
        if argc >= 3 {
            if args[2].to_lowercase() == "true" {
                mode = CreateMode::Ephemeral;
            }
        }

        if argc == 4 {
            if args[3].to_lowercase() == "true" {
                mode = match mode {
                    CreateMode::Ephemeral => CreateMode::EphemeralSequential,
                    _ => CreateMode::PersistentSequential
                }
            }
        }

        let zk = fetch_zk!(self.zk);
        let path = args[0];
        let data = args[1].as_bytes().to_vec();

        let ret = zk.create(
            path, data, self.default_acl.clone(), mode);

        match ret {
            Ok(_) => (),
            Err(err) => report_error(err, path),
        }
    }

    fn rm(&mut self, args: Vec<&str>) {
        let argc = check_args!(args, 1, 2, "<path> [version]");
        let version = match argc {
            2 => match args[1].parse::<i32>() {
                Ok(version) => version,
                Err(_) => -1
            },
            _ => -1
        };

        let zk = fetch_zk!(self.zk);
        let path = args[0];
        let ret = zk.delete(path, version);

        match ret {
            Ok(()) =>  (),
            Err(err) => report_error(err, path),
        }
    }

    fn exists(&mut self, args: Vec<&str>) {
        let argc = check_args!(args, 1, 2, "<path> [watch]");
        let watch = match argc {
            1 => false,
            _ => args[1].to_lowercase() == "true"
        };

        let zk = fetch_zk!(self.zk);
        let path = args[0];
        let ret = zk.exists(path, watch);

        match ret {
            Ok(stat) => println!("{:?}", stat),
            Err(err) => report_error(err, path),
        }
    }
}
