//! `wasi:cli/run` component used by runner-cli's tests. The first argument
//! picks a behavior, so one artifact covers every outcome the runner handles.

use std::{env, hint, process, thread, time::Duration};

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let arg = |i: usize| args.get(i).map(String::as_str).unwrap_or_default();
    let num = |i: usize| -> u64 { arg(i).parse().expect("numeric argument") };

    match arg(0) {
        // Print each remaining argument on its own stdout line.
        "echo" => args[1..].iter().for_each(|a| println!("{a}")),
        // Print the argument to stderr.
        "stderr" => eprintln!("{}", arg(1)),
        // Print the value of an env var.
        "env" => println!("{}", env::var(arg(1)).unwrap_or_default()),
        // Print without a trailing newline.
        "partial" => print!("{}", arg(1)),
        // Exit with the given code.
        "exit" => process::exit(num(1) as i32),
        // Panic (prints to stderr, then traps).
        "panic" => panic!("{}", arg(1)),
        // Busy-loop forever (only an epoch interrupt stops it).
        "spin" => loop {
            hint::spin_loop();
        },
        // Sleep in a host call (only the wall-clock backstop stops it).
        "sleep" => thread::sleep(Duration::from_millis(num(1))),
        // Print `tick <i>` <count> times, sleeping <ms> after each line.
        "tick" => {
            for i in 0..num(1) {
                println!("tick {i}");
                thread::sleep(Duration::from_millis(num(2)));
            }
        }
        // Print <bytes> bytes of output as 9-byte lines.
        "spam" => (0..num(1) / 9).for_each(|_| println!("xxxxxxxx")),
        // Allocate and touch <mib> MiB of memory.
        "alloc" => {
            let v = vec![1u8; (num(1) as usize) << 20];
            hint::black_box(&v);
        }
        other => {
            eprintln!("unknown command {other:?}");
            process::exit(2);
        }
    }
}
