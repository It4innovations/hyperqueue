use libc::pid_t;
use std::time::Duration;
use tokio::process::Command;

#[tokio::main]
async fn main() {
    let sid: u32 = unsafe { libc::setsid() } as u32;
    unsafe {
        println!(
            "PID: {}, PGID: {}, SID: {}",
            std::process::id(),
            libc::getpgid(std::process::id() as pid_t),
            sid
        );
    }

    {
        let mut cmd = Command::new("python3");
        // cmd.arg("bash");
        //cmd.arg("-c").arg("sleep 3600 &\necho $!");
        cmd.arg("test.py");
        let mut child = cmd.spawn().unwrap();
        std::thread::sleep(Duration::from_secs(3600));
        //child.kill().unwrap();
        child.wait().await.unwrap();
    }
}
