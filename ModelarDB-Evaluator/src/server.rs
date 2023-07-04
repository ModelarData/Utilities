/* Copyright 2023 The ModelarData Utilities Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Handle to a modelardbd process.

use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

/// Number of times to try to create the client and kill child processes.
const ATTEMPTS: u8 = 10;

/// Amount of time to sleep between each attempt to create the client and kill child processes.
const ATTEMPT_SLEEP_IN_SECONDS: Duration = Duration::from_secs(1);

/// Handle to a modelardbd process with [`Drop`] implemented so the process is killed automatically.
pub struct Server {
    modelardbd: Child,
}

impl Server {
    /// Compile modelardbd with the dev-release profile and execute it.
    pub fn new(local_data_folder: &Path) -> Self {
        // modelardbd's stdout and stderr is piped so the log messages (stdout) and expected errors
        // (stderr) are not printed when all of the tests are run using the "cargo test" command.
        let local_data_folder = local_data_folder.to_str().unwrap();
        let mut modelardbd = Command::new("cargo")
            .args([
                "run",
                "--profile",
                "dev-release",
                "--bin",
                "modelardbd",
                local_data_folder,
            ])
            .current_dir("ModelarDB-RS")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        // Ensure that modelardbd has started before executing the test. stdout will not include EOF
        // until the process ends, so bytes are read one at a time until the expected output is read.
        let stdout = modelardbd.stdout.as_mut().unwrap();
        let mut stdout_bytes = stdout.bytes().flatten();
        let mut stdout_output: Vec<u8> = Vec::with_capacity(512);
        let stdout_expected = "Starting Apache Arrow Flight on".as_bytes();
        while stdout_output.len() < stdout_expected.len()
            || &stdout_output[stdout_output.len() - stdout_expected.len()..] != stdout_expected
        {
            stdout_output.push(stdout_bytes.next().unwrap());
        }

        Self { modelardbd }
    }
}

impl Drop for Server {
    /// Kill modelardbd process when [`Server`] is dropped.
    fn drop(&mut self) {
        // Microsoft Windows often fails to kill the process and then succeed afterwards.
        let mut attempts = ATTEMPTS;
        while (self.modelardbd.kill().is_err() || self.modelardbd.wait().is_err()) && attempts > 0 {
            thread::sleep(ATTEMPT_SLEEP_IN_SECONDS);
            attempts -= 1;
        }
    }
}
