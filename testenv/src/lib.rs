use std::fs;

use electrsd::corepc_node::{self, Conf as CoreConfig, Node, P2P, tempfile};
use electrsd::{Conf as ElectrsConfig, ElectrsD};

pub struct TestEnv {
    _tmp_root: tempfile::TempDir,
    pub bitcoind: Node,
    pub electrsd: ElectrsD,
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl TestEnv {
    pub fn new() -> Self {
        let tmp_root = tempfile::TempDir::new().expect("failed to create temp root");

        let bitcoind_exe = corepc_node::downloaded_exe_path()
            .unwrap_or_else(|_| corepc_node::exe_path().expect("bitcoind executable not found"));
        let electrs_exe = electrsd::downloaded_exe_path()
            .unwrap_or_else(|| electrsd::exe_path().expect("electrs executable not found"));

        let bitcoind_tmp = tmp_root.path().join("bitcoind");
        let electrs_tmp = tmp_root.path().join("electrs");

        fs::create_dir_all(&bitcoind_tmp).expect("failed to create bitcoind temp dir");
        fs::create_dir_all(&electrs_tmp).expect("failed to create electrs temp dir");

        let mut bitcoind_conf = CoreConfig::default();
        bitcoind_conf.p2p = P2P::Yes;
        bitcoind_conf.tmpdir = Some(bitcoind_tmp);
        let bitcoind =
            Node::with_conf(&bitcoind_exe, &bitcoind_conf).expect("failed to start bitcoind");

        let mut electrs_conf = ElectrsConfig::default();
        electrs_conf.args.push("--skip-default-conf-files");
        electrs_conf.tmpdir = Some(electrs_tmp);
        let electrsd = ElectrsD::with_conf(&electrs_exe, &bitcoind, &electrs_conf)
            .expect("failed to start electrs");

        Self {
            _tmp_root: tmp_root,
            bitcoind,
            electrsd,
        }
    }
}
