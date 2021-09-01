use cubestore::config::Config;
use cubestore::metastore::RocksMetaStore;
use cubestore::remotefs::LocalDirRemoteFs;
use cubestore::validation::validate_table;
use cubestore::CubeError;
use itertools::Itertools;

fn main() {
    simple_logger::init().unwrap();

    let c = Config::default().update_config(|mut c| {
        c.upload_to_remote = false;
        c
    });
    let fs = LocalDirRemoteFs::new(None, c.local_dir().clone());
    let m = RocksMetaStore::new(c.meta_store_path(), fs.clone(), c.config_obj());

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            match &std::env::args().collect_vec().as_slice() {
                &[_, schema, table] => show_err(
                    "validateion failed",
                    validate_table(&schema, &table, m.as_ref(), fs.as_ref()).await,
                ),
                _ => println!(
                "Usage: validate <schema> <table>\n\nUse CubeStore variables for configuration."
            ),
            }
        })
}

fn show_err(explain: &str, r: Result<(), CubeError>) {
    if let Err(e) = r {
        log::error!("{}: {}", explain, e.display_with_backtrace());
    }
}
