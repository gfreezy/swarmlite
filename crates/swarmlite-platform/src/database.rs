use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use rusqlite::Connection;

pub const DATABASE_FILE: &str = "swarmlite.sqlite";

#[derive(Clone)]
pub struct Database {
    path: Arc<PathBuf>,
}

impl Database {
    pub fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("failed to create {}", data_dir.display()))?;
        let database = Self {
            path: Arc::new(data_dir.join(DATABASE_FILE)),
        };
        let connection = database.connect()?;
        connection.execute_batch("PRAGMA journal_mode = WAL;")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                database.path.as_ref(),
                std::fs::Permissions::from_mode(0o600),
            )
            .with_context(|| format!("failed to protect {}", database.path.display()))?;
        }
        Ok(database)
    }

    pub fn open_existing(data_dir: &Path) -> Result<Option<Self>> {
        data_dir
            .join(DATABASE_FILE)
            .exists()
            .then(|| Self::open(data_dir))
            .transpose()
    }

    pub fn connect(&self) -> Result<Connection> {
        let connection = Connection::open(self.path.as_ref())
            .with_context(|| format!("failed to open {}", self.path.display()))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA synchronous = FULL;")?;
        Ok(connection)
    }
}
