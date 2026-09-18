use anyhow::Result;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::fs;
use std::path::Path;

use crate::config;

// http_router
const ALIASES_TABLE: TableDefinition<(u32, &str), &str> = TableDefinition::new("aliases");
const KEYS_TABLE: TableDefinition<&str, bool> = TableDefinition::new("keys");
const PRESENT: bool = true;

pub(crate) struct Storage {
    database: Database,
}

impl Storage {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let database = Database::create(path)?;
        let write_txn = database.begin_write()?;
        {
            let _aliases = write_txn.open_table(ALIASES_TABLE)?;
            let _keys = write_txn.open_table(KEYS_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { database })
    }

    pub(crate) fn open_default() -> Result<Self> {
        Self::open(config::database_filename())
    }

    pub(crate) fn set_alias(&self, id: &u32, alias: &str, socket: &str) -> Result<()> {
        let write_txn = self.database.begin_write()?;
        {
            let mut aliases = write_txn.open_table(ALIASES_TABLE)?;
            aliases.insert((id.to_owned(), alias), socket)?;
        }
        write_txn.commit()?;

        Ok(())
    }

    pub(crate) fn get_alias(&self, id: &u32, alias: &str) -> Result<Option<String>> {
        let read_txn = self.database.begin_read()?;
        let aliases = read_txn.open_table(ALIASES_TABLE)?;

        Ok(aliases.get((id.to_owned(), alias))?.map(|socket| socket.value().to_owned()))
    }

    pub(crate) fn aliases(&self, id: &u32) -> Result<Vec<(String, String)>> {
        let read_txn = self.database.begin_read()?;
        let aliases = read_txn.open_table(ALIASES_TABLE)?;
        let start = (id.to_owned(), "");
        let end = (id.to_owned(), "\u{10FFFF}");
        let mut result = Vec::new();

        for entry in aliases.range(start..=end)? {
            let (key, socket) = entry?;
            let (id, alias) = key.value();
            result.push((alias.to_owned(), socket.value().to_owned()));
        }

        Ok(result)
    }

    pub(crate) fn remove_alias(&self, id: &u32, alias: &str) -> Result<bool> {
        let write_txn = self.database.begin_write()?;
        let removed = {
            let mut aliases = write_txn.open_table(ALIASES_TABLE)?;
            aliases.remove((id.to_owned(), alias))?.is_some()
        };
        write_txn.commit()?;

        Ok(removed)
    }

    pub(crate) fn add_key(&self, key: &str) -> Result<()> {
        let write_txn = self.database.begin_write()?;
        {
            let mut keys = write_txn.open_table(KEYS_TABLE)?;
            keys.insert(key, PRESENT)?;
        }
        write_txn.commit()?;

        Ok(())
    }

    pub(crate) fn has_key(&self, key: &str) -> Result<bool> {
        let read_txn = self.database.begin_read()?;
        let keys = read_txn.open_table(KEYS_TABLE)?;
        Ok(keys.get(key)?.is_some())
    }

    pub(crate) fn remove_key(&self, key: &str) -> Result<bool> {
        let write_txn = self.database.begin_write()?;
        let removed = {
            let mut keys = write_txn.open_table(KEYS_TABLE)?;
            keys.remove(key)?.is_some()
        };
        write_txn.commit()?;

        Ok(removed)
    }
}
