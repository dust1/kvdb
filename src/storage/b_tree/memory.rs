use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::common::range::Range;
use crate::common::range::Scan;
use crate::storage::Store;

/// an in-memory Store implementation using std's B+tree implementation
pub struct Memory {
    root: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            root: BTreeMap::new(),
        }
    }
}

impl Display for Memory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl Store for Memory {
    fn delete(&mut self, key: &[u8]) -> crate::error::Result<()> {
        self.root.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> crate::error::Result<()> {
        Ok(())
    }

    fn get(&self, key: &[u8]) -> crate::error::Result<Option<Vec<u8>>> {
        Ok(self.root.get(key).cloned())
    }

    fn scan(&self, range: Range) -> Scan {
        Box::new(
            self.root
                .range(range)
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> crate::error::Result<()> {
        self.root.insert(Vec::from(key), value);
        Ok(())
    }
}
