// REMOVE THIS LINE after fully implementing this functionality
// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                .read(true)
                .create_new(true)
                .write(true)
                .open(_path)
                .context("failed to open log file for wal")?)))
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = _path.as_ref();
        let mut file = OpenOptions::new().read(true).append(true).open(path).context("failed to open log file for wal")?;
        let mut buf = Vec::new();

        file.read_to_end(&mut buf);
        let mut rbuf = buf.as_slice();
        while rbuf.has_remaining() {
            let key_len = rbuf.get_u16() as usize;
            let key = Bytes::copy_from_slice(&rbuf[..key_len]);
            rbuf.advance(key_len);
            let value_len = rbuf.get_u16() as usize;
            let value = Bytes::copy_from_slice(&rbuf[..value_len]);
            rbuf.advance(value_len);
            _skiplist.insert(key, value);
        }
        
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file)))
        })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> = Vec::new();
        let key_len = _key.len();
        let value_len = _value.len();
        buf.put_u16(key_len as u16);
        buf.put_slice(_key);
        buf.put_u16(value_len as u16);
        buf.put_slice(_value);
        file.write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
