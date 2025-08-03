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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let begin_key = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlapping_ssts = Vec::new();
        for sst_id in &_snapshot.levels[_in_level - 1].1 {
            let sst = &_snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if first_key <= &end_key && last_key >= &begin_key {
                overlapping_ssts.push(*sst_id);
            }
        }

        overlapping_ssts
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // Task1.1: Compute Target Sizes
        let mut target_level_size = (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>();
        let mut real_level_size = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        for i in 0..self.options.max_levels {
            real_level_size.push(
                _snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| _snapshot.sstables[x].table_size())
                    .sum::<u64>() as usize,
            );
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        target_level_size[self.options.max_levels - 1] = std::cmp::max(
            real_level_size[self.options.max_levels - 1],
            base_level_size_bytes,
        );
        for i in (0..self.options.max_levels - 1).rev() {
            let next_level_size = target_level_size[i + 1];
            let current_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                target_level_size[i] = current_level_size;
            }
            if target_level_size[i] > 0 {
                base_level = i + 1;
            }
        }

        // flush l0
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &_snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut priorities = Vec::with_capacity(self.options.max_levels);
        let mut select_level = 0;
        let mut max_priority = 0.0;
        for level in 0..self.options.max_levels {
            let priority = real_level_size[level] as f64 / target_level_size[level] as f64;
            if priority > 1.0 {
                priorities.push((priority, level + 1));
            }
            if priority > 1.0 && priority > max_priority {
                max_priority = priority;
                select_level = level + 1;
            }
        }

        if select_level > 0 {
            let selected_sst = _snapshot.levels[select_level - 1]
                .1
                .iter()
                .min()
                .copied()
                .unwrap();
            return Some(LeveledCompactionTask {
                upper_level: Some(select_level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level: select_level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &[selected_sst],
                    select_level + 1,
                ),
                is_lower_level_bottom_level: select_level + 1 == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut upper_level_sst_set = _task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_set = _task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        if let Some(upper_level) = _task.upper_level {
            let new_upper_level_sst = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|id| {
                    if upper_level_sst_set.remove(id) {
                        return None;
                    }
                    Some(*id)
                })
                .collect::<Vec<_>>();
            snapshot.levels[upper_level - 1].1 = new_upper_level_sst;
        } else {
            let new_l0_sst = snapshot
                .l0_sstables
                .iter()
                .filter_map(|id| {
                    if upper_level_sst_set.remove(id) {
                        return None;
                    }
                    Some(*id)
                })
                .collect::<Vec<_>>();
            snapshot.l0_sstables = new_l0_sst;
        }

        files_to_remove.extend(&_task.upper_level_sst_ids);
        files_to_remove.extend(&_task.lower_level_sst_ids);

        let mut new_lower_level_sst = snapshot.levels[_task.lower_level - 1]
            .1
            .iter()
            .filter_map(|id| {
                if lower_level_sst_set.remove(id) {
                    return None;
                }
                Some(*id)
            })
            .collect::<Vec<_>>();
        new_lower_level_sst.extend(_output);
        if !_in_recovery {
            new_lower_level_sst.sort_by(|x, y| {
                snapshot.sstables[x]
                    .first_key()
                    .cmp(snapshot.sstables[y].first_key())
            });
        }
        snapshot.levels[_task.lower_level - 1].1 = new_lower_level_sst;

        (snapshot, files_to_remove)
    }
}
