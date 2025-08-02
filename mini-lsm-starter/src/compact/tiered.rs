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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Task1.1: Triggered by Space Amplification Ratio
        let level_num = _snapshot.levels.len();
        let last_level_size = _snapshot.levels[level_num - 1].1.len();
        let mut engine_size = 0;
        for level_id in 0..level_num - 1 {
            engine_size += _snapshot.levels[level_id].1.len();
        }
        let space_amplification_ratio = engine_size as f64 / last_level_size as f64;
        if space_amplification_ratio >= self.options.max_size_amplification_percent as f64 / 100.0 {
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Task1.2: Triggered by Size Ratio
        let size_ratio = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut previous_tier_size = 0;
        for level_id in 0..level_num - 1 {
            previous_tier_size += _snapshot.levels[level_id].1.len();
            let next_level_size = _snapshot.levels[level_id + 1].1.len();
            let current_size_ratio = next_level_size as f64 / previous_tier_size as f64;
            if current_size_ratio > size_ratio && level_id + 1 >= self.options.min_merge_width {
                return Some(TieredCompactionTask {
                    tiers: _snapshot
                        .levels
                        .iter()
                        .take(level_id + 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: false,
                });
            }
        }

        // Task1.3: Reduce Sorted Runs
        let num_tiers_to_take = std::cmp::min(
            level_num,
            self.options.max_merge_width.unwrap_or(usize::MAX),
        );

        Some(TieredCompactionTask {
            tiers: _snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: level_num >= num_tiers_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut tiers_to_remove = _task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut new_tier_added = false;

        for (tier_id, files) in &snapshot.levels {
            if let Some(ffiles) = tiers_to_remove.remove(tier_id) {
                // remove the tier
                assert_eq!(ffiles, files, "file changed after issuing compaction task");
                files_to_remove.extend(ffiles.iter().copied());
            } else {
                // retain the tier
                levels.push((*tier_id, files.clone()));
            }
            if tiers_to_remove.is_empty() && !new_tier_added {
                new_tier_added = true;
                // levels.insert(0, (*_output.first().unwrap(), _output.to_vec()));
                levels.push((_output[0], _output.to_vec()));
            }
        }

        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
