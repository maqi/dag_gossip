// Copyright 2018 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under (1) the MaidSafe.net Commercial License,
// version 1.0 or later, or (2) The General Public License (GPL), version 3, depending on which
// licence you accepted on initial access to the Software (the "Licences").
//
// By contributing code to the SAFE Network Software, or to this project generally, you agree to be
// bound by the terms of the MaidSafe Contributor Agreement.  This, along with the Licenses can be
// found in the root directory of this project at LICENSE, COPYING and CONTRIBUTOR.
//
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
//
// Please review the Licences for the specific language governing permissions and limitations
// relating to use of the SAFE Network Software.

use id::Id;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Formatter};
use unit::Unit;

/// DAG handler.
#[derive(Clone, Serialize, Deserialize)]
pub struct Dag {
    units: BTreeMap<Vec<u8>, Unit>,
    genesis: Unit,
    majority: u8,
}

/// The graph is composed by: a list of units, each holds the parent it points to.
/// The graph starts with a genesis unit, which is a hard-coded unit.

impl Dag {
    /// Creating a new DAG, with the gensis block inserted.
    pub fn new(id: Id) -> Self {
        let mut observers = BTreeSet::new();
        let _ = observers.insert(id);
        let gensis_unit = Unit::new_genesis(observers);
        let mut units = BTreeMap::new();
        let _ = units.insert(gensis_unit.identifier.clone(), gensis_unit.clone());
        Dag {
            units,
            genesis: gensis_unit,
            majority: 0,
        }
    }

    /// Update the majority counter.
    pub fn set_majority(&mut self, majority: u8) {
        self.majority = majority;
    }

    /// Union with the other DAG.
    ///     * If don't know a unit from other, insert it into graph.
    ///     * If already know a unit, union the units.
    pub fn union(&mut self, other: &Dag) {
        for (identifier, other_unit) in &other.units {
            if let Some(unit) = self.units.get_mut(identifier) {
                // If already see the unit, union these two.
                unit.union(other_unit);
                continue;
            }
            // If already observed the payload along the path the other uint sits, only union
            // the observers.
            if let Some(identifier) = self.has_observed_in(
                other_unit.parent.clone(),
                &other_unit.payload,
            )
            {
                if let Some(unit) = self.units.get_mut(&identifier) {
                    unit.observers = unit.observers
                        .union(&other_unit.observers)
                        .cloned()
                        .collect();
                }
            } else {
                let _ = self.units.insert(
                    other_unit.identifier.clone(),
                    other_unit.clone(),
                );
            }
        }
        // Only merge a child in when it was not a child to us yet.
        let mut our_known_children = BTreeSet::new();
        for unit in self.units.values() {
            our_known_children = our_known_children.union(&unit.children).cloned().collect();
        }
        let mut children_to_union = BTreeMap::new();
        for (k, v) in &other.units {
            let non_exist_children = v.children
                .intersection(&our_known_children)
                .cloned()
                .collect();
            let _ = children_to_union.insert(k.clone(), non_exist_children);
        }
        for (identifier, children) in children_to_union {
            if let Some(unit) = self.units.get_mut(&identifier) {
                unit.children = unit.children.union(&children).cloned().collect();
            }
        }
    }

    /// A new event being observed.
    ///     * pickup a best parent
    ///     * if the best parent is alread the incoming event, i.e. others observed it and notified,
    ///       we shall only be inserted into that unit as an observer.
    ///     * otherwise, create a new unit and insert into graph.
    pub fn new_payload(&mut self, payload: Vec<u8>, own_id: &Id) {
        let mut observers = BTreeSet::new();
        let _ = observers.insert(*own_id);
        let parent = self.get_best_parent(own_id);

        // In case the parent is regarding the same event but be seen by others first
        // we shall only add us as an observer to it
        if let Some(observed) = self.has_observed_in(parent.identifier.clone(), &payload) {
            if let Some(unit) = self.units.get_mut(&observed) {
                unit.add_observer(own_id);
                return;
            } else {
                panic!("just find a best parent but cann't fetch it from graph");
            }
        }

        let unit = Unit::new(parent.clone(), payload, observers);
        if let Some(parent) = self.units.get_mut(&parent.identifier) {
            parent.add_child(unit.identifier.clone());
        } else {
            panic!("just find a best parent but cann't fetch it from graph");
        }

        self.units
            .entry(unit.identifier.clone())
            .or_insert_with(|| unit.clone())
            .union(&unit);
    }

    // Travel along the path started from the input tip, to find out whether the payload has been
    // observed before. If so, return the identifier of the unit holds such payload.
    fn has_observed_in(&self, tip: Vec<u8>, payload: &[u8]) -> Option<Vec<u8>> {
        let mut iterator = tip;
        let mut steps = 0;
        while let Some(unit) = self.units.get(&iterator) {
            if unit.payload == payload.to_vec() {
                return Some(unit.identifier.clone());
            }
            if unit.identifier == self.genesis.identifier {
                break;
            }
            steps += 1;
            if steps > self.units.len() {
                for unit in self.units.values() {
                    println!("{:?}", unit);
                }
                panic!("breaking as experiencing a cycle in {:?}", self);
            }
            iterator = unit.parent.clone();
        }
        None
    }

    // The parent shall be a clildless unit, and:
    //  * any if a stable unit, otherwise:
    //  * having the longest length of stable units along the path back to the GENESIS
    //  * if still multiple, choose the ones self observed
    //  * if still multiple, choose the ones having the majority votes
    //  * if still multiple, choose the one by its name order.
    fn get_best_parent(&self, own_id: &Id) -> Unit {
        // Pick up childless units
        let childless: Vec<Unit> = self.units
            .values()
            .filter_map(|unit| if unit.children.is_empty() {
                Some(unit.clone())
            } else {
                None
            })
            .collect();
        if childless.len() == 1 {
            return childless[0].clone();
        }

        // Find the childless stable unit
        let stable_childless: Vec<Unit> = childless
            .iter()
            .filter_map(|unit| if unit.observers.len() as u8 >= self.majority {
                Some(unit.clone())
            } else {
                None
            })
            .collect();
        if stable_childless.len() == 1 {
            return stable_childless[0].clone();
        }

        // Travel along the path from the childless unit to the gensis to collect the scores
        // The score is so far defined as :
        //      (the length of the path, number of stable unit alongs the path)
        let mut path_counters = BTreeMap::new();
        for child in &childless {
            let mut stats = (0, 0);
            let mut iterator = &child.parent;
            while let Some(parent) = self.units.get(iterator) {
                stats.0 += 1;
                // The unit is stable
                if parent.observers.len() as u8 >= self.majority {
                    stats.1 += 1;
                }
                // Reached the genesis.
                if parent.identifier == self.genesis.identifier {
                    break;
                }
                iterator = &parent.parent;
            }
            let _ = path_counters.insert(child.identifier.clone(), stats);
        }

        // Pick the childless units who have the most stable units along it.
        let mut max = 0;
        let mut max_childless: Vec<(u8, Unit)> = Vec::new();
        for child in &childless {
            if let Some(counters) = path_counters.get(&child.identifier) {
                if counters.1 == max {
                    max_childless.push((counters.0, child.clone()));
                } else if counters.1 > max {
                    max = counters.1;
                    max_childless.clear();
                    max_childless.push((counters.0, child.clone()));
                }
            }
        }
        if max_childless.len() == 1 {
            return max_childless[0].1.clone();
        }

        // Pick the candidates which self observed.
        let self_observed_childless: Vec<Unit> = max_childless
            .iter()
            .filter_map(|entry| if entry.1.observers.contains(own_id) {
                Some(entry.1.clone())
            } else {
                None
            })
            .collect();
        if self_observed_childless.len() == 1 {
            return self_observed_childless[0].clone();
        }

        // Pick the candidates which has the most observers.
        let mut max_votes = 0;
        let mut max_votes_childless = Vec::new();
        for entry in &max_childless {
            if entry.1.observers.len() == max_votes {
                max_votes_childless.push(entry.1.clone());
            } else if entry.1.observers.len() > max_votes {
                max_votes = entry.1.observers.len();
                max_votes_childless.clear();
                max_votes_childless.push(entry.1.clone());
            }
        }

        if let Some(best_parent) = max_votes_childless.pop() {
            best_parent.clone()
        } else {
            panic!("cannot find a best parent in the DAG of {:?} ", self);
        }
    }
}

impl Debug for Dag {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        writeln!(
            formatter,
            "DAG has majority of {} and contains graph of :",
            self.majority
        )?;
        writeln!(formatter, "```graphviz")?;
        writeln!(formatter, "digraph hierarchy {{")?;
        writeln!(formatter, "    nodesep=1.0")?;
        let mut units_state = BTreeMap::new();
        for unit in self.units.values() {
            let graph_node_name = unit.graphviz();
            if unit.observers.len() as u8 >= self.majority {
                writeln!(formatter, "    node [color=Black,fontname=Courier]")?;
                let _ = units_state.insert(unit.identifier.clone(), (
                    "stable",
                    graph_node_name.clone(),
                    unit.parent.clone(),
                ));
            } else {
                writeln!(formatter, "    node [color=Red,fontname=Courier]")?;
                let _ = units_state.insert(unit.identifier.clone(), (
                    "unstable",
                    graph_node_name.clone(),
                    unit.parent.clone(),
                ));
            }
            writeln!(formatter, "    {}", graph_node_name)?;
        }
        for entry in units_state.values() {
            if let Some(parent) = units_state.get(&entry.2) {
                if entry.0 == "stable" {
                    writeln!(formatter, "    edge [color=black, style=line]")?;
                } else {
                    writeln!(formatter, "    edge [color=Red, style=dashed]")?;
                }
                writeln!(formatter, "    {} -> {}", entry.1, parent.1)?;
            }
        }
        writeln!(formatter, "}}")?;
        writeln!(formatter, "```")
    }
}
