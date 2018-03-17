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
use maidsafe_utilities::serialisation;

use std::collections::BTreeSet;
use std::fmt::{self, Debug, Formatter};
use tiny_keccak::sha3_256;

/// A unit in the DAG.
#[derive(Clone, Serialize, Deserialize)]
pub struct Unit {
    /// Identifier of this unit in the DAG.
    pub identifier: Vec<u8>,
    /// The identifier of the parent unit that pointing to.
    pub parent: Vec<u8>,
    /// The event observed or notified.
    pub payload: Vec<u8>,
    /// The peers witnessed the same unit.
    pub observers: BTreeSet<Id>,
    /// The clidren field is only for the quick check of childless state.
    pub children: BTreeSet<Vec<u8>>,
}

impl Unit {
    /// Generate a genesis unit. The parent and payload is hard coded.
    pub fn new_genesis(observers: BTreeSet<Id>) -> Self {
        if let Ok(serialised) = serialisation::serialise(&(vec![0, 0, 0], vec![0, 0, 0])) {
            Unit {
                identifier: sha3_256(&serialised).to_vec(),
                parent: vec![0, 0, 0],
                payload: vec![0, 0, 0],
                observers,
                children: BTreeSet::new(),
            }
        } else {
            panic!("cannot generate genesis identifier");
        }
    }

    /// Create a new unit based on the input infos.
    pub fn new(parent: Self, payload: Vec<u8>, observers: BTreeSet<Id>) -> Self {
        let identifier =
            if let Ok(serialised) = serialisation::serialise(&(parent.payload, payload.clone())) {
                sha3_256(&serialised)
            } else {
                panic!("cannot generate identifier for a unit");
            };
        Unit {
            identifier: identifier.to_vec(),
            parent: parent.identifier.clone(),
            payload,
            observers,
            children: BTreeSet::new(),
        }
    }

    /// Union with the other unit.
    pub fn union(&mut self, other: &Unit) {
        self.observers = self.observers.union(&other.observers).cloned().collect();
        self.children = self.children.union(&other.children).cloned().collect();
    }

    /// Add a new child.
    pub fn add_child(&mut self, child: Vec<u8>) {
        let _ = self.children.insert(child);
    }

    /// Add a new observer.
    pub fn add_observer(&mut self, id: &Id) {
        let _ = self.observers.insert(*id);
    }
}

impl Debug for Unit {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(
            formatter,
            "Unit identifier: {:02x}{:02x}{:02x}.. , parent: {:02x}{:02x}{:02x}.. , \
             payload: {:?} , observers: {:?}",
            self.identifier[0],
            self.identifier[1],
            self.identifier[2],
            self.parent[0],
            self.parent[1],
            self.parent[2],
            self.payload,
            self.observers
        )
    }
}
