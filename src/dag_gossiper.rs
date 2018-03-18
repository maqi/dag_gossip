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

#![allow(dead_code)]

use dag::Dag;
use ed25519_dalek::Keypair;
use error::Error;
use id::Id;
#[cfg(test)]
use maidsafe_utilities::SeededRng as rand;
use maidsafe_utilities::serialisation;
#[cfg(not(test))]
use rand;
use rand::Rng;

use serde::ser::Serialize;
use sha3::Sha3_512;
use std::fmt::{self, Debug, Formatter};

/// An entity on the network which will gossip messages.
pub struct Gossiper {
    keys: Keypair,
    peers: Vec<Id>,
    dag: Dag,
}

impl Gossiper {
    /// The ID of this `Gossiper`, i.e. its public key.
    pub fn id(&self) -> Id {
        self.keys.public.into()
    }

    /// Add the ID of another node on the network.
    pub fn add_peer(&mut self, peer_id: Id) -> Result<(), Error> {
        self.peers.push(peer_id);
        self.dag.set_majority((self.peers.len() / 2 + 1) as u8);
        Ok(())
    }

    /// Send a new message starting at this `Gossiper`.
    /// This is interpreted as an new event observed by this node.
    pub fn send_new<T: Serialize>(&mut self, message: &T) -> Result<(), Error> {
        self.dag.new_payload(
            serialisation::serialise(message)?,
            &self.keys.public.into(),
        );
        Ok(())
    }

    /// Start a new round.
    pub fn next_round(&mut self) -> Result<(Id, Vec<u8>), Error> {
        let peer_id = match rand::thread_rng().choose(&self.peers) {
            Some(id) => *id,
            None => return Err(Error::NoPeers),
        };
        let message = self.prepare_to_send();
        debug!(
            "{:?} pushing to {:?} with DAG {:?}",
            self,
            peer_id,
            self.dag
        );
        Ok((peer_id, message))
    }

    /// Handles an incoming DAG from peer.
    pub fn handle_received_message(&mut self, peer_id: &Id, serialised_msg: &[u8]) {
        debug!("{:?} handling DAG from {:?}", self, peer_id);
        let dag: Dag = if let Ok(dag) = serialisation::deserialise(serialised_msg) {
            dag
        } else {
            error!("Failed to deserialise message");
            return;
        };
        self.dag.union(&dag);
    }

    fn prepare_to_send(&mut self) -> Vec<u8> {
        if let Ok(serialised) = serialisation::serialise(&self.dag) {
            serialised
        } else {
            panic!("cannot serialise own DAG");
        }
    }

    #[cfg(test)]
    pub fn print_dag(&self) {
        println!("{:?} has DAG : \n {:?}", self, self.dag);
    }
}

impl Debug for Gossiper {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{:?}", self.id())
    }
}

impl Default for Gossiper {
    fn default() -> Self {
        let mut rng = rand::thread_rng();
        let keys = Keypair::generate::<Sha3_512>(&mut rng);
        let id: Id = keys.public.into();
        let dag = Dag::new(id);
        Gossiper {
            keys,
            peers: Vec::new(),
            dag,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::{self, Itertools};
    use maidsafe_utilities::SeededRng;
    use rand::Rng;
    use std::collections::BTreeMap;

    fn create_network(node_count: u32) -> Vec<Gossiper> {
        let mut gossipers = itertools::repeat_call(Gossiper::default)
            .take(node_count as usize)
            .collect_vec();
        // Connect all the gossipers.
        for i in 0..(gossipers.len() - 1) {
            let lhs_id = gossipers[i].id();
            for j in (i + 1)..gossipers.len() {
                let rhs_id = gossipers[j].id();
                let _ = gossipers[j].add_peer(lhs_id);
                let _ = gossipers[i].add_peer(rhs_id);
            }
        }
        gossipers
    }

    fn send_messages(gossipers: &mut Vec<Gossiper>, num_of_msgs: u32, rng: &mut SeededRng) {
        let mut msg_pool: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut msgs = Vec::new();
        for j in 1..(num_of_msgs + 1) as u8 {
            msgs.push(vec![j, j, j]);
        }
        for _ in 0..gossipers.len() as u8 {
            msg_pool.push(msgs.clone())
        }

        // Polling
        // TODO: need to decide a termination condition. So far, it's using a fixed rounds.
        //       The number of rounds shall be adjusted in relation to the network size.
        //       The result from safe_gossip can be a reference.
        for _ in 0..30 {
            let mut messages = BTreeMap::new();
            // Call `next_round()` on each node to gather a list of serialised DAG.
            for (i, gossiper) in gossipers.iter_mut().enumerate() {
                if !msg_pool[i].is_empty() && rng.gen() {
                    let index = rng.gen_range(0, msg_pool[i].len() as u32) as usize;
                    let _ = gossiper.send_new(&msg_pool[i][index].clone());
                    let _ = msg_pool[i].remove(index);
                }
                let (dst_id, push_msg) = unwrap!(gossiper.next_round());
                let _ = messages.insert((gossiper.id(), dst_id), push_msg);
            }

            // Send all Push DAGs.
            for ((src_id, dst_id), push_msg) in messages {
                let mut dst = unwrap!(gossipers.iter_mut().find(|node| node.id() == dst_id));
                dst.handle_received_message(&src_id, &push_msg);
            }
        }

        for gossiper in gossipers {
            gossiper.print_dag();
        }
    }

    #[test]
    // Have a network of gossipers all known each other. The list of messages will be observed by
    // all of the gossipers, however each one with its own sequence.
    fn dag_gossip() {
        let mut rng = SeededRng::new();
        let num_of_nodes: Vec<u32> = vec![9];
        let num_of_msgs: Vec<u32> = vec![5];
        for nodes in &num_of_nodes {
            for msgs in &num_of_msgs {
                print!("Network of {} nodes, gossiping {} messages:\n", nodes, msgs);
                let mut gossipers = create_network(*nodes);
                send_messages(&mut gossipers, *msgs, &mut rng);
            }
        }
    }
}
