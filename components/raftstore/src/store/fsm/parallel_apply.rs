// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ops::Index, thread, thread::JoinHandle};

use batch_system::Priority;
use collections::HashMap;
use crossbeam::channel::{unbounded, Receiver as CBReceiver, Sender as CBSender, TryRecvError};
use engine_traits::{KvEngine, RaftEngine};
use tikv_util::{error, thd_name};

use crate::{
    store::{
        fsm::{
            apply::{ApplyContext, ApplyFsm, Msg, Notifier},
            ApplyRouter, ApplyTask, RaftPollerBuilder,
        },
        Peer,
    },
    Result,
};

/// Senders for parallel apply workers.
#[derive(Clone)]
pub struct ParallelApplySenders<EK: KvEngine> {
    senders: Vec<CBSender<(u64, ApplyTask<EK>)>>,
    apply_round_robin: usize,
}

impl<EK: KvEngine> ParallelApplySenders<EK> {
    pub fn new() -> Self {
        ParallelApplySenders {
            senders: vec![],
            apply_round_robin: rand::random(),
        }
    }

    pub fn senders(&self) -> &Vec<CBSender<(u64, Msg<EK>)>> {
        &self.senders
    }

    pub fn set_senders(&mut self, senders: Vec<CBSender<(u64, Msg<EK>)>>) {
        self.senders = senders
    }

    pub fn schedule_task(&mut self, task: (u64, ApplyTask<EK>)) {
        self.apply_round_robin = (self.apply_round_robin + 1) % self.senders.len();
        match self.senders[self.apply_round_robin].send(task) {
            Ok(_) => {}
            Err(err) => {
                println!("{:?}", err)
            }
        }
    }
}

impl<EK: KvEngine> Index<usize> for ParallelApplySenders<EK> {
    type Output = CBSender<(u64, ApplyTask<EK>)>;

    #[inline]
    fn index(&self, index: usize) -> &CBSender<(u64, ApplyTask<EK>)> {
        &self.senders[index]
    }
}

struct ParallelApplyWorker<EK>
where
    EK: KvEngine,
{
    receiver: CBReceiver<(u64, Msg<EK>)>,
    apply_ctx: ApplyContext<EK>,
    peers: HashMap<u64, Box<ApplyFsm<EK>>>,
}

impl<EK> ParallelApplyWorker<EK>
where
    EK: KvEngine,
{
    fn fetch_msg(&mut self) -> Option<(u64, Msg<EK>)> {
        match self.receiver.try_recv() {
            Ok(m) => Some(m),
            Err(TryRecvError::Empty) => {
                self.apply_ctx.flush();
                match self.receiver.recv() {
                    Ok(m) => Some(m),
                    Err(_) => None,
                }
            }
            Err(TryRecvError::Disconnected) => None,
        }
    }

    fn run(&mut self) {
        while let Some(m) = self.fetch_msg() {
            if m.0 == 0 {
                break;
            }
            let apply_fsm = self.peers.get_mut(&m.0);
            if let Some(p) = apply_fsm {
                // TODO: we might be able to do some batching for normal logs like the
                // handle_tasks, but it's unclear what the benefits would be
                p.handle_task(&mut self.apply_ctx, m.1);
            } else {
                if let Msg::Registration(r) = m.1 {
                    let pending_parallel_task_num = r.pending_parallel_task_num.clone();
                    self.apply_ctx
                        .parallel_apply_tasks
                        .insert(m.0, pending_parallel_task_num);
                    let (_, p) = ApplyFsm::from_registration(r);
                    self.peers.insert(m.0, p);
                } else {
                    error!("no peer for msg";
                        "region_id" => m.0,
                        "msg" => ?m.1,
                    );
                }
            }
        }
    }
}

pub struct ParallelApplySystem<EK: KvEngine> {
    senders: Vec<CBSender<(u64, Msg<EK>)>>,
    handlers: Vec<JoinHandle<()>>,
}

impl<EK> ParallelApplySystem<EK>
where
    EK: KvEngine,
{
    pub fn new() -> Self {
        Self {
            senders: vec![],
            handlers: vec![],
        }
    }

    pub fn senders(&self) -> &Vec<CBSender<(u64, Msg<EK>)>> {
        &self.senders
    }

    pub fn spawn<T, ER: RaftEngine>(
        &mut self,
        builder: &RaftPollerBuilder<EK, ER, T>,
        sender: Box<dyn Notifier<EK>>,
        router: &ApplyRouter<EK>,
    ) -> Result<()> {
        let tag = format!("[store {}]", builder.store.get_id());
        for i in 0..builder.cfg.value().apply_batch_system.pool_size {
            let (tx, rx) = unbounded();
            let mut worker = ParallelApplyWorker {
                receiver: rx,
                apply_ctx: ApplyContext::<EK>::new(
                    tag.clone(),
                    builder.coprocessor_host.clone(),
                    builder.importer.clone(),
                    builder.region_scheduler.clone(),
                    builder.engines.kv.clone(),
                    router.clone(),
                    sender.clone_box(),
                    &builder.cfg.value(),
                    builder.store.get_id(),
                    true,
                    builder.pending_create_peers.clone(),
                    Priority::Normal,
                ),
                peers: HashMap::default(),
            };
            let props = tikv_util::thread_group::current_properties();
            let t = thread::Builder::new()
                .name(thd_name!(format!("Parallel-Apply-{}", i)))
                .spawn(move || {
                    tikv_util::thread_group::set_properties(props);
                    worker.run();
                })?;
            self.senders.push(tx);
            self.handlers.push(t);
        }
        Ok(())
    }

    pub fn schedule_all<'a, ER: RaftEngine>(
        &mut self,
        peers: impl Iterator<Item = &'a Peer<EK, ER>>,
    ) {
        for peer in peers {
            for sender in &mut self.senders {
                sender
                    .send((peer.region().get_id(), Msg::register(peer)))
                    .unwrap();
            }
        }
    }

    pub fn shutdown(&mut self) {
        assert_eq!(self.senders.len(), self.handlers.len());
        for (i, handler) in self.handlers.drain(..).enumerate() {
            self.senders[i].send((0, Msg::Noop)).unwrap();
            handler.join().unwrap();
        }
    }
}
