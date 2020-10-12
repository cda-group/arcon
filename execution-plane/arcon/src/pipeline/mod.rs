// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "arcon_tui")]
use crate::tui::{component::TuiComponent, widgets::node::Node as TuiNode};
use crate::{
    allocator::ArconAllocator, buffer::event::PoolInfo, conf::ArconConf, manager::node::*,
    prelude::*, util::SafelySendableFn,
};
use fxhash::FxHashMap;
use kompact::{component::AbstractComponent, prelude::KompactSystem};
use std::sync::{Arc, Mutex};

/// A struct meant to simplify the creation of an Arcon Pipeline
#[derive(Clone)]
pub struct ArconPipeline {
    /// [kompact] system that drives the execution of components
    system: KompactSystem,
    /// Arcon configuration for this pipeline
    conf: ArconConf,
    /// Arcon allocator for this pipeline
    allocator: Arc<Mutex<ArconAllocator>>,
    /// NodeManagers launched on top of this ArconPipeline
    //node_managers: FxHashMap<String, ActorRefStrong<NodeEvent>>,
    #[cfg(feature = "arcon_tui")]
    tui_component: Arc<Component<TuiComponent>>,
    #[cfg(feature = "arcon_tui")]
    arcon_event_receiver: Arc<crossbeam_channel::Receiver<TuiNode>>,
}

/// A Node with operator type, state backend type, and timer type erased
pub type DynamicNode<IN> = Box<dyn CreateErased<ArconMessage<IN>>>;
/// Result of creating a [`DynamicNode`] in a [`KompactSystem`](kompact::KompactSystem)
pub type CreatedDynamicNode<IN> = Arc<dyn AbstractComponent<Message = ArconMessage<IN>>>;
/// A Source with operator type, state backend type, and timer type erased
pub type DynamicSource = Box<dyn CreateErased<()>>;

impl ArconPipeline {
    /// Creates a new ArconPipeline using the default ArconConf
    pub fn new() -> ArconPipeline {
        let conf = ArconConf::default();
        let allocator = Arc::new(Mutex::new(ArconAllocator::new(conf.allocator_capacity)));
        #[cfg(feature = "arcon_tui")]
        let (system, tui_component, arcon_receiver) = ArconPipeline::setup(&conf);
        #[cfg(not(feature = "arcon_tui"))]
        let system = ArconPipeline::setup(&conf);

        ArconPipeline {
            system,
            conf,
            allocator,
            //node_managers: FxHashMap::default(),
            #[cfg(feature = "arcon_tui")]
            tui_component,
            #[cfg(feature = "arcon_tui")]
            arcon_event_receiver: Arc::new(arcon_receiver),
        }
    }

    /// Creates a new ArconPipeline using the given ArconConf
    pub fn with_conf(conf: ArconConf) -> ArconPipeline {
        let allocator = Arc::new(Mutex::new(ArconAllocator::new(conf.allocator_capacity)));
        #[cfg(feature = "arcon_tui")]
        let (system, tui_component, arcon_receiver) = ArconPipeline::setup(&conf);
        #[cfg(not(feature = "arcon_tui"))]
        let system = ArconPipeline::setup(&conf);

        ArconPipeline {
            system,
            conf,
            allocator,
            //node_managers: FxHashMap::default(),
            #[cfg(feature = "arcon_tui")]
            tui_component,
            #[cfg(feature = "arcon_tui")]
            arcon_event_receiver: Arc::new(arcon_receiver),
        }
    }

    /// Creates a PoolInfo struct to be used by a ChannelStrategy
    pub fn get_pool_info(&self) -> PoolInfo {
        PoolInfo::new(
            self.conf.channel_batch_size,
            self.conf.buffer_pool_size,
            self.conf.buffer_pool_limit,
            self.allocator.clone(),
        )
    }

    /// Helper function to set up internals of the pipeline
    #[cfg(not(feature = "arcon_tui"))]
    fn setup(arcon_conf: &ArconConf) -> KompactSystem {
        let kompact_config = arcon_conf.kompact_conf();
        let system = kompact_config.build().expect("KompactSystem");
        system
    }

    /// Helper function to set up internals of the pipeline
    #[cfg(feature = "arcon_tui")]
    fn setup(
        arcon_conf: &ArconConf,
    ) -> (
        KompactSystem,
        Arc<Component<TuiComponent>>,
        crossbeam_channel::Receiver<TuiNode>,
    ) {
        let kompact_config = arcon_conf.kompact_conf();
        let system = kompact_config.build().expect("KompactSystem");
        let (arcon_sender, arcon_receiver) = crossbeam_channel::unbounded::<TuiNode>();
        let tui_c = TuiComponent::new(arcon_sender);
        let tui_component = system.create_dedicated(|| tui_c);
        let timeout = std::time::Duration::from_millis(500);
        system
            .start_notify(&tui_component)
            .wait_timeout(timeout)
            .expect("TuiComponent never started!");
        (system, tui_component, arcon_receiver)
    }

    /// Give out a mutable reference to the KompactSystem of the pipeline
    pub fn system(&mut self) -> &mut KompactSystem {
        &mut self.system
    }

    /// Give out a reference to the ArconConf of the pipeline
    pub fn arcon_conf(&self) -> &ArconConf {
        &self.conf
    }

    /*
    /// Adds a NodeManager to the Arcon Pipeline
    pub fn create_node_manager<IN, OUT>(
        &mut self,
        node_description: String,
        node_fn: &'static dyn SafelySendableFn(
            NodeDescriptor,
            NodeID,
            Vec<NodeID>,
            ChannelStrategy<OUT>,
            state::BackendType,
        ) -> DynamicNode<IN>,
        in_channels: Vec<NodeID>,
        nodes: Vec<DynamicNode<IN>>,
    ) -> Vec<CreatedDynamicNode<IN>>
    where
        IN: ArconType,
        OUT: ArconType,
    {
        let timeout = std::time::Duration::from_millis(500);
        let mut node_comps = Vec::with_capacity(nodes.len());
        // Create Node components
        for node in nodes {
            let node_comp = self.system.create_erased(node);
            self.system
                .start_notify(&node_comp)
                .wait_timeout(timeout)
                .expect("node never started!");
            node_comps.push(node_comp);
        }

        let node_manager = NodeManager::new(
            node_description.clone(),
            node_fn,
            in_channels,
            node_comps.clone(),
            None,
            None,
            #[cfg(feature = "arcon_tui")]
            self.tui_component.actor_ref().hold().expect("fail"),
        );

        let node_manager_comp = self.system.create(|| node_manager);
        let node_manager_ref = node_manager_comp.actor_ref().hold().expect("no");
        self.node_managers
            .insert(node_description, node_manager_ref);

        self.system
            .start_notify(&node_manager_comp)
            .wait_timeout(timeout)
            .expect("node_manager never started!");

        node_comps
    }
    */

    /// Awaits termination from the pipeline
    /// 
    /// Note that this blocks the current thread
    pub fn await_termination(self) {
        self.system.await_termination();
    }

    /// Shuts the pipeline down and consumes the struct
    pub fn shutdown(self) {
        let _ = self.system.shutdown();
    }

    /// Launches tui dashboard
    #[cfg(feature = "arcon_tui")]
    pub fn tui(&mut self) {
        // This is mainly code from ytop's main.rs
        use tui_helpers::*;

        better_panic::install();

        let draw_interval = Ratio::min(Ratio::from_integer(1), Ratio::from_integer(1));
        let cpu_interval = Ratio::from_integer(1);
        let mem_interval = Ratio::from_integer(1);
        let schemes = Colorschemes::from_str("default").unwrap();
        let colorscheme = read_colorscheme(std::path::Path::new(""), &schemes);
        let mut app = app::setup_app(cpu_interval, mem_interval, &colorscheme);

        let backend = CrosstermBackend::new(std::io::stdout());
        let mut terminal = Terminal::new(backend).unwrap();

        tui_helpers::setup_panic_hook();
        tui_helpers::setup_terminal();

        let ticker = tick(Duration::from_secs_f64(
            *draw_interval.numer() as f64 / *draw_interval.denom() as f64,
        ));
        let ui_events_receiver = tui_helpers::setup_ui_events();
        let ctrl_c_events = tui_helpers::setup_ctrl_c();

        let mut update_seconds = Ratio::from_integer(0);
        let mut paused = false;

        // Used to keep track of the previous key for actions that required 2 keypresses.
        let mut previous_key_event: Option<KeyEvent> = None;
        // If `skip_key` is set to true, we set the previous key to None instead of recording it.
        let mut skip_key: bool;
        // Used to keep track of whether we need to redraw the process or CPU/Mem widgets after they
        // have been updated.
        let mut node_modified: bool;
        let mut graphs_modified: bool;

        update_widgets(&mut app.widgets, update_seconds);
        draw(&mut terminal, &mut app);

        loop {
            select! {
                recv(ctrl_c_events) -> _ => {
                    break;
                }
                recv(self.arcon_event_receiver) -> message => {
                    app.widgets.node.node_update(message.expect("Recv Error"));
                    // let the normal ticker update do the drawing
                }
                recv(ticker) -> _ => {
                    if !paused {
                        update_seconds = (update_seconds + draw_interval) % Ratio::from_integer(60);
                        update_widgets(&mut app.widgets, update_seconds);
                        draw(&mut terminal, &mut app);
                    }
                }
                recv(ui_events_receiver) -> message => {
                    node_modified = false;
                    graphs_modified = false;
                    skip_key = false;

                    match message.unwrap() {
                        Event::Key(key_event) => {
                            if key_event.modifiers.is_empty() {
                                match key_event.code {
                                    KeyCode::Char('q') => {
                                        break
                                    },
                                    KeyCode::Char(' ') => {
                                        paused = !paused;
                                    },
                                    KeyCode::Char('j') | KeyCode::Down => {
                                        app.widgets.node.scroll_down();
                                        node_modified = true;
                                    },
                                    KeyCode::Char('k') | KeyCode::Up => {
                                        app.widgets.node.scroll_up();
                                        node_modified = true;
                                    },
                                    KeyCode::Char('g') => {
                                        if previous_key_event == Some(KeyEvent::from(KeyCode::Char('g'))) {
                                            app.widgets.node.scroll_top();
                                            node_modified = true;
                                            skip_key = true;
                                        }
                                    },
                                    KeyCode::Home => {
                                        app.widgets.node.scroll_top();
                                        node_modified = true;
                                    },
                                    KeyCode::Char('G') | KeyCode::End => {
                                        app.widgets.node.scroll_bottom();
                                        node_modified = true;
                                    },
                                    KeyCode::Char('h') => {
                                        app.widgets.cpu.scale_in();
                                        app.widgets.mem.scale_in();
                                        graphs_modified = true;
                                    },
                                    KeyCode::Char('l') => {
                                        app.widgets.cpu.scale_out();
                                        app.widgets.mem.scale_out();
                                        graphs_modified = true;
                                    },
                                    _ => {}
                                }
                            } else if key_event.modifiers == KeyModifiers::CONTROL {
                                match key_event.code {
                                    KeyCode::Char('c') => {
                                        break
                                    },
                                    KeyCode::Char('d') => {
                                        app.widgets.node.scroll_half_page_down();
                                        node_modified = true;
                                    },
                                    KeyCode::Char('u') => {
                                        app.widgets.node.scroll_half_page_up();
                                        node_modified = true;
                                    },
                                    KeyCode::Char('f') => {
                                        app.widgets.node.scroll_full_page_down();
                                        node_modified = true;
                                    },
                                    KeyCode::Char('b') => {
                                        app.widgets.node.scroll_full_page_up();
                                        node_modified = true;
                                    },
                                    _ => {}
                                }
                            }

                            previous_key_event = if skip_key {
                                None
                            } else {
                                Some(key_event)
                            };
                        }
                        // TODO: figure out why these aren't working
                        Event::Mouse(mouse_event) => match mouse_event {
                            MouseEvent::ScrollUp(_, _, _) => {
                                app.widgets.node.scroll_up();
                                node_modified = true;
                            },
                            MouseEvent::ScrollDown(_, _, _) => {
                                app.widgets.node.scroll_down();
                                node_modified = true;
                            },
                            _ => {},
                        }
                        Event::Resize(_width, _height) => {
                            draw(&mut terminal, &mut app);
                        }
                    }


                    if node_modified {
                        draw_node(&mut terminal, &mut app);
                    } else if graphs_modified {
                        draw_graphs(&mut terminal, &mut app);
                    }
                }
            }
        }

        tui_helpers::cleanup_terminal();
    }
}

#[cfg(feature = "arcon_tui")]
pub(crate) mod tui_helpers {
    pub(crate) use crate::tui::{
        colorscheme::*,
        draw::{draw, draw_graphs, draw_node},
        update::update_widgets,
        *,
    };
    pub(crate) use crossbeam_channel::{select, tick, unbounded, Receiver};
    pub(crate) use crossterm::{
        cursor,
        event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent},
        execute, terminal,
    };
    pub(crate) use num_rational::Ratio;
    pub(crate) use std::{
        io::{self, Write},
        panic,
        str::FromStr,
        time::Duration,
    };
    pub(crate) use tui::{backend::CrosstermBackend, Terminal};

    // Helper functions from ytop...

    pub fn setup_terminal() {
        let mut stdout = io::stdout();

        execute!(stdout, terminal::EnterAlternateScreen).unwrap();
        execute!(stdout, cursor::Hide).unwrap();

        // Needed for when ytop is run in a TTY since TTYs don't actually have an alternate screen.
        // Must be executed after attempting to enter the alternate screen so that it only clears the
        // 		primary screen if we are running in a TTY.
        // If not running in a TTY, then we just end up clearing the alternate screen which should have
        // 		no effect.
        execute!(stdout, terminal::Clear(terminal::ClearType::All)).unwrap();

        terminal::enable_raw_mode().unwrap();
    }

    pub(crate) fn cleanup_terminal() {
        let mut stdout = io::stdout();

        // Needed for when ytop is run in a TTY since TTYs don't actually have an alternate screen.
        // Must be executed before attempting to leave the alternate screen so that it only modifies the
        // 		primary screen if we are running in a TTY.
        // If not running in a TTY, then we just end up modifying the alternate screen which should have
        // 		no effect.
        execute!(stdout, cursor::MoveTo(0, 0)).unwrap();
        execute!(stdout, terminal::Clear(terminal::ClearType::All)).unwrap();

        execute!(stdout, terminal::LeaveAlternateScreen).unwrap();
        execute!(stdout, cursor::Show).unwrap();

        terminal::disable_raw_mode().unwrap();
    }

    pub fn setup_ui_events() -> Receiver<Event> {
        let (sender, receiver) = unbounded();
        std::thread::spawn(move || loop {
            sender.send(crossterm::event::read().unwrap()).unwrap();
        });

        receiver
    }

    pub fn setup_ctrl_c() -> Receiver<()> {
        let (sender, receiver) = unbounded();
        ctrlc::set_handler(move || {
            sender.send(()).unwrap();
        })
        .unwrap();

        receiver
    }

    // We need to catch panics since we need to close the UI and cleanup the terminal before logging any
    // error messages to the screen.
    pub(crate) fn setup_panic_hook() {
        panic::set_hook(Box::new(|panic_info| {
            cleanup_terminal();
            better_panic::Settings::auto().create_panic_handler()(panic_info);
        }));
    }
}
