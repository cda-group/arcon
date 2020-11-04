// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// This module is based on the great work of the ytop project.
// Reference: https://github.com/cjbassi/ytop

pub mod component;
pub mod widgets;

pub(crate) mod app;
pub(crate) mod colorscheme;
pub(crate) mod draw;
pub(crate) mod update;

pub use crossbeam_channel::{unbounded, Receiver};

use crate::{
    colorscheme::*,
    draw::{draw, draw_graphs, draw_node},
    update::update_widgets,
    widgets::node::Node,
};

use crossbeam_channel::{select, tick};
use crossterm::{
    cursor,
    event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent},
    execute, terminal,
};
use num_rational::Ratio;
use std::{
    io::{self, Write},
    panic,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tui::{backend::CrosstermBackend, Terminal};

pub fn tui(arcon_event_receiver: Arc<Receiver<Node>>) {
    // This is mainly code from ytop's main.rs
    better_panic::install();

    let draw_interval = Ratio::min(Ratio::from_integer(1), Ratio::from_integer(1));
    let cpu_interval = Ratio::from_integer(1);
    let mem_interval = Ratio::from_integer(1);
    let schemes = Colorschemes::from_str("default").unwrap();
    let colorscheme = read_colorscheme(std::path::Path::new(""), &schemes);
    let mut app = app::setup_app(cpu_interval, mem_interval, &colorscheme);

    let backend = CrosstermBackend::new(std::io::stdout());
    let mut terminal = Terminal::new(backend).unwrap();

    setup_panic_hook();
    setup_terminal();

    let ticker = tick(Duration::from_secs_f64(
        *draw_interval.numer() as f64 / *draw_interval.denom() as f64,
    ));
    let ui_events_receiver = setup_ui_events();
    let ctrl_c_events = setup_ctrl_c();

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
            recv(arcon_event_receiver) -> _ => {
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

    cleanup_terminal();
}

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
