use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout, Rect},
    widgets::Widget as _,
    Frame, Terminal,
};

use crate::tui::app::{App, Widgets};

pub fn draw<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) {
    terminal
        .draw(|mut frame| {
            let chunks = Layout::default()
                .constraints(vec![Constraint::Percentage(100)])
                .split(frame.size());
            draw_widgets(&mut frame, &mut app.widgets, chunks[0]);
        })
        .unwrap();
}

pub fn draw_widgets<B: Backend>(frame: &mut Frame<B>, widgets: &mut Widgets, area: Rect) {
    /*
    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)].as_ref())
        .split(area);
    draw_top_row(frame, widgets, vertical_chunks[0]);
    draw_bottom_row(frame, widgets, vertical_chunks[1]);
    */
    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Ratio(1, 3),
                Constraint::Ratio(1, 3),
                Constraint::Ratio(1, 3),
            ]
            .as_ref(),
        )
        .split(area);
    draw_top_row(frame, widgets, vertical_chunks[0]);
    draw_middle_row(frame, widgets, vertical_chunks[1]);
    draw_bottom_row(frame, widgets, vertical_chunks[2]);
    /*
    if widgets.temp.is_some() {
    } else {
    }
    */
}

// NODES?
pub fn draw_top_row<B: Backend>(frame: &mut Frame<B>, widgets: &mut Widgets, area: Rect) {
    let horizontal_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(100)].as_ref())
        .split(area);
    widgets.node.render(frame, horizontal_chunks[0]);
    /*
    if let Some(net) = widgets.net.as_mut() {
        net.render(frame, horizontal_chunks[0]);
    } else {
        widgets.mem.render(frame, horizontal_chunks[0]);
    }
    */
    //widgets.proc.render(frame, horizontal_chunks[1]);
}

// CPU
pub fn draw_middle_row<B: Backend>(frame: &mut Frame<B>, widgets: &mut Widgets, area: Rect) {
    let horizontal_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(100)].as_ref())
        .split(area);
    widgets.cpu.render(frame, horizontal_chunks[0]);
}

// NET / MEM
pub fn draw_bottom_row<B: Backend>(frame: &mut Frame<B>, widgets: &mut Widgets, area: Rect) {
    let horizontal_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 3), Constraint::Ratio(2, 3)].as_ref())
        .split(area);
    widgets.net.render(frame, horizontal_chunks[0]);
    widgets.mem.render(frame, horizontal_chunks[1]);
}

/*
pub fn draw_help_menu<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) {
    terminal
        .draw(|mut frame| {
            let rect = app.help_menu.get_rect(frame.size());
            app.help_menu.render(&mut frame, rect);
        })
        .unwrap();
}
*/

// TODO: figure out how to draw the proc widget without clearing rest of the screen
pub fn draw_node<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) {
    draw(terminal, app);
    // terminal.draw(|mut frame| {
    // 	let chunks = if app.statusbar.is_some() {
    // 		Layout::default()
    // 			.constraints([Constraint::Min(0), Constraint::Length(1)].as_ref())
    // 			.split(frame.size())
    // 	} else {
    // 		Layout::default()
    // 			.constraints(vec![Constraint::Percentage(100)])
    // 			.split(frame.size())
    // 	};

    // 	let vertical_chunks = if app.widgets.temp.is_some() {
    // 		Layout::default()
    // 			.direction(Direction::Vertical)
    // 			.constraints(
    // 				[
    // 					Constraint::Ratio(1, 3),
    // 					Constraint::Ratio(1, 3),
    // 					Constraint::Ratio(1, 3),
    // 				]
    // 				.as_ref(),
    // 			)
    // 			.split(chunks[0])
    // 	} else {
    // 		Layout::default()
    // 			.direction(Direction::Vertical)
    // 			.constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)].as_ref())
    // 			.split(chunks[0])
    // 	};

    // 	let horizontal_chunks = Layout::default()
    // 		.direction(Direction::Horizontal)
    // 		.constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)].as_ref())
    // 		.split(*vertical_chunks.last().unwrap());
    // 	app.widgets.proc.render(&mut frame, horizontal_chunks[1]);
    // })
}

// TODO: figure out how to draw the graphs without clearing rest of the screen
pub fn draw_graphs<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) {
    draw(terminal, app);
}
