use tui::{
    style::{Color, Modifier, Style},
    widgets::{Block, Borders},
};

use crate::tui::colorscheme::Colorscheme;

pub fn new<'a>(colorscheme: &Colorscheme, title: &'a str) -> Block<'a> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(colorscheme.borders)
        .title(title)
        .title_style(colorscheme.titles)
}
