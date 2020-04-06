// Copyright (c) 2019 Caleb Bassi
// SPDX-License-Identifier: MIT

use crate::tui::colorscheme::Colorscheme;
use tui::widgets::{Block, Borders};

pub fn new<'a>(colorscheme: &Colorscheme, title: &'a str) -> Block<'a> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(colorscheme.borders)
        .title(title)
        .title_style(colorscheme.titles)
}
