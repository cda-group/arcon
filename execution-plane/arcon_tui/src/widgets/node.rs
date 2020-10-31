// Copyright (c) 2019 Caleb Bassi
// SPDX-License-Identifier: MIT
// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use num_rational::Ratio;
use std::collections::HashMap;
use tui::{
    buffer::Buffer,
    layout::{Constraint, Rect},
    style::Modifier,
    widgets::{Row, Table, Widget},
};

use crate::{colorscheme::Colorscheme, update::UpdatableWidget, widgets::block};

#[derive(Debug, Clone)]
pub struct Node {
    pub(crate) description: String,
    pub(crate) id: u32,
    pub(crate) parallelism: usize,
    pub(crate) watermark: u64,
    pub(crate) epoch: u64,
    pub(crate) inbound_channels: usize,
    pub(crate) outbound_channels: usize,
    pub(crate) inbound_throughput_one_min: f64,
}

pub struct NodeWidget<'a> {
    title: String,
    update_interval: Ratio<u64>,
    colorscheme: &'a Colorscheme,
    grouping: bool,
    selected_row: usize,
    view_offset: usize,
    scrolled: bool,
    view_height: usize,
    nodes: Vec<Node>,
    grouped_nodes: HashMap<(u32, String), Node>,
}
impl NodeWidget<'_> {
    pub fn new(colorscheme: &Colorscheme) -> NodeWidget {
        NodeWidget {
            title: " Arcon Nodes ".to_string(),
            update_interval: Ratio::from_integer(1),
            colorscheme,
            grouping: true,
            selected_row: 0,
            view_offset: 0,
            scrolled: false,
            view_height: 0,
            nodes: Vec::new(),
            grouped_nodes: HashMap::new(),
        }
    }

    pub fn node_update(&mut self, node: Node) {
        let descriptor = node.description.clone();
        let id = node.id;
        self.grouped_nodes.insert((id, descriptor), node);
    }

    fn scroll_count(&mut self, count: isize) {
        self.selected_row = isize::max(0, self.selected_row as isize + count) as usize;
        self.scrolled = true;
    }

    fn scroll_to(&mut self, count: usize) {
        self.selected_row = usize::min(
            count,
            if self.grouping {
                self.grouped_nodes.len()
            } else {
                self.nodes.len()
            } - 1,
        );
        self.scrolled = true;
    }

    pub fn scroll_up(&mut self) {
        self.scroll_count(-1);
    }

    pub fn scroll_down(&mut self) {
        self.scroll_count(1);
    }

    pub fn scroll_top(&mut self) {
        self.scroll_to(0);
    }

    pub fn scroll_bottom(&mut self) {
        self.scroll_to(if self.grouping {
            self.grouped_nodes.len()
        } else {
            self.nodes.len()
        });
    }

    pub fn scroll_half_page_down(&mut self) {
        self.scroll_count(self.view_height as isize / 2);
    }

    pub fn scroll_half_page_up(&mut self) {
        self.scroll_count(-(self.view_height as isize / 2));
    }

    pub fn scroll_full_page_down(&mut self) {
        self.scroll_count(self.view_height as isize);
    }

    pub fn scroll_full_page_up(&mut self) {
        self.scroll_count(-(self.view_height as isize));
    }
}
impl UpdatableWidget for NodeWidget<'_> {
    fn update(&mut self) {}

    fn get_update_interval(&self) -> Ratio<u64> {
        self.update_interval
    }
}

impl Widget for NodeWidget<'_> {
    fn draw(&mut self, area: Rect, buf: &mut Buffer) {
        if area.height < 3 {
            return;
        }

        let inner = Rect {
            x: area.x + 1,
            y: area.y + 1,
            width: area.width - 2,
            height: area.height - 2,
        };

        self.view_height = inner.height as usize - 1;

        let nodes = if self.grouping {
            self.grouped_nodes.values().cloned().collect()
        } else {
            self.nodes.clone()
        };

        let header = [
            "Count",
            "Name",
            "ID",
            "Watermark",
            "Epoch",
            "Inbound Channels",
            "Outbound Channels",
            "Inbound rate (1MIN)",
        ];

        if self.scrolled {
            self.scrolled = false;
            if self.selected_row > inner.height as usize + self.view_offset - 2 {
                self.view_offset = self.selected_row + 2 - inner.height as usize;
            } else if self.selected_row < self.view_offset {
                self.view_offset = self.selected_row;
            }
        }

        Table::new(
            header.iter(),
            nodes.into_iter().skip(self.view_offset).map(|node| {
                Row::StyledData(
                    vec![
                        node.parallelism.to_string(),
                        if self.grouping {
                            node.description
                        } else {
                            node.id.to_string()
                        },
                        node.id.to_string(),
                        node.watermark.to_string(),
                        node.epoch.to_string(),
                        node.inbound_channels.to_string(),
                        node.outbound_channels.to_string(),
                        format!("{:2.1}", node.inbound_throughput_one_min),
                    ]
                    .into_iter(),
                    self.colorscheme.text,
                )
            }),
        )
        .block(block::new(self.colorscheme, &self.title))
        .header_style(self.colorscheme.text.modifier(Modifier::BOLD))
        // TODO: this is only a temporary workaround until we fix the table column resizing
        // https://github.com/cjbassi/ytop/issues/23
        .widths(&[
            Constraint::Length(6),
            Constraint::Length(u16::max((area.width as i16 - 2 - 124 - 3) as u16, 5)),
            Constraint::Length(12),
            Constraint::Length(20),
            Constraint::Length(20),
            Constraint::Length(20),
            Constraint::Length(20),
            Constraint::Length(20),
        ])
        .column_spacing(1)
        .header_gap(0)
        .draw(area, buf);

        // Draw cursor.
        let cursor_y = inner.y + 1 + self.selected_row as u16 - self.view_offset as u16;
        if cursor_y < inner.bottom() {
            for i in inner.x..inner.right() {
                let cell = buf.get_mut(i, cursor_y);
                if cell.symbol != " " {
                    cell.set_modifier(Modifier::REVERSED);
                    cell.set_fg(self.colorscheme.proc_cursor);
                } else {
                    cell.set_bg(self.colorscheme.proc_cursor);
                }
            }
        }
    }
}
