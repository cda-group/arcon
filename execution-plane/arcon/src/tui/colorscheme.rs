// Copyright (c) 2019 Caleb Bassi
// SPDX-License-Identifier: MIT

use std::{fs, path::Path, str::FromStr};

use serde::Deserialize;
use tui::style::{Color, Style};

pub enum Colorschemes {
    Default,
    DefaultDark,
    Monokai,
    SolarizedDark,
    Vice,
    Custom(String),
}

impl FromStr for Colorschemes {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "default" => Colorschemes::Default,
            "default-dark" => Colorschemes::DefaultDark,
            "monokai" => Colorschemes::Monokai,
            "solarized-dark" => Colorschemes::SolarizedDark,
            "vice" => Colorschemes::Vice,
            _ => Colorschemes::Custom(s.to_string()),
        })
    }
}

#[derive(Deserialize)]
struct ColorschemeRaw {
    fg: i64,
    bg: i64,

    titles: i64,
    borders: i64,

    battery_lines: Vec<i64>,

    // need at least 8 entries
    cpu_lines: Vec<i64>,

    mem_main: i64,
    mem_swap: i64,

    net_bars: i64,

    proc_cursor: i64,

    temp_low: i64,
    temp_high: i64,
}

pub struct Colorscheme {
    pub text: Style,

    pub titles: Style,
    pub borders: Style,

    pub battery_lines: Vec<Style>,

    // need at least 8 entries
    pub cpu_lines: Vec<Style>,

    pub mem_main: Style,
    pub mem_swap: Style,

    pub net_bars: Style,

    pub proc_cursor: Color,

    pub temp_low: Style,
    pub temp_high: Style,
}

impl From<ColorschemeRaw> for Colorscheme {
    fn from(raw: ColorschemeRaw) -> Self {
        Colorscheme {
            text: Style::default()
                .fg(convert_color(raw.fg))
                .bg(convert_color(raw.bg)),

            titles: Style::default().fg(convert_color(raw.titles)),
            borders: Style::default().fg(convert_color(raw.borders)),

            battery_lines: raw
                .battery_lines
                .into_iter()
                .map(|entry| Style::default().fg(convert_color(entry)))
                .collect(),

            cpu_lines: raw
                .cpu_lines
                .into_iter()
                .map(|entry| Style::default().fg(convert_color(entry)))
                .collect(),

            mem_main: Style::default().fg(convert_color(raw.mem_main)),
            mem_swap: Style::default().fg(convert_color(raw.mem_swap)),

            net_bars: Style::default().fg(convert_color(raw.net_bars)),

            proc_cursor: convert_color(raw.proc_cursor),

            temp_low: Style::default().fg(convert_color(raw.temp_low)),
            temp_high: Style::default().fg(convert_color(raw.temp_high)),
        }
    }
}

fn convert_color(raw: i64) -> Color {
    if raw == -1 {
        Color::Reset
    } else {
        Color::Indexed(raw as u8)
    }
}

fn parse_colorscheme(config_folder: &Path, colorscheme: &Colorschemes) -> ColorschemeRaw {
    match colorscheme {
        Colorschemes::Custom(name) => serde_json::from_str(
            &fs::read_to_string(config_folder.join(name).with_extension("json")).unwrap(),
        )
        .unwrap(),
        _ => {
            let json_string = match colorscheme {
                Colorschemes::Default => include_str!("schemes/default.json"),
                Colorschemes::Monokai => include_str!("schemes/monokai.json"),
                /*
                Colorschemes::DefaultDark => include_str!("../colorschemes/default-dark.json"),
                Colorschemes::SolarizedDark => include_str!("../colorschemes/solarized-dark.json"),
                Colorschemes::Vice => include_str!("../colorschemes/vice.json"),
                */
                _ => unreachable!(),
            };
            serde_json::from_str(json_string)
                .expect("statically defined and verified json colorschemes")
        }
    }
}

pub fn read_colorscheme(config_folder: &Path, colorscheme: &Colorschemes) -> Colorscheme {
    parse_colorscheme(config_folder, colorscheme).into()
}
