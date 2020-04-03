use crate::tui::{
    colorscheme::Colorscheme,
    widgets::{cpu::CpuWidget, mem::MemWidget, net::NetWidget, node::NodeWidget},
};
use num_rational::Ratio;

pub struct App<'a, 'b> {
    pub widgets: Widgets<'a, 'b>,
}

pub struct Widgets<'a, 'b> {
    pub cpu: CpuWidget<'a>,
    pub mem: MemWidget<'a>,
    pub net: NetWidget<'a, 'b>,
    pub node: NodeWidget<'a>,
}

pub fn setup_app<'a, 'b>(
    cpu_update_interval: Ratio<u64>,
    mem_update_interval: Ratio<u64>,
    colorscheme: &'a Colorscheme,
    program_name: &str,
) -> App<'a, 'b> {
    let cpu = CpuWidget::new(colorscheme, cpu_update_interval, false, true);
    let mem = MemWidget::new(colorscheme, mem_update_interval);
    let net = NetWidget::new(colorscheme, "all");
    let node = NodeWidget::new(colorscheme);

    App {
        widgets: Widgets {
            cpu,
            mem,
            net,
            node,
        },
    }
}
