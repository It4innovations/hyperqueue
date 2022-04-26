use chrono::{DateTime, Local};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tako::common::Map;
use tui::layout::Rect;
use tui::style::{Color, Modifier, Style};
use tui::symbols::Marker;
use tui::text::Span;
use tui::widgets::{Axis, Block, Borders, Chart, Dataset};

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::styles::chart_style_deselected;
use crate::dashboard::ui::terminal::DashboardFrame;

/// Generic chart that shows graphs described by different `ChartPlotter`.
pub struct DashboardChart {
    /// The Chart's name
    label: String,
    /// The charts shown on the `DashboardChart`.
    chart_plotters: Vec<Box<dyn ChartPlotter>>,
    /// Chart's data, for each of the different plots on the chart.  
    datasets: Map<String, Vec<(f64, f64)>>,
    /// The style of a plot on the chart.
    dataset_styles: Map<String, PlotStyle>,
    /// The end time of the data displayed on the chart,
    end_time: SystemTime,
    /// The duration for which the data is plotted for.
    view_size: Duration,
}

#[derive(Copy, Clone)]
pub struct PlotStyle {
    pub color: Color,
    pub marker: Marker,
}

/// Declares a group of charts plotted on the `DashboardChart` with `get_charts`
/// and defines how data will be fetched for each chart with `fetch_data_points`
/// Multiple `ChartPlotters` can be used to add different charts to `DashboardChart`
/// with each of the charts fetching their own data.
pub trait ChartPlotter {
    /// Declares the labels of the charts that will be plotted by this plotter and their style.
    fn get_charts(&self) -> Map<String, PlotStyle>;

    /// Fetches one set of points for each of the keys returned by `get_charts`
    /// at given `time`
    fn fetch_data_points(&self, data: &DashboardData, time: SystemTime) -> Map<String, (f64, f64)>;
}

impl DashboardChart {
    /// Sets a label for the chart
    pub fn set_chart_name(&mut self, chart_name: &str) {
        self.label = chart_name.to_string();
    }

    /// Adds a chart to be plotted by `DashboardChart`
    pub fn add_chart_plotter(&mut self, chart_plotter: Box<dyn ChartPlotter>) {
        self.chart_plotters.push(chart_plotter);
    }

    pub fn update(&mut self, data: &DashboardData, display_time: SystemTime) {
        self.end_time = display_time;
        // Clears the old data, TODO: load only the new data points
        self.datasets.clear();
        let mut start = self.end_time - self.view_size;
        let mut times = vec![];
        while start <= self.end_time {
            times.push(start);
            start += Duration::from_secs(1);
        }

        let styles: Map<_, _> = self
            .chart_plotters
            .iter()
            .flat_map(move |fetcher| fetcher.get_charts())
            .collect();

        let data_points: Vec<(_, _)> = times
            .iter()
            .flat_map(|time| {
                self.chart_plotters
                    .iter()
                    .flat_map(|fetcher| fetcher.fetch_data_points(data, *time))
            })
            .collect();

        for (key, data) in data_points.iter() {
            if !self.datasets.contains_key(&*key) {
                let style = styles.get(key).copied();
                self.add_chart(key.to_string(), style);
            }
            self.datasets.get_mut(key).unwrap().push(*data);
        }
    }

    /// Adds a chart with empty data and input `PlotStyle`, if `plt_style` is None, it's set to default.
    fn add_chart(&mut self, chart_label: String, plt_style: Option<PlotStyle>) {
        self.datasets.insert(chart_label.clone(), vec![]);
        match plt_style {
            Some(plot_style) => self.dataset_styles.insert(chart_label, plot_style),
            None => self.dataset_styles.insert(chart_label, Default::default()),
        };
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let mut y_max: f64 = 0.0;
        let datasets: Vec<Dataset> = self
            .datasets
            .iter()
            .map(|(label, dataset)| {
                let style = self.dataset_styles.get(label).unwrap();
                y_max = dataset
                    .iter()
                    .map(|(_, y)| if *y > y_max { *y } else { y_max })
                    .fold(y_max, f64::max);
                Dataset::default()
                    .name(label)
                    .marker(style.marker)
                    .style(Style::default().fg(style.color))
                    .data(dataset)
            })
            .collect();

        let start_time_label: DateTime<Local> = (self.end_time - self.view_size).into();
        let end_time_label: DateTime<Local> = self.end_time.into();
        let chart = Chart::new(datasets)
            .style(chart_style_deselected())
            .block(
                Block::default()
                    .title(Span::styled(
                        &self.label,
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ))
                    .borders(Borders::ALL),
            )
            .x_axis(
                Axis::default()
                    .title("t->")
                    .style(Style::default().fg(Color::Gray))
                    .bounds([
                        get_time_as_secs(self.end_time - self.view_size) as f64,
                        get_time_as_secs(self.end_time) as f64,
                    ])
                    .labels(vec![
                        Span::from(start_time_label.format("%H:%M").to_string()),
                        Span::from(end_time_label.format("%H:%M").to_string()),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().fg(Color::Gray))
                    .bounds([0.00, y_max])
                    .labels(vec![
                        Span::from(0u8.to_string()),
                        Span::from(y_max.to_string()),
                    ]),
            );
        frame.render_widget(chart, rect);
    }
}

impl Default for DashboardChart {
    fn default() -> Self {
        Self {
            label: "Chart".to_string(),
            chart_plotters: vec![],
            datasets: Default::default(),
            dataset_styles: Default::default(),
            end_time: SystemTime::now(),
            view_size: Duration::from_secs(300),
        }
    }
}

impl Default for PlotStyle {
    fn default() -> Self {
        PlotStyle {
            color: Color::Magenta,
            marker: Marker::Dot,
        }
    }
}

fn get_time_as_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_secs()
}
