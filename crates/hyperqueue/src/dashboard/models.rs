use tui::widgets::TableState;

#[derive(Clone)]
pub struct StatefulTable<T> {
    pub state: TableState,
    pub items: Vec<T>,
}

impl<T> StatefulTable<T> {
    pub fn new() -> StatefulTable<T> {
        StatefulTable {
            state: TableState::default(),
            items: Vec::new(),
        }
    }

    pub fn set_items(&mut self, items: Vec<T>) {
        let item_len = items.len();
        self.items = items;
        if !self.items.is_empty() {
            let i = self.state.selected().map_or(0, |i| {
                if i > 0 && i < item_len {
                    i
                } else if i >= item_len {
                    item_len - 1
                } else {
                    0
                }
            });
            self.state.select(Some(i));
        }
    }
}
