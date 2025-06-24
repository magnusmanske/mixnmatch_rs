#[derive(Debug, Default)]
pub struct PropTodo {
    pub id: usize,
    pub prop_num: u64,
    pub name: String,
    pub default_type: String,
    pub status: String,
    pub note: String,
    pub user_id: u64,
    pub items_using: Option<u64>,
    pub number_of_records: Option<u64>,
}

impl PropTodo {
    pub fn new(prop_num: u64, name: String) -> Self {
        Self {
            prop_num,
            name,
            status: "NO_CATALOG".to_string(),
            ..Default::default()
        }
    }

    pub fn from_row(r: mysql_async::Row) -> Option<Self> {
        Some(Self {
            id: r.get(0)?,
            prop_num: r.get(1)?,
            name: r.get(2)?,
            default_type: r.get(3)?,
            status: r.get(4)?,
            note: r.get(5)?,
            user_id: r.get(6)?,
            items_using: r.get(7)?,
            number_of_records: r.get(8)?,
        })
    }
}
