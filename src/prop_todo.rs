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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_defaults() {
        let pt = PropTodo::new(123, "Test property".to_string());
        assert_eq!(pt.id, 0);
        assert_eq!(pt.prop_num, 123);
        assert_eq!(pt.name, "Test property");
        assert_eq!(pt.default_type, "");
        assert_eq!(pt.status, "NO_CATALOG");
        assert_eq!(pt.note, "");
        assert_eq!(pt.user_id, 0);
        assert!(pt.items_using.is_none());
        assert!(pt.number_of_records.is_none());
    }

    #[test]
    fn test_default() {
        let pt = PropTodo::default();
        assert_eq!(pt.id, 0);
        assert_eq!(pt.prop_num, 0);
        assert_eq!(pt.name, "");
        assert_eq!(pt.default_type, "");
        assert_eq!(pt.status, "");
        assert_eq!(pt.note, "");
        assert_eq!(pt.user_id, 0);
        assert!(pt.items_using.is_none());
        assert!(pt.number_of_records.is_none());
    }
}
