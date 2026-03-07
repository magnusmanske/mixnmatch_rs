use crate::match_state::MatchState;

#[derive(Debug, Clone, Default)]
pub struct EntryQuery {
    pub catalog_id: Option<usize>,
    pub entry_type: Option<String>,
    pub min_dates: Option<u8>,
    pub min_aux: Option<usize>,
    pub match_state: Option<MatchState>,
    pub has_description: bool,
    pub has_coordinates: bool,
    pub desc_hint: Option<String>,
    pub name_regexp: Option<String>,
    pub limit: Option<usize>,
    pub ext_ids: Option<Vec<String>>,
    pub offset: Option<usize>,
}

impl EntryQuery {
    /// Only return entries with the specified catalog ID.
    pub fn with_catalog_id(mut self, catalog_id: usize) -> Self {
        self.catalog_id = Some(catalog_id);
        self
    }

    /// Only return entries with the specified type (eg "Q5").
    pub fn with_type(mut self, entry_type: &str) -> Self {
        self.entry_type = Some(entry_type.to_string());
        self
    }

    /// Only return entries where `ext_name` fits the specified regexp.
    pub fn with_name_regexp(mut self, name_regexp: &str) -> Self {
        self.name_regexp = Some(name_regexp.to_string());
        self
    }

    /// Only return entries with at least the specified number of person dates.
    pub fn with_min_dates(mut self, num_dates: u8) -> Self {
        self.min_dates = Some(num_dates);
        self
    }

    /// Only return entries with at least the specified number of auxiliary properties.
    pub fn with_min_aux(mut self, num_aux: usize) -> Self {
        self.min_aux = Some(num_aux);
        self
    }

    /// Only return entries with the specified match state.
    pub fn with_match_state(mut self, match_state: MatchState) -> Self {
        self.match_state = Some(match_state);
        self
    }

    /// Only return entries that have a description.
    pub fn with_description(mut self) -> Self {
        self.has_description = true;
        self
    }

    /// Only return entries that have coordinates.
    pub fn with_coordinates(mut self) -> Self {
        self.has_coordinates = true;
        self
    }

    /// Only return entries that have a description hint.
    pub fn with_desc_hint(mut self, desc_hint: &str) -> Self {
        self.desc_hint = Some(desc_hint.to_string());
        self
    }

    /// Only return entries with the specified external IDs.
    /// Requires a catalog ID to be specified.
    pub fn with_ext_ids(mut self, ext_ids: Vec<String>) -> Self {
        self.ext_ids = Some(ext_ids);
        self
    }

    /// Only return entries with the specified limit.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Only return entries with the specified offset.
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let q = EntryQuery::default();
        assert!(q.catalog_id.is_none());
        assert!(q.entry_type.is_none());
        assert!(q.min_dates.is_none());
        assert!(q.min_aux.is_none());
        assert!(q.match_state.is_none());
        assert!(!q.has_description);
        assert!(!q.has_coordinates);
        assert!(q.desc_hint.is_none());
        assert!(q.name_regexp.is_none());
        assert!(q.limit.is_none());
        assert!(q.ext_ids.is_none());
        assert!(q.offset.is_none());
    }

    #[test]
    fn test_with_catalog_id() {
        let q = EntryQuery::default().with_catalog_id(42);
        assert_eq!(q.catalog_id, Some(42));
    }

    #[test]
    fn test_with_type() {
        let q = EntryQuery::default().with_type("Q5");
        assert_eq!(q.entry_type, Some("Q5".to_string()));
    }

    #[test]
    fn test_with_name_regexp() {
        let q = EntryQuery::default().with_name_regexp("^John");
        assert_eq!(q.name_regexp, Some("^John".to_string()));
    }

    #[test]
    fn test_with_min_dates() {
        let q = EntryQuery::default().with_min_dates(2);
        assert_eq!(q.min_dates, Some(2));
    }

    #[test]
    fn test_with_min_aux() {
        let q = EntryQuery::default().with_min_aux(3);
        assert_eq!(q.min_aux, Some(3));
    }

    #[test]
    fn test_with_match_state() {
        let q = EntryQuery::default().with_match_state(MatchState::unmatched());
        assert!(q.match_state.is_some());
    }

    #[test]
    fn test_with_description() {
        let q = EntryQuery::default().with_description();
        assert!(q.has_description);
    }

    #[test]
    fn test_with_coordinates() {
        let q = EntryQuery::default().with_coordinates();
        assert!(q.has_coordinates);
    }

    #[test]
    fn test_with_desc_hint() {
        let q = EntryQuery::default().with_desc_hint("painter");
        assert_eq!(q.desc_hint, Some("painter".to_string()));
    }

    #[test]
    fn test_with_ext_ids() {
        let ids = vec!["a".to_string(), "b".to_string()];
        let q = EntryQuery::default().with_ext_ids(ids.clone());
        assert_eq!(q.ext_ids, Some(ids));
    }

    #[test]
    fn test_with_limit() {
        let q = EntryQuery::default().with_limit(100);
        assert_eq!(q.limit, Some(100));
    }

    #[test]
    fn test_with_offset() {
        let q = EntryQuery::default().with_offset(50);
        assert_eq!(q.offset, Some(50));
    }

    #[test]
    fn test_chained_builders() {
        let q = EntryQuery::default()
            .with_catalog_id(5)
            .with_type("Q5")
            .with_min_dates(1)
            .with_match_state(MatchState::unmatched())
            .with_limit(10)
            .with_offset(20);
        assert_eq!(q.catalog_id, Some(5));
        assert_eq!(q.entry_type, Some("Q5".to_string()));
        assert_eq!(q.min_dates, Some(1));
        assert!(q.match_state.is_some());
        assert_eq!(q.limit, Some(10));
        assert_eq!(q.offset, Some(20));
    }
}
