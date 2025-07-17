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
