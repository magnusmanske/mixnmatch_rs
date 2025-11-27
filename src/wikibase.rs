use crate::{app_state::AppState, catalog::Catalog, extended_entry::ExtendedEntry};
use anyhow::{Result, anyhow};
use mediawiki::Api;
use serde_json::Value;
use wikibase::{EntityTrait, ItemEntity, LocaleString, Snak, Statement};

/*
This requires config.json to have a section like:
"wikibase": {"api": "https://mix-n-match.wikibase.cloud/w/api.php","token":".."},
*/

const WIKIBASE_ITEM_NAMESPACE: &str = "120";
const ITEM_CATALOG: usize = 7;
// const ITEM_ENTRY: usize = 9;
// const ITEM_NAME_DATE_MATCHER: usize =14;
const ITEM_ACTIVE: usize = 76;
const ITEM_INACTIVE: usize = 77;
const ITEM_AUTOMATIC_NAME_DATE_MATCHER: usize = 14;
const ITEM_AUXILIARY_DATA_MATCHER: usize = 81;
const ITEM_AUTOMATIC_LOCATION_MATCHER: usize = 82;
const ITEM_CERSEI_IMPORTER: usize = 83;
const ITEM_AUTOMATIC_WORKS_MATCHER: usize = 84;

const PROP_CATALOG: usize = 10;
const PROP_CATALOG_TYPE: usize = 8;
const PROP_EXTERNAL_ID: usize = 11;
const PROP_EXTERNAL_URL: usize = 7;
// const PROP_FORMATTER_URL: usize = 14;
// const PROP_FULLY_MATCHED: usize = 12;
// const PROP_IMAGE: usize = 4;
const PROP_INSTANCE_OF: usize = 5;
const PROP_MIXNMATCH_ALGORITHM: usize = 16;
// const PROP_MOTHER: usize = 2;
const PROP_OLD_MIXNMATCH_CATALOG_ID: usize = 18;
const PROP_OLD_MIXNMATCH_ENTITY_ID: usize = 19;
// const PROP_PRELIMINARILY_MATCHED: usize = 13;
// const PROP_SUBCLASS_OF: usize = 6;
// const PROP_TIMESTAMP: usize = 17;
const PROP_WIKIDATA_PROPERTY: usize = 9;
const PROP_WIKIDATA_USER: usize = 15;
const PROP_WIKIPEDIA_LANGUAGE_CODE: usize = 20;
const PROP_STATUS: usize = 21;
const PROP_WIKIDATA_SOURCE_ITEM: usize = 22;
const PROP_ENTRY_TYPE: usize = 23;
const PROP_BORN: usize = 24;
const PROP_DIED: usize = 25;
const PROP_COORDINATES: usize = 26;

enum CatalogType {
    Biography = 12,
    Religion = 16,
    PoliticsBiography = 17,
    Literature = 18,
    Food = 19,
    Chemistry = 20,
    Art = 21,
    MusicArtists = 22,
    Organisation = 23,
    Geography = 24,
    General = 25,
    Encyclopedia = 26,
    Thesaurus = 27,
    Genealogy = 28,
    Science = 29,
    Infrastructure = 30,
    Media = 31,
    Language = 32,
    SportBiography = 33,
    Unknown = 34,
    Journals = 35,
    Entertainment = 36,
    Biology = 37,
    Heritage = 38,
    Cinema = 39,
    ChessBiography = 40,
    Location = 41,
    Software = 42,
    TelevisionEpisodes = 43,
    Archives = 44,
    Music = 45,
    VideoGames = 46,
    Medical = 47,
    Bibliography = 48,
    Events = 49,
    Failed = 50,
    VisualArts = 51,
    Pornography = 52,
    Books = 53,
    Technology = 54,
    Philately = 55,
    MusicWorks = 56,
    TelevisionSeries = 57,
    Duplicates = 58,
    Publishers = 59,
    SportOrganisation = 60,
    VideoGamePlatforms = 61,
    VideoGameTopics = 62,
    VideoGamePeople = 63,
    VideoGameSeries = 64,
    WolframLanguageEntity = 65,
    MusicReleases = 66,
    Newspapers = 67,
    Podcast = 68,
    VideoGameCompanies = 69,
    VideoGameGenres = 70,
    BotWorks = 71,
    MusicGenres = 72,
    VideoGameSources = 73,
    AuthorityControl = 74,
    MusicGenre = 75,
}

impl CatalogType {
    fn from_str(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "biography" => Some(Self::Biography),
            "religion" => Some(Self::Religion),
            "politics biography" => Some(Self::PoliticsBiography),
            "literature" => Some(Self::Literature),
            "food" => Some(Self::Food),
            "chemistry" => Some(Self::Chemistry),
            "art" => Some(Self::Art),
            "music artists" => Some(Self::MusicArtists),
            "organisation" => Some(Self::Organisation),
            "geography" => Some(Self::Geography),
            "general" => Some(Self::General),
            "encyclopedia" => Some(Self::Encyclopedia),
            "thesaurus" => Some(Self::Thesaurus),
            "genealogy" => Some(Self::Genealogy),
            "science" => Some(Self::Science),
            "infrastructure" => Some(Self::Infrastructure),
            "media" => Some(Self::Media),
            "language" => Some(Self::Language),
            "sport biography" => Some(Self::SportBiography),
            "unknown" => Some(Self::Unknown),
            "journals" => Some(Self::Journals),
            "entertainment" => Some(Self::Entertainment),
            "biology" => Some(Self::Biology),
            "heritage" => Some(Self::Heritage),
            "cinema" => Some(Self::Cinema),
            "chess biography" => Some(Self::ChessBiography),
            "location" => Some(Self::Location),
            "software" => Some(Self::Software),
            "television episodes" => Some(Self::TelevisionEpisodes),
            "archives" => Some(Self::Archives),
            "music" => Some(Self::Music),
            "video games" => Some(Self::VideoGames),
            "medical" => Some(Self::Medical),
            "bibliography" => Some(Self::Bibliography),
            "events" => Some(Self::Events),
            "~ failed" => Some(Self::Failed),
            "visual arts" => Some(Self::VisualArts),
            "pornography" => Some(Self::Pornography),
            "books" => Some(Self::Books),
            "technology" => Some(Self::Technology),
            "philately" => Some(Self::Philately),
            "music works" => Some(Self::MusicWorks),
            "television series" => Some(Self::TelevisionSeries),
            "~ duplicates" => Some(Self::Duplicates),
            "publishers" => Some(Self::Publishers),
            "sport organisation" => Some(Self::SportOrganisation),
            "video game platforms" => Some(Self::VideoGamePlatforms),
            "video game topics" => Some(Self::VideoGameTopics),
            "video game people" => Some(Self::VideoGamePeople),
            "video game series" => Some(Self::VideoGameSeries),
            "wolfram language entity" => Some(Self::WolframLanguageEntity),
            "music releases" => Some(Self::MusicReleases),
            "newspapers" => Some(Self::Newspapers),
            "podcast" => Some(Self::Podcast),
            "video game companies" => Some(Self::VideoGameCompanies),
            "video game genres" => Some(Self::VideoGameGenres),
            "~ bot works" => Some(Self::BotWorks),
            "music genres" => Some(Self::MusicGenres),
            "video game sources" => Some(Self::VideoGameSources),
            "authority control" => Some(Self::AuthorityControl),
            "music genre" => Some(Self::MusicGenre),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum WikiBaseUser {
    Automatch,
    Algorithm { id: usize, item: usize },
    User { id: usize, name: String },
}

impl WikiBaseUser {
    pub async fn new_from_id(user_id: usize, app: &AppState) -> Option<Self> {
        match user_id {
            0 => Some(Self::Automatch),
            2 => Some(Self::User {
                id: user_id,
                name: "Magnus Manske".to_string(),
            }),
            3 => Some(Self::Algorithm {
                id: user_id,
                item: ITEM_AUTOMATIC_NAME_DATE_MATCHER,
            }),
            4 => Some(Self::Algorithm {
                id: user_id,
                item: ITEM_AUXILIARY_DATA_MATCHER,
            }),
            5 => Some(Self::Algorithm {
                id: user_id,
                item: ITEM_AUTOMATIC_LOCATION_MATCHER,
            }),
            6 => Some(Self::Algorithm {
                id: user_id,
                item: ITEM_CERSEI_IMPORTER,
            }),
            7 => Some(Self::Algorithm {
                id: user_id,
                item: ITEM_AUTOMATIC_WORKS_MATCHER,
            }),
            _ => {
                // TODO cache user ID mappings
                let user_name = app.storage().get_user_name_from_id(user_id).await?;
                Some(Self::User {
                    id: user_id,
                    name: user_name,
                })
            }
        }
    }

    pub fn get_snak(&self) -> Option<Snak> {
        match self {
            Self::Automatch => None, // Automatch needs no user
            Self::User { id: _, name } => Some(Snak::new_external_id(
                format!("P{PROP_WIKIDATA_USER}"),
                name.to_string(),
            )),
            Self::Algorithm { id: _, item } => Some(Snak::new_item(
                format!("P{PROP_MIXNMATCH_ALGORITHM}"),
                format!("Q{item}"),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WikiBase {
    api: Api,
}

impl WikiBase {
    pub async fn new(j: &Value) -> Option<Self> {
        let url = j["api"].as_str()?;
        let token = j["token"].as_str()?;
        let mut api = Api::new(url).await.ok()?;
        api.set_oauth2(token);
        Some(Self { api })
    }

    pub fn api(&self) -> &Api {
        &self.api
    }

    pub async fn generate_entry_item(
        &self,
        app: &AppState,
        ext_entry: &ExtendedEntry,
        catalog_item: &str,
    ) -> Option<ItemEntity> {
        let mut item = ItemEntity::new_missing();
        let entry = &ext_entry.entry;
        // pub random: f64, NOT USED
        // pub q: Option<isize>,
        // pub user: Option<usize>,
        // pub timestamp: Option<String>,

        // pub aux: HashSet<(usize, String)>,

        // Main label
        item.labels_mut()
            .push(LocaleString::new("en", entry.ext_name.trim()));

        // Main description
        if !entry.ext_desc.trim().is_empty() {
            item.descriptions_mut()
                .push(LocaleString::new("en", entry.ext_desc.trim()));
        }

        // Catalog item
        item.claims_mut().push(Statement::new_normal(
            Snak::new_item(format!("P{PROP_CATALOG}"), catalog_item.to_string()),
            vec![],
            vec![],
        ));

        // Old MnM catalog
        item.claims_mut().push(Statement::new_normal(
            Snak::new_external_id(
                format!("P{PROP_OLD_MIXNMATCH_CATALOG_ID}"),
                format!("P{}", entry.catalog),
            ),
            vec![],
            vec![],
        ));

        // ext_id
        item.claims_mut().push(Statement::new_normal(
            Snak::new_string(format!("P{PROP_EXTERNAL_ID}"), entry.ext_id.to_string()),
            vec![],
            vec![],
        ));

        // id
        if let Some(id) = entry.id {
            item.claims_mut().push(Statement::new_normal(
                Snak::new_string(format!("P{PROP_OLD_MIXNMATCH_ENTITY_ID}"), format!("{id}")),
                vec![],
                vec![],
            ));
        }

        // ext_url
        if !entry.ext_url.trim().is_empty() {
            item.claims_mut().push(Statement::new_normal(
                Snak::new_string(
                    format!("P{PROP_EXTERNAL_URL}"),
                    entry.ext_url.trim().to_string(),
                ),
                vec![],
                vec![],
            ));
        }

        if let Some(entry_type) = &entry.type_name {
            item.claims_mut().push(Statement::new_normal(
                Snak::new_external_id(format!("P{PROP_ENTRY_TYPE}"), entry_type.to_string()),
                vec![],
                vec![],
            ));
        }

        // aliases
        for alias in &ext_entry.aliases {
            let trimmed = alias.value().trim();
            if !trimmed.is_empty() {
                item.aliases_mut()
                    .push(LocaleString::new(alias.language(), trimmed));
            }
        }

        // descriptions
        for (language, description) in &ext_entry.descriptions {
            let trimmed = description.trim();
            if !trimmed.is_empty() {
                item.aliases_mut()
                    .push(LocaleString::new(language.as_str(), trimmed));
            }
        }

        // born
        if let Some((value, precision)) = Self::date2claim(&ext_entry.born) {
            item.claims_mut().push(Statement::new_normal(
                Snak::new_time(format!("P{PROP_BORN}"), value, precision),
                vec![],
                vec![],
            ));
        }

        // died
        if let Some((value, precision)) = Self::date2claim(&ext_entry.died) {
            item.claims_mut().push(Statement::new_normal(
                Snak::new_time(format!("P{PROP_DIED}"), value, precision),
                vec![],
                vec![],
            ));
        }

        // location / coordinates
        if let Some(location) = &ext_entry.location {
            item.claims_mut().push(Statement::new_normal(
                Snak::new_coordinate(
                    format!("P{PROP_COORDINATES}"),
                    location.lat(),
                    location.lon(),
                ),
                vec![],
                vec![],
            ));
        }

        if let Some(user_id) = entry.user {
            if let Some(user) = WikiBaseUser::new_from_id(user_id, app).await {
                if let Some(snak) = user.get_snak() {
                    // item.add_claim(Statement::new_normal(snak, vec![], vec![]));
                }
            }
        }
        // if let Some(snak) = self.get_user_snak(catalog.owner(), app).await {
        //     item.add_claim(Statement::new_normal(snak, vec![], vec![]));
        // }

        todo!()
    }

    fn date2claim(date: &Option<String>) -> Option<(String, u64)> {
        let date = date.to_owned()?;
        if date.trim().is_empty() {
            return None;
        }
        const ALLOWED_CHARS: &str = "0123456789-";
        if !date.chars().all(|c| ALLOWED_CHARS.contains(c)) {
            return None;
        }
        let parts = date.split('-').collect::<Vec<&str>>();
        if parts.len() == 1 && !parts[0].trim().is_empty() {
            let value = format!("+{}-01-01T00:00:00Z", parts[0]);
            let precision = 9; // Year precision
            Some((value, precision))
        } else if parts.len() == 2 {
            let value = format!("+{}-{}-01T00:00:00Z", parts[0], parts[1]);
            let precision = 10; // Month precision
            Some((value, precision))
        } else if parts.len() == 3 {
            let value = format!("+{}-{}-{}T00:00:00Z", parts[0], parts[1], parts[2]);
            let precision = 11; // Day precision
            Some((value, precision))
        } else {
            None
        }
    }

    // Returns the equivalent Wikibase item for the catalog, or tries to create one.
    pub async fn get_or_create_catalog(
        &mut self,
        app: &AppState,
        catalog_id: usize,
    ) -> Result<String> {
        if let Some(catalog_item) = self.get_catalog_item_from_old_id(catalog_id).await {
            // println!("Already as {catalog_item}");
            return Ok(catalog_item);
        }
        let catalog = Catalog::from_id(catalog_id, app).await?;
        let item = self
            .generate_catalog_item(app, &catalog)
            .await
            .ok_or(anyhow!("Could not create item for catalog"))?;
        let token = self.api.get_token("csrf").await?;
        let data = serde_json::to_string(&item.to_json())?;

        let params = self.api.params_into(&[
            ("action", "wbeditentity"),
            ("new", "item"),
            ("summary", "creating new item for catalog"),
            ("token", &token),
            ("data", data.as_str()),
        ]);

        let result = self.api.post_query_api_json_mut(&params).await?;

        let ret = result["entity"]["id"]
            .as_str()
            .ok_or(anyhow!("Missing entity ID"))?
            .to_string();
        Ok(ret)
    }

    pub async fn get_catalog_item_from_old_id(&self, catalog_id: usize) -> Option<String> {
        let query = format!("haswbstatement:P{PROP_OLD_MIXNMATCH_CATALOG_ID}={catalog_id}");
        let params = self.api.params_into(&[
            ("action", "query"),
            ("list", "search"),
            ("srnamespace", WIKIBASE_ITEM_NAMESPACE),
            ("srsearch", query.as_str()),
        ]);
        let result = self
            .api
            .get_query_api_json_limit(&params, Some(20))
            .await
            .ok()?;
        let pages = result["query"]["search"].as_array()?;
        if pages.len() != 1 {
            return None;
        }
        if let Some(title) = pages[0]["title"].as_str() {
            if let Some((_before, after)) = title.split_once(':') {
                return Some(after.to_string());
            }
        }
        None
    }

    pub async fn generate_catalog_item(
        &self,
        app: &AppState,
        catalog: &Catalog,
    ) -> Option<ItemEntity> {
        let mut item = ItemEntity::new_missing();
        if let Some(name) = catalog.name() {
            item.labels_mut().push(LocaleString::new("en", name));
        }
        if let Some(catalog_id) = catalog.id() {
            item.add_claim(Statement::new_normal(
                Snak::new_external_id(
                    format!("P{PROP_OLD_MIXNMATCH_CATALOG_ID}"),
                    format!("{catalog_id}"),
                ),
                vec![],
                vec![],
            ));
        }
        item.add_claim(Statement::new_normal(
            Snak::new_item(format!("P{PROP_INSTANCE_OF}"), format!("Q{ITEM_CATALOG}")),
            vec![],
            vec![],
        ));
        if let Some(url) = catalog.url() {
            item.add_claim(Statement::new_normal(
                Snak::new_external_id(format!("P{PROP_EXTERNAL_URL}"), url.to_string()),
                vec![],
                vec![],
            ));
        }
        item.descriptions_mut()
            .push(LocaleString::new("en", catalog.desc()));
        if let Some(ct) = CatalogType::from_str(catalog.type_name()) {
            item.add_claim(Statement::new_normal(
                Snak::new_item(format!("P{PROP_CATALOG_TYPE}"), format!("Q{}", ct as usize)),
                vec![],
                vec![],
            ));
        }
        if let (Some(prop), None) = (catalog.wd_prop(), catalog.wd_qual()) {
            item.add_claim(Statement::new_normal(
                Snak::new_external_id(format!("P{PROP_WIKIDATA_PROPERTY}"), format!("P{}", prop)),
                vec![],
                vec![],
            ));
        }
        if !catalog.search_wp().trim().is_empty() {
            item.add_claim(Statement::new_normal(
                Snak::new_string(
                    format!("P{PROP_WIKIPEDIA_LANGUAGE_CODE}"),
                    catalog.search_wp().trim().to_lowercase(),
                ),
                vec![],
                vec![],
            ));
        }

        let active = if catalog.is_active() {
            ITEM_ACTIVE
        } else {
            ITEM_INACTIVE
        };
        item.add_claim(Statement::new_normal(
            Snak::new_item(format!("P{PROP_STATUS}"), format!("Q{}", active)),
            vec![],
            vec![],
        ));

        if let Some(user) = WikiBaseUser::new_from_id(catalog.owner(), app).await {
            if let Some(snak) = user.get_snak() {
                item.add_claim(Statement::new_normal(snak, vec![], vec![]));
            }
        }

        if let Some(source_item) = catalog.source_item() {
            item.add_claim(Statement::new_normal(
                Snak::new_external_id(
                    format!("P{PROP_WIKIDATA_SOURCE_ITEM}"),
                    format!("Q{}", source_item),
                ),
                vec![],
                vec![],
            ));
        }

        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date2claim_day() {
        let date = "234-10-24";
        let (value, precision) = WikiBase::date2claim(&Some(date.to_string())).unwrap();
        assert_eq!(value, "+234-10-24T00:00:00Z");
        assert_eq!(precision, 11);
    }

    #[test]
    fn test_date2claim_month() {
        let date = "1234-10";
        let (value, precision) = WikiBase::date2claim(&Some(date.to_string())).unwrap();
        assert_eq!(value, "+1234-10-01T00:00:00Z");
        assert_eq!(precision, 10);
    }

    #[test]
    fn test_date2claim_year() {
        let date = "234";
        let (value, precision) = WikiBase::date2claim(&Some(date.to_string())).unwrap();
        assert_eq!(value, "+234-01-01T00:00:00Z");
        assert_eq!(precision, 9);
    }

    #[test]
    fn test_date2claim_none() {
        assert_eq!(WikiBase::date2claim(&None), None);
    }

    #[test]
    fn test_date2claim_bad_date() {
        assert_eq!(WikiBase::date2claim(&Some("foobar".to_string())), None);
    }
}
// date2claim

/*
TEST CATALOGS:
small:
- https://mix-n-match.toolforge.org/#/catalog/2974 "African American Women Writers of the 19th Century" 42

locations and image patterns:
- https://mix-n-match.toolforge.org/#/catalog/5516 "Scottish Buildings at Risk" 501
- https://mix-n-match.toolforge.org/#/catalog/106 "skyscrapercenter" 31K

aliases:
- https://mix-n-match.toolforge.org/#/catalog/6792 "Modern History Database person" 149K

descriptions, locations:
- https://mix-n-match.toolforge.org/#/catalog/4048 "Finnish lakes" 55K

aux,dates:
- https://mix-n-match.toolforge.org/#/catalog/54 "Kaiserhof" 5K (entries with P214,P227, dates)
- https://mix-n-match.toolforge.org/#/catalog/73 "HLS" 36K (entries with P214,P227, day-level dates)

kv_entry image_url:
- https://mix-n-match.toolforge.org/#/catalog/760 "Philadelphia Museum of Art artwork" 75K

mnm relations:
- https://mix-n-match.toolforge.org/#/catalog/82 "MOMA" 123K LINKS TO:
- https://mix-n-match.toolforge.org/#/catalog/101 "MOMO artists" 26K

kv_catalog:
LATER
 */
