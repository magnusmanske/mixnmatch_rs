use anyhow::{Result, anyhow};
use mlua::{Lua, LuaOptions, StdLib, Value, VmState};
use serde::{Deserialize, Serialize};

/// Memory limit for Lua VM (1 MB)
const LUA_MEMORY_LIMIT: usize = 1_048_576;

/// Instruction limit for Lua VM (100,000 instructions)
const LUA_INSTRUCTION_LIMIT: u32 = 100_000;

/// Convert an mlua::Error to anyhow::Error (since mlua::Error is not Send+Sync).
fn lua_err(e: mlua::Error) -> anyhow::Error {
    anyhow!("{e}")
}

/// Represents a code fragment row from the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeFragment {
    pub id: usize,
    pub function: String,
    pub catalog: usize,
    pub php: String,
    pub json: String,
    pub is_active: bool,
    pub note: Option<String>,
    pub lua: Option<String>,
}

/// The type of function a code fragment implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeFragmentFunction {
    PersonDate,
    DescFromHtml,
    AuxFromDesc,
    CoordsFromHtml,
    BespokeScraper,
}

impl CodeFragmentFunction {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "PERSON_DATE" => Some(Self::PersonDate),
            "DESC_FROM_HTML" => Some(Self::DescFromHtml),
            "AUX_FROM_DESC" => Some(Self::AuxFromDesc),
            "COORDS_FROM_HTML" => Some(Self::CoordsFromHtml),
            "BESPOKE_SCRAPER" => Some(Self::BespokeScraper),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PersonDate => "PERSON_DATE",
            Self::DescFromHtml => "DESC_FROM_HTML",
            Self::AuxFromDesc => "AUX_FROM_DESC",
            Self::CoordsFromHtml => "COORDS_FROM_HTML",
            Self::BespokeScraper => "BESPOKE_SCRAPER",
        }
    }
}

/// An entry object exposed to Lua code, mirroring the PHP `$o` variable.
#[derive(Debug, Clone, Default)]
pub struct LuaEntry {
    pub id: usize,
    pub catalog: usize,
    pub ext_id: String,
    pub ext_url: String,
    pub ext_name: String,
    pub ext_desc: String,
    pub q: Option<isize>,
    pub user: Option<usize>,
    pub type_name: Option<String>,
}

/// Commands that Lua code can produce (equivalent to PHP Command class).
#[derive(Debug, Clone, PartialEq)]
pub enum LuaCommand {
    SetPersonDates {
        entry_id: usize,
        born: String,
        died: String,
    },
    SetAux {
        entry_id: usize,
        property: String,
        value: String,
    },
    SetMatch {
        entry_id: usize,
        q: String,
    },
    SetLocation {
        entry_id: usize,
        lat: f64,
        lon: f64,
    },
    SetDescription {
        entry_id: usize,
        value: String,
    },
    SetEntryName {
        entry_id: usize,
        value: String,
    },
    SetEntryType {
        entry_id: usize,
        value: String,
    },
    AddAlias {
        entry_id: usize,
        label: String,
        language: String,
    },
    AddLocationText {
        entry_id: usize,
        property: usize,
        value: String,
    },
}

/// Result from running a PERSON_DATE code fragment.
#[derive(Debug, Clone, Default)]
pub struct PersonDateResult {
    pub born: String,
    pub died: String,
}

/// Result from running a DESC_FROM_HTML code fragment.
#[derive(Debug, Clone, Default)]
pub struct DescFromHtmlResult {
    pub descriptions: Vec<String>,
    pub born: String,
    pub died: String,
    pub change_type: Option<(String, String)>,
    pub change_name: Option<(String, String)>,
    pub location: Option<(f64, f64)>,
    pub aux: Vec<(String, String)>,
    pub location_texts: Vec<(usize, String)>,
    pub commands: Vec<LuaCommand>,
}

/// Result from running an AUX_FROM_DESC code fragment.
#[derive(Debug, Clone, Default)]
pub struct AuxFromDescResult {
    pub commands: Vec<LuaCommand>,
}

/// Creates a new sandboxed Lua VM with memory and instruction limits.
fn create_lua() -> Result<Lua> {
    // Only load safe standard libraries (no os, io, debug, package, ffi)
    let libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8;
    let lua = Lua::new_with(libs, LuaOptions::default()).map_err(lua_err)?;
    let _ = lua.set_memory_limit(LUA_MEMORY_LIMIT);
    Ok(lua)
}

/// Sets up an instruction-count hook that aborts execution after the limit.
fn set_instruction_limit(lua: &Lua) {
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(1000),
        {
            let count = std::cell::Cell::new(0u32);
            move |_lua, _debug| {
                let new_count = count.get() + 1000;
                count.set(new_count);
                if new_count > LUA_INSTRUCTION_LIMIT {
                    Err(mlua::Error::RuntimeError(
                        "instruction limit exceeded".into(),
                    ))
                } else {
                    Ok(VmState::Continue)
                }
            }
        },
    );
}

/// Sets up the entry object (`o`) as a Lua global table.
fn set_entry_global(lua: &Lua, entry: &LuaEntry) -> Result<()> {
    let o = lua.create_table().map_err(lua_err)?;
    o.set("id", entry.id).map_err(lua_err)?;
    o.set("catalog", entry.catalog).map_err(lua_err)?;
    o.set("ext_id", entry.ext_id.as_str()).map_err(lua_err)?;
    o.set("ext_url", entry.ext_url.as_str()).map_err(lua_err)?;
    o.set("ext_name", entry.ext_name.as_str()).map_err(lua_err)?;
    o.set("ext_desc", entry.ext_desc.as_str()).map_err(lua_err)?;
    match entry.q {
        Some(q) => o.set("q", q).map_err(lua_err)?,
        None => o.set("q", Value::Nil).map_err(lua_err)?,
    }
    match entry.user {
        Some(u) => o.set("user", u).map_err(lua_err)?,
        None => o.set("user", Value::Nil).map_err(lua_err)?,
    }
    match &entry.type_name {
        Some(t) => o.set("type", t.as_str()).map_err(lua_err)?,
        None => o.set("type", Value::Nil).map_err(lua_err)?,
    }
    lua.globals().set("o", o).map_err(lua_err)?;
    Ok(())
}

/// Register helper functions available in code fragments.
fn register_date_helpers(lua: &Lua) -> Result<()> {
    // dp(date_string) -> parsed date string (equivalent to PHP parse_date)
    let dp = lua
        .create_function(|_, s: String| Ok(parse_date(&s)))
        .map_err(lua_err)?;
    lua.globals().set("dp", dp).map_err(lua_err)?;

    // ml(month_name) -> three-letter month abbreviation
    let ml = lua
        .create_function(|_, s: String| Ok(try_get_three_letter_month(&s)))
        .map_err(lua_err)?;
    lua.globals().set("ml", ml).map_err(lua_err)?;

    // clean_html(html) -> cleaned text
    let clean_html_fn = lua
        .create_function(|_, s: String| Ok(clean_html(&s)))
        .map_err(lua_err)?;
    lua.globals().set("clean_html", clean_html_fn).map_err(lua_err)?;

    Ok(())
}

/// Register command callback functions (setAux, setMatch, setLocation, etc.)
fn register_command_functions(lua: &Lua) -> Result<()> {
    let commands_table = lua.create_table().map_err(lua_err)?;
    lua.globals().set("_commands", commands_table).map_err(lua_err)?;
    lua.globals().set("_cmd_idx", 0i64).map_err(lua_err)?;

    // setAux(entry_id, property, value)
    let set_aux = lua
        .create_function(|lua_inner, (entry_id, property, value): (usize, mlua::Value, String)| {
            let cmds: mlua::Table = lua_inner.globals().get("_commands")?;
            let idx: i64 = lua_inner.globals().get("_cmd_idx")?;
            let new_idx = idx + 1;
            let cmd = lua_inner.create_table()?;
            cmd.set("type", "set_aux")?;
            cmd.set("entry_id", entry_id)?;
            // Property can be number or string (e.g. 214 or "P345")
            let prop_str = match property {
                mlua::Value::Integer(n) => n.to_string(),
                mlua::Value::String(s) => s.to_string_lossy().to_string(),
                mlua::Value::Number(n) => (n as i64).to_string(),
                _ => "0".to_string(),
            };
            cmd.set("property", prop_str)?;
            cmd.set("value", value)?;
            cmds.set(new_idx, cmd)?;
            lua_inner.globals().set("_cmd_idx", new_idx)?;
            Ok(())
        })
        .map_err(lua_err)?;
    lua.globals().set("setAux", set_aux).map_err(lua_err)?;

    // setMatch(entry_id, q)
    let set_match = lua
        .create_function(|lua_inner, (entry_id, q): (usize, String)| {
            let cmds: mlua::Table = lua_inner.globals().get("_commands")?;
            let idx: i64 = lua_inner.globals().get("_cmd_idx")?;
            let new_idx = idx + 1;
            let cmd = lua_inner.create_table()?;
            cmd.set("type", "set_match")?;
            cmd.set("entry_id", entry_id)?;
            cmd.set("q", q)?;
            cmds.set(new_idx, cmd)?;
            lua_inner.globals().set("_cmd_idx", new_idx)?;
            Ok(())
        })
        .map_err(lua_err)?;
    lua.globals().set("setMatch", set_match).map_err(lua_err)?;

    // setLocation(entry_id, lat, lon)
    let set_location = lua
        .create_function(|lua_inner, (entry_id, lat, lon): (usize, f64, f64)| {
            let cmds: mlua::Table = lua_inner.globals().get("_commands")?;
            let idx: i64 = lua_inner.globals().get("_cmd_idx")?;
            let new_idx = idx + 1;
            let cmd = lua_inner.create_table()?;
            cmd.set("type", "set_location")?;
            cmd.set("entry_id", entry_id)?;
            cmd.set("lat", lat)?;
            cmd.set("lon", lon)?;
            cmds.set(new_idx, cmd)?;
            lua_inner.globals().set("_cmd_idx", new_idx)?;
            Ok(())
        })
        .map_err(lua_err)?;
    lua.globals().set("setLocation", set_location).map_err(lua_err)?;

    // setPersonDates(entry_id, born, died)
    let set_person_dates = lua
        .create_function(|lua_inner, (entry_id, born, died): (usize, String, String)| {
            let cmds: mlua::Table = lua_inner.globals().get("_commands")?;
            let idx: i64 = lua_inner.globals().get("_cmd_idx")?;
            let new_idx = idx + 1;
            let cmd = lua_inner.create_table()?;
            cmd.set("type", "set_person_dates")?;
            cmd.set("entry_id", entry_id)?;
            cmd.set("born", born)?;
            cmd.set("died", died)?;
            cmds.set(new_idx, cmd)?;
            lua_inner.globals().set("_cmd_idx", new_idx)?;
            Ok(())
        })
        .map_err(lua_err)?;
    lua.globals().set("setPersonDates", set_person_dates).map_err(lua_err)?;

    // setAlias(entry_id, label, language)
    let set_alias = lua
        .create_function(
            |lua_inner, (entry_id, label, language): (usize, String, Option<String>)| {
                let cmds: mlua::Table = lua_inner.globals().get("_commands")?;
                let idx: i64 = lua_inner.globals().get("_cmd_idx")?;
                let new_idx = idx + 1;
                let cmd = lua_inner.create_table()?;
                cmd.set("type", "add_alias")?;
                cmd.set("entry_id", entry_id)?;
                cmd.set("label", label)?;
                cmd.set("language", language.unwrap_or_default())?;
                cmds.set(new_idx, cmd)?;
                lua_inner.globals().set("_cmd_idx", new_idx)?;
                Ok(())
            },
        )
        .map_err(lua_err)?;
    lua.globals().set("setAlias", set_alias).map_err(lua_err)?;

    Ok(())
}

/// Collect commands from the Lua _commands table.
fn collect_commands(lua: &Lua) -> Result<Vec<LuaCommand>> {
    let cmds: mlua::Table = lua.globals().get("_commands").map_err(lua_err)?;
    let mut result = Vec::new();
    for pair in cmds.pairs::<i64, mlua::Table>() {
        if let Ok((_, cmd_table)) = pair {
            if let Ok(cmd) = table_to_command(&cmd_table) {
                result.push(cmd);
            }
        }
    }
    Ok(result)
}

/// Run a PERSON_DATE Lua code fragment.
pub fn run_person_date(lua_code: &str, entry: &LuaEntry) -> Result<PersonDateResult> {
    let lua = create_lua()?;
    set_instruction_limit(&lua);
    set_entry_global(&lua, entry)?;
    register_date_helpers(&lua)?;

    // Set up the born/died variables
    lua.globals().set("born", "").map_err(lua_err)?;
    lua.globals().set("died", "").map_err(lua_err)?;

    lua.load(lua_code).exec().map_err(lua_err)?;

    let born: String = lua.globals().get("born").map_err(lua_err)?;
    let died: String = lua.globals().get("died").map_err(lua_err)?;

    Ok(PersonDateResult { born, died })
}

/// Run a DESC_FROM_HTML Lua code fragment.
pub fn run_desc_from_html(
    lua_code: &str,
    entry: &LuaEntry,
    html: &str,
) -> Result<DescFromHtmlResult> {
    let lua = create_lua()?;
    set_instruction_limit(&lua);
    set_entry_global(&lua, entry)?;
    register_date_helpers(&lua)?;
    register_command_functions(&lua)?;

    // Set up all the variables the code fragment can write to
    lua.globals().set("html", html).map_err(lua_err)?;
    lua.globals().set("born", "").map_err(lua_err)?;
    lua.globals().set("died", "").map_err(lua_err)?;
    lua.globals().set("d", lua.create_table().map_err(lua_err)?).map_err(lua_err)?;
    lua.globals().set("change_type", lua.create_table().map_err(lua_err)?).map_err(lua_err)?;
    lua.globals().set("change_name", lua.create_table().map_err(lua_err)?).map_err(lua_err)?;
    lua.globals().set("location", lua.create_table().map_err(lua_err)?).map_err(lua_err)?;
    lua.globals().set("aux", lua.create_table().map_err(lua_err)?).map_err(lua_err)?;
    lua.globals().set("location_texts", lua.create_table().map_err(lua_err)?).map_err(lua_err)?;

    lua.load(lua_code).exec().map_err(lua_err)?;

    // Collect results
    let mut result = DescFromHtmlResult::default();

    result.born = lua.globals().get::<String>("born").unwrap_or_default();
    result.died = lua.globals().get::<String>("died").unwrap_or_default();

    // Read d[] (descriptions)
    if let Ok(d) = lua.globals().get::<mlua::Table>("d") {
        for pair in d.pairs::<i64, String>() {
            if let Ok((_, v)) = pair {
                result.descriptions.push(v);
            }
        }
    }

    // Read change_type
    if let Ok(ct) = lua.globals().get::<mlua::Table>("change_type") {
        let ct1: Option<String> = ct.get(1).ok();
        let ct2: Option<String> = ct.get(2).ok();
        if let (Some(from), Some(to)) = (ct1, ct2) {
            result.change_type = Some((from, to));
        }
    }

    // Read change_name
    if let Ok(cn) = lua.globals().get::<mlua::Table>("change_name") {
        let cn1: Option<String> = cn.get(1).ok();
        let cn2: Option<String> = cn.get(2).ok();
        if let (Some(from), Some(to)) = (cn1, cn2) {
            result.change_name = Some((from, to));
        }
    }

    // Read location
    if let Ok(loc) = lua.globals().get::<mlua::Table>("location") {
        let lat: Option<f64> = loc.get(1).ok();
        let lon: Option<f64> = loc.get(2).ok();
        if let (Some(lat), Some(lon)) = (lat, lon) {
            result.location = Some((lat, lon));
        }
    }

    // Read aux
    if let Ok(aux_table) = lua.globals().get::<mlua::Table>("aux") {
        for pair in aux_table.pairs::<i64, mlua::Table>() {
            if let Ok((_, t)) = pair {
                let prop: String = match t.get::<Value>(1) {
                    Ok(Value::Integer(n)) => n.to_string(),
                    Ok(Value::String(s)) => s.to_string_lossy().to_string(),
                    Ok(Value::Number(n)) => (n as i64).to_string(),
                    _ => continue,
                };
                let val: String = t.get(2).unwrap_or_default();
                result.aux.push((prop, val));
            }
        }
    }

    // Read location_texts
    if let Ok(lt_table) = lua.globals().get::<mlua::Table>("location_texts") {
        for pair in lt_table.pairs::<i64, mlua::Table>() {
            if let Ok((_, t)) = pair {
                let prop: usize = t.get(1).unwrap_or_default();
                let val: String = t.get(2).unwrap_or_default();
                result.location_texts.push((prop, val));
            }
        }
    }

    // Read commands from callback functions
    result.commands = collect_commands(&lua)?;

    Ok(result)
}

/// Run an AUX_FROM_DESC Lua code fragment.
pub fn run_aux_from_desc(lua_code: &str, entry: &LuaEntry) -> Result<AuxFromDescResult> {
    let lua = create_lua()?;
    set_instruction_limit(&lua);
    set_entry_global(&lua, entry)?;
    register_date_helpers(&lua)?;
    register_command_functions(&lua)?;

    lua.load(lua_code).exec().map_err(lua_err)?;

    let commands = collect_commands(&lua)?;
    Ok(AuxFromDescResult { commands })
}

/// Convert a Lua table representing a command into a LuaCommand.
fn table_to_command(t: &mlua::Table) -> Result<LuaCommand> {
    let cmd_type: String = t.get("type").map_err(lua_err)?;
    match cmd_type.as_str() {
        "set_aux" => Ok(LuaCommand::SetAux {
            entry_id: t.get("entry_id").map_err(lua_err)?,
            property: t.get("property").map_err(lua_err)?,
            value: t.get("value").map_err(lua_err)?,
        }),
        "set_match" => Ok(LuaCommand::SetMatch {
            entry_id: t.get("entry_id").map_err(lua_err)?,
            q: t.get("q").map_err(lua_err)?,
        }),
        "set_location" => Ok(LuaCommand::SetLocation {
            entry_id: t.get("entry_id").map_err(lua_err)?,
            lat: t.get("lat").map_err(lua_err)?,
            lon: t.get("lon").map_err(lua_err)?,
        }),
        "set_person_dates" => Ok(LuaCommand::SetPersonDates {
            entry_id: t.get("entry_id").map_err(lua_err)?,
            born: t.get("born").map_err(lua_err)?,
            died: t.get("died").map_err(lua_err)?,
        }),
        "add_alias" => Ok(LuaCommand::AddAlias {
            entry_id: t.get("entry_id").map_err(lua_err)?,
            label: t.get("label").map_err(lua_err)?,
            language: t.get("language").map_err(lua_err)?,
        }),
        _ => Err(anyhow!("Unknown command type: {cmd_type}")),
    }
}

// =============================
// Helper functions exposed to Lua
// =============================

/// Equivalent to PHP's `parse_date()` / `dp()`.
/// Tries to turn a (reasonable) date string into proper ISO date format.
pub fn parse_date(d: &str) -> String {
    let d = d.trim();
    if d.is_empty() {
        return String::new();
    }

    // Pure year (1-4 digits)
    if let Some(caps) = regex_match(r"^\s*(\d{1,4})\s*$", d) {
        return format!("{:0>4}", &caps[1]);
    }

    // DD.MM.YYYY format
    if let Some(caps) = regex_match(r"^\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{3,4})\s*$", d) {
        let year = format!("{:0>4}", &caps[3]);
        let month = format!("{:0>2}", &caps[2]);
        let day = format!("{:0>2}", &caps[1]);
        return format!("{year}-{month}-{day}");
    }

    // MM.YYYY format
    if let Some(caps) = regex_match(r"^\s*(\d{1,2})\.\s*(\d{3,4})\s*$", d) {
        let year = format!("{:0>4}", &caps[2]);
        let month = format!("{:0>2}", &caps[1]);
        return format!("{year}-{month}");
    }

    // Try parsing with natural date parsing
    if let Some(parsed) = try_parse_natural_date(d) {
        return parsed;
    }

    // Fallback: return trimmed input
    d.to_string()
}

/// Try to parse a natural-language date string like "12 jan 2000" or "jan 12, 2000".
fn try_parse_natural_date(d: &str) -> Option<String> {
    let d_lower = d.to_lowercase();
    let d_clean = d_lower.replace(',', " ").replace('.', " ");
    let parts: Vec<&str> = d_clean.split_whitespace().collect();

    if parts.len() < 2 {
        return None;
    }

    // Try "DD month YYYY" or "month DD YYYY"
    if parts.len() >= 3 {
        // "DD month YYYY"
        if let (Some(day), Some(month_num), Some(year)) = (
            parts[0]
                .parse::<u32>()
                .ok()
                .filter(|&d| (1..=31).contains(&d)),
            month_name_to_number(parts[1]),
            parts[2].parse::<i32>().ok(),
        ) {
            return Some(format!("{:0>4}-{:02}-{:02}", year, month_num, day));
        }
        // "month DD YYYY"
        if let (Some(month_num), Some(day), Some(year)) = (
            month_name_to_number(parts[0]),
            parts[1]
                .parse::<u32>()
                .ok()
                .filter(|&d| (1..=31).contains(&d)),
            parts[2].parse::<i32>().ok(),
        ) {
            return Some(format!("{:0>4}-{:02}-{:02}", year, month_num, day));
        }
    }

    // "month YYYY"
    if parts.len() == 2 {
        if let (Some(month_num), Some(year)) =
            (month_name_to_number(parts[0]), parts[1].parse::<i32>().ok())
        {
            return Some(format!("{:0>4}-{:02}", year, month_num));
        }
    }

    None
}

/// Convert a month name (in various languages) to a month number 1-12.
fn month_name_to_number(name: &str) -> Option<u32> {
    let s = name.to_lowercase();
    if s.len() < 3 {
        return None;
    }
    // Match on first 3 characters for standard month abbreviations
    let prefix = &s[..s.len().min(3)];
    match prefix {
        "jan" => Some(1),
        "feb" => Some(2),
        "mar" => Some(3),
        "apr" | "avr" => Some(4),
        "may" | "mai" | "mei" => Some(5),
        "jun" => Some(6),
        "jul" => Some(7),
        "aug" => Some(8),
        "sep" => Some(9),
        "oct" | "okt" => Some(10),
        "nov" => Some(11),
        "dec" | "dez" | "dic" => Some(12),
        _ => {
            // Full-word matches for other languages
            match s.as_str() {
                "enero" | "janvier" | "gennaio" | "januari" | "janeiro" => Some(1),
                "febrero" | "février" | "fevrier" | "febbraio" | "februari" | "fevereiro" => {
                    Some(2)
                }
                "marzo" | "mars" | "märz" | "maerz" | "maart" | "março" | "marco" => Some(3),
                "abril" | "avril" | "aprile" => Some(4),
                "mayo" | "maggio" | "maio" | "mag" => Some(5),
                "junio" | "juin" | "giugno" | "juni" | "junho" => Some(6),
                "julio" | "juillet" | "luglio" | "juli" | "julho" => Some(7),
                "agosto" | "août" | "aout" | "augustus" => Some(8),
                "septiembre" | "septembre" | "settembre" | "september" | "setembro" => Some(9),
                "octubre" | "octobre" | "ottobre" | "oktober" | "outubro" => Some(10),
                "noviembre" | "novembre" | "november" | "novembro" => Some(11),
                "diciembre" | "décembre" | "decembre" | "dicembre" | "dezember" | "dezembro" => {
                    Some(12)
                }
                _ => None,
            }
        }
    }
}

/// Equivalent to PHP's `try_get_three_letter_month()` / `ml()`.
/// Tries to convert a month name into a three-letter month code.
pub fn try_get_three_letter_month(month: &str) -> String {
    let month = month.trim().to_lowercase();
    if let Some(num) = month_name_to_number(&month) {
        match num {
            1 => "jan",
            2 => "feb",
            3 => "mar",
            4 => "apr",
            5 => "may",
            6 => "jun",
            7 => "jul",
            8 => "aug",
            9 => "sep",
            10 => "oct",
            11 => "nov",
            12 => "dec",
            _ => return month,
        }
        .to_string()
    } else {
        month
    }
}

/// Equivalent to PHP's `clean_html()`.
/// Removes HTML tags, collapses whitespace, decodes entities.
pub fn clean_html(html: &str) -> String {
    // Replace &nbsp; with space
    let s = html.replace("&nbsp;", " ");
    // Remove HTML tags
    let s = regex::Regex::new(r"<.+?>")
        .unwrap()
        .replace_all(&s, " ");
    // Collapse whitespace
    let s = regex::Regex::new(r"\s+").unwrap().replace_all(&s, " ");
    // Decode HTML entities
    let s = html_escape::decode_html_entities(&s);
    s.trim().to_string()
}

/// Simple regex match helper, returns captures.
fn regex_match<'a>(pattern: &str, text: &'a str) -> Option<regex::Captures<'a>> {
    regex::Regex::new(pattern).ok()?.captures(text)
}

// ==========
// Tests
// ==========

#[cfg(test)]
mod tests {
    use super::*;

    fn test_entry() -> LuaEntry {
        LuaEntry {
            id: 42,
            catalog: 100,
            ext_id: "test_123".into(),
            ext_url: "https://example.com/test".into(),
            ext_name: "John Doe".into(),
            ext_desc: "1920-2000".into(),
            q: None,
            user: None,
            type_name: Some("Q5".into()),
        }
    }

    // ---- parse_date tests ----

    #[test]
    fn test_parse_date_year_only() {
        assert_eq!(parse_date("1920"), "1920");
        assert_eq!(parse_date("800"), "0800");
        assert_eq!(parse_date("50"), "0050");
        assert_eq!(parse_date("  1999  "), "1999");
    }

    #[test]
    fn test_parse_date_dd_mm_yyyy() {
        assert_eq!(parse_date("12.03.1920"), "1920-03-12");
        assert_eq!(parse_date("1.1.800"), "0800-01-01");
        assert_eq!(parse_date("  5. 6. 1990 "), "1990-06-05");
    }

    #[test]
    fn test_parse_date_mm_yyyy() {
        assert_eq!(parse_date("3.1920"), "1920-03");
        assert_eq!(parse_date("12. 1999"), "1999-12");
    }

    #[test]
    fn test_parse_date_natural() {
        assert_eq!(parse_date("12 jan 2000"), "2000-01-12");
        assert_eq!(parse_date("jan 12 2000"), "2000-01-12");
        assert_eq!(parse_date("15 mars 1800"), "1800-03-15");
        assert_eq!(parse_date("3 febrero 1950"), "1950-02-03");
    }

    #[test]
    fn test_parse_date_empty() {
        assert_eq!(parse_date(""), "");
        assert_eq!(parse_date("  "), "");
    }

    // ---- try_get_three_letter_month tests ----

    #[test]
    fn test_ml_basic() {
        assert_eq!(try_get_three_letter_month("January"), "jan");
        assert_eq!(try_get_three_letter_month("febrero"), "feb");
        assert_eq!(try_get_three_letter_month("dec"), "dec");
        assert_eq!(try_get_three_letter_month("unknown"), "unknown");
    }

    // ---- clean_html tests ----

    #[test]
    fn test_clean_html() {
        assert_eq!(clean_html("<b>hello</b> world"), "hello world");
        assert_eq!(clean_html("a&nbsp;b"), "a b");
        assert_eq!(clean_html("  <p>  text  </p>  "), "text");
        assert_eq!(clean_html("&amp; &lt; &gt;"), "& < >");
    }

    // ---- PERSON_DATE Lua execution tests ----

    #[test]
    fn test_person_date_simple_regex() {
        let lua = r#"
local m = string.match(o.ext_desc, "(%d%d%d%d)-(%d%d%d%d)")
if m then
    born = string.match(o.ext_desc, "(%d%d%d%d)-")
    died = string.match(o.ext_desc, "%-(%d%d%d%d)")
end
"#;
        let entry = test_entry();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "1920");
        assert_eq!(result.died, "2000");
    }

    #[test]
    fn test_person_date_no_match() {
        let lua = r#"
local m = string.match(o.ext_desc, "born (%d+)")
if m then born = m end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "no dates here".into();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "");
        assert_eq!(result.died, "");
    }

    #[test]
    fn test_person_date_with_dp() {
        let lua = r#"
local m = string.match(o.ext_desc, "born (.+)")
if m then born = dp(m) end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "born 12 jan 1950".into();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "1950-01-12");
    }

    #[test]
    fn test_person_date_with_ml_and_dp() {
        let lua = r#"
local day, month, year = string.match(o.ext_desc, "(%d+) (%a+) (%d+)")
if day and month and year then
    born = dp(day .. " " .. ml(month) .. " " .. year)
end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "15 febrero 1890".into();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "1890-02-15");
    }

    #[test]
    fn test_person_date_entry_fields() {
        let lua = r#"
if o.ext_name == "John Doe" then born = "1920" end
if o.type == "Q5" then died = "2000" end
"#;
        let entry = test_entry();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "1920");
        assert_eq!(result.died, "2000");
    }

    // ---- AUX_FROM_DESC tests ----

    #[test]
    fn test_aux_from_desc_set_aux() {
        let lua = r#"
local m = string.match(o.ext_desc, "VIAF: (%d+)")
if m then setAux(o.id, 214, m) end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "Some person; VIAF: 12345678".into();
        let result = run_aux_from_desc(lua, &entry).unwrap();
        assert_eq!(result.commands.len(), 1);
        assert_eq!(
            result.commands[0],
            LuaCommand::SetAux {
                entry_id: 42,
                property: "214".into(),
                value: "12345678".into(),
            }
        );
    }

    #[test]
    fn test_aux_from_desc_set_aux_string_property() {
        let lua = r#"
local m = string.match(o.ext_desc, "IMDB:(nm%d+)")
if m then setAux(o.id, "P345", m) end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "IMDB:nm12345".into();
        let result = run_aux_from_desc(lua, &entry).unwrap();
        assert_eq!(result.commands.len(), 1);
        assert_eq!(
            result.commands[0],
            LuaCommand::SetAux {
                entry_id: 42,
                property: "P345".into(),
                value: "nm12345".into(),
            }
        );
    }

    #[test]
    fn test_aux_from_desc_set_match() {
        let lua = r#"
local m = string.match(o.ext_desc, "(Q%d+)")
if m then setMatch(o.id, m) end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "Linked to Q42".into();
        let result = run_aux_from_desc(lua, &entry).unwrap();
        assert_eq!(result.commands.len(), 1);
        assert_eq!(
            result.commands[0],
            LuaCommand::SetMatch {
                entry_id: 42,
                q: "Q42".into(),
            }
        );
    }

    #[test]
    fn test_aux_from_desc_set_location() {
        let lua = r#"
local lat, lon = string.match(o.ext_desc, "%(([%d%.%-]+),([%d%.%-]+)%)")
if lat and lon then setLocation(o.id, tonumber(lat), tonumber(lon)) end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "Location: (52.5,13.4)".into();
        let result = run_aux_from_desc(lua, &entry).unwrap();
        assert_eq!(result.commands.len(), 1);
        match &result.commands[0] {
            LuaCommand::SetLocation {
                entry_id,
                lat,
                lon,
            } => {
                assert_eq!(*entry_id, 42);
                assert!((lat - 52.5).abs() < 0.001);
                assert!((lon - 13.4).abs() < 0.001);
            }
            _ => panic!("Expected SetLocation"),
        }
    }

    // ---- DESC_FROM_HTML tests ----

    #[test]
    fn test_desc_from_html_simple() {
        let lua = r#"
local m = string.match(html, "<h1>(.-)</h1>")
if m then d[#d+1] = m end
"#;
        let entry = test_entry();
        let html = "<html><h1>A great person</h1><p>Details...</p></html>";
        let result = run_desc_from_html(lua, &entry, html).unwrap();
        assert_eq!(result.descriptions, vec!["A great person"]);
    }

    #[test]
    fn test_desc_from_html_change_type() {
        let lua = r#"
change_type = {"", "Q5"}
"#;
        let entry = test_entry();
        let result = run_desc_from_html(lua, &entry, "").unwrap();
        assert_eq!(result.change_type, Some(("".into(), "Q5".into())));
    }

    #[test]
    fn test_desc_from_html_with_aux() {
        let lua = r#"
local m = string.match(html, "VIAF: (%d+)")
if m then
    aux[#aux+1] = {214, m}
end
"#;
        let entry = test_entry();
        let html = "VIAF: 99887766";
        let result = run_desc_from_html(lua, &entry, html).unwrap();
        assert_eq!(result.aux.len(), 1);
        assert_eq!(result.aux[0], ("214".into(), "99887766".into()));
    }

    #[test]
    fn test_desc_from_html_with_clean_html() {
        let lua = r#"
local m = string.match(html, "<div class=\"bio\">(.-)</div>")
if m then d[#d+1] = clean_html(m) end
"#;
        let entry = test_entry();
        let html = r#"<div class="bio"><b>Born</b>&nbsp;in <i>London</i></div>"#;
        let result = run_desc_from_html(lua, &entry, html).unwrap();
        assert_eq!(result.descriptions.len(), 1);
        assert_eq!(result.descriptions[0], "Born in London");
    }

    #[test]
    fn test_desc_from_html_born_died() {
        let lua = r#"
born = string.match(html, "Born: (%d%d%d%d)") or ""
died = string.match(html, "Died: (%d%d%d%d)") or ""
"#;
        let entry = test_entry();
        let html = "Born: 1850, Died: 1920";
        let result = run_desc_from_html(lua, &entry, html).unwrap();
        assert_eq!(result.born, "1850");
        assert_eq!(result.died, "1920");
    }

    // ---- Sandboxing tests ----

    #[test]
    fn test_instruction_limit() {
        let lua = r#"
while true do end
"#;
        let entry = test_entry();
        let result = run_person_date(lua, &entry);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("instruction limit") || err.contains("runtime error"),
            "Expected instruction limit error, got: {err}"
        );
    }

    #[test]
    fn test_no_os_library() {
        let lua = r#"
os.execute("echo pwned")
"#;
        let entry = test_entry();
        let result = run_person_date(lua, &entry);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_io_library() {
        let lua = r#"
io.open("/etc/passwd", "r")
"#;
        let entry = test_entry();
        let result = run_person_date(lua, &entry);
        assert!(result.is_err());
    }

    #[test]
    fn test_memory_limit() {
        let lua = r#"
local t = {}
for i = 1, 10000000 do
    t[i] = string.rep("x", 1000)
end
"#;
        let entry = test_entry();
        let result = run_person_date(lua, &entry);
        assert!(result.is_err(), "Expected memory limit error");
    }

    // ---- Integration-style tests ----

    #[test]
    fn test_person_date_multiple_patterns() {
        let lua = r#"
-- Try YYYY-YYYY format
local b, d_val = string.match(o.ext_desc, "(%d%d%d%d)%-(%d%d%d%d)")
if b then
    born = b
    died = d_val
    return
end
-- Try "b. YYYY" format
local b2 = string.match(o.ext_desc, "b%. (%d%d%d%d)")
if b2 then born = b2 end
-- Try "d. YYYY" format
local d2 = string.match(o.ext_desc, "d%. (%d%d%d%d)")
if d2 then died = d2 end
"#;
        let mut entry = test_entry();

        // Test YYYY-YYYY
        entry.ext_desc = "1850-1920".into();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "1850");
        assert_eq!(result.died, "1920");

        // Test b./d. format
        entry.ext_desc = "b. 1900; d. 1980".into();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "1900");
        assert_eq!(result.died, "1980");
    }

    #[test]
    fn test_aux_from_desc_multiple_commands() {
        let lua = r#"
for prop, val in string.gmatch(o.ext_desc, "P(%d+):(%S+)") do
    setAux(o.id, prop, val)
end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "P214:12345 P236:6789-0001".into();
        let result = run_aux_from_desc(lua, &entry).unwrap();
        assert_eq!(result.commands.len(), 2);
    }

    #[test]
    fn test_person_date_return_early() {
        let lua = r#"
if string.match(o.ext_desc, "active") then return end
local b, d_val = string.match(o.ext_desc, "(%d%d%d%d)%-(%d%d%d%d)")
if b then
    born = b
    died = d_val
end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "active 1920-2000".into();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "");
        assert_eq!(result.died, "");
    }

    #[test]
    fn test_person_date_nil_entry_fields() {
        let lua = r#"
if o.q == nil then born = "1900" end
if o.user == nil then died = "2000" end
"#;
        let entry = test_entry();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "1900");
        assert_eq!(result.died, "2000");
    }

    #[test]
    fn test_person_date_numeric_conversion() {
        // Test that tonumber works on strings from entry
        let lua = r#"
local desc = o.ext_desc
local b = string.match(desc, "(%d+)")
if b and tonumber(b) > 1000 then
    born = b
end
"#;
        let mut entry = test_entry();
        entry.ext_desc = "born in 1850".into();
        let result = run_person_date(lua, &entry).unwrap();
        assert_eq!(result.born, "1850");
    }
}
