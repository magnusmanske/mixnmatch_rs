/**
 * JSON/JSONL import validation logic.
 * Extracted from import.js to reduce file size and improve testability.
 */

/**
 * Validate a single MetaEntry object from a JSON import file.
 * @param {Object} obj  - The entry to validate
 * @param {number} index - Zero-based index for error messages
 * @returns {{ errors: string[], warnings: string[] }}
 */
export function validate_meta_entry(obj, index) {
	var errors = [];
	var warnings = [];
	var prefix = 'Entry #' + (index + 1);

	if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) {
		errors.push(prefix + ': not an object');
		return { errors: errors, warnings: warnings };
	}

	// entry (required)
	if (!obj.entry || typeof obj.entry !== 'object' || Array.isArray(obj.entry)) {
		errors.push(prefix + ': missing or invalid "entry" object');
		return { errors: errors, warnings: warnings };
	}

	var ent = obj.entry;
	if (typeof ent.catalog === 'undefined' || ent.catalog === null) {
		errors.push(prefix + ': entry.catalog is required');
	} else if (typeof ent.catalog !== 'number' || !Number.isInteger(ent.catalog)) {
		errors.push(prefix + ': entry.catalog must be an integer, got ' + typeof ent.catalog);
	}
	if (!ent.ext_id && ent.ext_id !== 0) {
		errors.push(prefix + ': entry.ext_id is required');
	} else if (typeof ent.ext_id !== 'string') {
		errors.push(prefix + ': entry.ext_id must be a string, got ' + typeof ent.ext_id);
	}
	if (!ent.ext_name && ent.ext_name !== '') {
		errors.push(prefix + ': entry.ext_name is required');
	} else if (typeof ent.ext_name !== 'string') {
		errors.push(prefix + ': entry.ext_name must be a string, got ' + typeof ent.ext_name);
	} else if (ent.ext_name.trim() === '') {
		warnings.push(prefix + ': entry.ext_name is empty');
	}

	// Optional entry string fields. `type` is the canonical key (matches
	// the PHP API and display code); `type_name` is accepted as a legacy
	// alias of the same field.
	var opt_str_fields = ['ext_url', 'ext_desc', 'type', 'type_name'];
	for (var si = 0; si < opt_str_fields.length; si++) {
		var sf = opt_str_fields[si];
		if (typeof ent[sf] !== 'undefined' && ent[sf] !== null && typeof ent[sf] !== 'string') {
			errors.push(prefix + ': entry.' + sf + ' must be a string if provided');
		}
	}
	if (typeof ent.q !== 'undefined' && ent.q !== null) {
		if (typeof ent.q !== 'number' || !Number.isInteger(ent.q)) {
			errors.push(prefix + ': entry.q must be an integer (no "Q" prefix)');
		}
	}
	if (typeof ent.user !== 'undefined' && ent.user !== null) {
		if (typeof ent.user !== 'number' || !Number.isInteger(ent.user)) {
			errors.push(prefix + ': entry.user must be an integer');
		}
	}

	// Warn about unknown entry fields
	var known_entry = { catalog: 1, ext_id: 1, ext_name: 1, ext_url: 1, ext_desc: 1, type: 1, type_name: 1, q: 1, user: 1, id: 1, timestamp: 1, random: 1 };
	for (var ek in ent) {
		if (!known_entry[ek]) warnings.push(prefix + ': unknown entry field "' + ek + '"');
	}

	// auxiliary
	if (typeof obj.auxiliary !== 'undefined') {
		if (!Array.isArray(obj.auxiliary)) {
			errors.push(prefix + ': "auxiliary" must be an array');
		} else {
			for (var ai = 0; ai < obj.auxiliary.length; ai++) {
				var aux = obj.auxiliary[ai];
				if (typeof aux !== 'object' || aux === null) {
					errors.push(prefix + ': auxiliary[' + ai + '] is not an object');
					continue;
				}
				if (typeof aux.prop_numeric === 'undefined' || typeof aux.prop_numeric !== 'number' || !Number.isInteger(aux.prop_numeric)) {
					errors.push(prefix + ': auxiliary[' + ai + '].prop_numeric must be an integer');
				}
				if (typeof aux.value === 'undefined' || typeof aux.value !== 'string') {
					errors.push(prefix + ': auxiliary[' + ai + '].value must be a string');
				}
			}
		}
	}

	// coordinate
	if (typeof obj.coordinate !== 'undefined' && obj.coordinate !== null) {
		if (typeof obj.coordinate !== 'object' || Array.isArray(obj.coordinate)) {
			errors.push(prefix + ': "coordinate" must be an object');
		} else {
			if (typeof obj.coordinate.lat !== 'number') errors.push(prefix + ': coordinate.lat must be a number');
			if (typeof obj.coordinate.lon !== 'number') errors.push(prefix + ': coordinate.lon must be a number');
			if (typeof obj.coordinate.lat === 'number' && (obj.coordinate.lat < -90 || obj.coordinate.lat > 90)) {
				errors.push(prefix + ': coordinate.lat out of range (-90..90)');
			}
			if (typeof obj.coordinate.lon === 'number' && (obj.coordinate.lon < -180 || obj.coordinate.lon > 180)) {
				errors.push(prefix + ': coordinate.lon out of range (-180..180)');
			}
			if (typeof obj.coordinate.precision !== 'undefined' && typeof obj.coordinate.precision !== 'number') {
				errors.push(prefix + ': coordinate.precision must be a number if provided');
			}
		}
	}

	// person_dates
	if (typeof obj.person_dates !== 'undefined' && obj.person_dates !== null) {
		if (typeof obj.person_dates !== 'object' || Array.isArray(obj.person_dates)) {
			errors.push(prefix + ': "person_dates" must be an object');
		} else {
			var date_re = /^-?\d{1,4}(-\d{2}(-\d{2})?)?$/;
			if (typeof obj.person_dates.born !== 'undefined' && obj.person_dates.born !== null) {
				if (typeof obj.person_dates.born !== 'string') {
					errors.push(prefix + ': person_dates.born must be a string');
				} else if (!date_re.test(obj.person_dates.born)) {
					warnings.push(prefix + ': person_dates.born "' + obj.person_dates.born + '" does not match expected format (YYYY, YYYY-MM, or YYYY-MM-DD)');
				}
			}
			if (typeof obj.person_dates.died !== 'undefined' && obj.person_dates.died !== null) {
				if (typeof obj.person_dates.died !== 'string') {
					errors.push(prefix + ': person_dates.died must be a string');
				} else if (!date_re.test(obj.person_dates.died)) {
					warnings.push(prefix + ': person_dates.died "' + obj.person_dates.died + '" does not match expected format (YYYY, YYYY-MM, or YYYY-MM-DD)');
				}
			}
		}
	}

	// descriptions
	if (typeof obj.descriptions !== 'undefined' && obj.descriptions !== null) {
		if (typeof obj.descriptions !== 'object' || Array.isArray(obj.descriptions)) {
			errors.push(prefix + ': "descriptions" must be an object (language code -> string)');
		} else {
			for (var dlang in obj.descriptions) {
				if (typeof obj.descriptions[dlang] !== 'string') {
					errors.push(prefix + ': descriptions["' + dlang + '"] must be a string');
				}
			}
		}
	}

	// aliases
	if (typeof obj.aliases !== 'undefined') {
		if (!Array.isArray(obj.aliases)) {
			errors.push(prefix + ': "aliases" must be an array');
		} else {
			for (var ali = 0; ali < obj.aliases.length; ali++) {
				var al = obj.aliases[ali];
				if (typeof al !== 'object' || al === null) {
					errors.push(prefix + ': aliases[' + ali + '] is not an object');
					continue;
				}
				if (typeof al.language !== 'string') errors.push(prefix + ': aliases[' + ali + '].language must be a string');
				if (typeof al.value !== 'string') errors.push(prefix + ': aliases[' + ali + '].value must be a string');
			}
		}
	}

	// mnm_relations
	if (typeof obj.mnm_relations !== 'undefined') {
		if (!Array.isArray(obj.mnm_relations)) {
			errors.push(prefix + ': "mnm_relations" must be an array');
		} else {
			for (var ri = 0; ri < obj.mnm_relations.length; ri++) {
				var rel = obj.mnm_relations[ri];
				if (typeof rel !== 'object' || rel === null) {
					errors.push(prefix + ': mnm_relations[' + ri + '] is not an object');
					continue;
				}
				if (typeof rel.property !== 'number' || !Number.isInteger(rel.property)) {
					errors.push(prefix + ': mnm_relations[' + ri + '].property must be an integer');
				}
				if (!rel.target || typeof rel.target !== 'object') {
					errors.push(prefix + ': mnm_relations[' + ri + '].target must be an object');
				} else {
					var valid_types = ['EntryId', 'CatalogExtId', 'WikidataQid'];
					if (valid_types.indexOf(rel.target.type) === -1) {
						errors.push(prefix + ': mnm_relations[' + ri + '].target.type must be one of: ' + valid_types.join(', '));
					}
					if (typeof rel.target.value === 'undefined') {
						errors.push(prefix + ': mnm_relations[' + ri + '].target.value is required');
					}
				}
			}
		}
	}

	// kv_entries
	if (typeof obj.kv_entries !== 'undefined') {
		if (!Array.isArray(obj.kv_entries)) {
			errors.push(prefix + ': "kv_entries" must be an array');
		} else {
			for (var ki = 0; ki < obj.kv_entries.length; ki++) {
				var kv = obj.kv_entries[ki];
				if (typeof kv !== 'object' || kv === null) {
					errors.push(prefix + ': kv_entries[' + ki + '] is not an object');
					continue;
				}
				if (typeof kv.key !== 'string') errors.push(prefix + ': kv_entries[' + ki + '].key must be a string');
				if (typeof kv.value !== 'string') errors.push(prefix + ': kv_entries[' + ki + '].value must be a string');
			}
		}
	}

	// Warn about unknown top-level fields
	var known_top = { entry: 1, auxiliary: 1, coordinate: 1, person_dates: 1, descriptions: 1, aliases: 1, mnm_relations: 1, kv_entries: 1, issues: 1, log_entries: 1, multi_match: 1, statement_text: 1 };
	for (var tk in obj) {
		if (!known_top[tk]) warnings.push(prefix + ': unknown field "' + tk + '" (will be ignored)');
	}

	return { errors: errors, warnings: warnings };
}

/**
 * Validate and parse an entire JSON/JSONL import file.
 * @param {string} text - Raw file content
 * @param {string} format - 'json' or 'jsonl'
 * @param {string|number} expectedCatalogId - Optional catalog ID to verify against
 * @returns {{ entries: Object[]|null, preview: Object|null, error: string|string[]|null, warnings: string[], catalogId: number|null }}
 */
export function parseAndValidateImportFile(text, format, expectedCatalogId) {
	var entries = [];
	var error = null;
	var warnings = [];

	// Parse
	try {
		if (format === 'jsonl') {
			var lines = text.split('\n');
			for (var i = 0; i < lines.length; i++) {
				if (lines[i].trim() === '') continue;
				try {
					entries.push(JSON.parse(lines[i]));
				} catch (lineErr) {
					return { entries: null, preview: null, error: 'JSON parse error on line ' + (i + 1) + ': ' + lineErr.message, warnings: [], catalogId: null };
				}
			}
		} else {
			var data = JSON.parse(text);
			if (!Array.isArray(data)) {
				return { entries: null, preview: null, error: 'JSON file must contain an array of MetaEntry objects (got ' + typeof data + ')', warnings: [], catalogId: null };
			}
			entries = data;
		}
	} catch (err) {
		return { entries: null, preview: null, error: 'JSON parse error: ' + err.message, warnings: [], catalogId: null };
	}

	if (entries.length === 0) {
		return { entries: null, preview: null, error: 'File contains no entries', warnings: [], catalogId: null };
	}

	// Validate all entries
	var all_errors = [];
	var ext_ids = {};
	var catalog_ids = {};
	var max_display = 50;

	for (var ei = 0; ei < entries.length; ei++) {
		var result = validate_meta_entry(entries[ei], ei);
		for (var j = 0; j < result.errors.length && all_errors.length < max_display; j++) {
			all_errors.push(result.errors[j]);
		}
		for (var k = 0; k < result.warnings.length && warnings.length < max_display; k++) {
			warnings.push(result.warnings[k]);
		}
		// Duplicate ext_id check
		if (entries[ei].entry && entries[ei].entry.ext_id) {
			var eid = entries[ei].entry.ext_id;
			if (ext_ids[eid]) {
				if (all_errors.length < max_display) all_errors.push('Entry #' + (ei + 1) + ': duplicate ext_id "' + eid + '" (first seen at entry #' + ext_ids[eid] + ')');
			} else {
				ext_ids[eid] = ei + 1;
			}
		}
		if (entries[ei].entry && typeof entries[ei].entry.catalog === 'number') {
			catalog_ids[entries[ei].entry.catalog] = true;
		}
	}

	if (all_errors.length >= max_display) all_errors.push('... and more errors (showing first ' + max_display + ')');
	if (warnings.length >= max_display) warnings.push('... and more warnings (showing first ' + max_display + ')');

	// Catalog consistency
	var cat_keys = Object.keys(catalog_ids);
	var catalogId = cat_keys.length === 1 ? parseInt(cat_keys[0]) : null;

	if (cat_keys.length > 1) {
		all_errors.unshift('Multiple catalog IDs found in entries: ' + cat_keys.join(', ') + '. All entries must have the same catalog ID.');
	}
	if (expectedCatalogId && cat_keys.length === 1 && parseInt(cat_keys[0]) !== parseInt(expectedCatalogId)) {
		all_errors.unshift('Catalog ID in file (' + cat_keys[0] + ') does not match the catalog ID field above (' + expectedCatalogId + ')');
	}

	if (all_errors.length > 0) {
		return { entries: null, preview: null, error: all_errors, warnings: warnings, catalogId: catalogId };
	}

	var preview = {
		catalog: expectedCatalogId || catalogId || '?',
		entry_count: entries.length,
		sample: entries.slice(0, 5),
		valid: true,
	};

	return { entries: entries, preview: preview, error: null, warnings: warnings, catalogId: catalogId };
}
