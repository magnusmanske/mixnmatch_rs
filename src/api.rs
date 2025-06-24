/*
<?PHP

namespace MixNMatch;

require_once dirname(__DIR__) . '/vendor/autoload.php';

class SetCompare {
    protected $mnm ;
    protected $table_name ;
    protected $catalog ;
    protected $wd_cache = [] ;
    protected $bad_qs_to_sync = [] ;

    function __construct ( $mnm , $catalog ) {
        $this->catalog = $catalog * 1 ;
        $this->mnm = $mnm ;
        $this->table_name = "MnM_SetCompare_{$this->catalog}" ;
        $this->mnm->max_execution_time = 1000*60*2 ; # 2 min max query time

        # Create temporary table
        $this->mnm->getSQL ( "DROP TEMPORARY TABLE IF EXISTS {$this->table_name}") ;
        $sql = "CREATE TEMPORARY TABLE {$this->table_name} ( " .
            "q INT UNSIGNED NOT NULL DEFAULT 0," .
            "`ext_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',".
            "in_wd  INT UNSIGNED NOT NULL DEFAULT 0," .
            "in_mnm INT UNSIGNED NOT NULL DEFAULT 0," .
            "PRIMARY KEY (q,ext_id)," .
            "INDEX wd_mnm (in_wd,in_mnm)" .
            ")" ;
        $this->mnm->getSQL($sql);
    }

    function __destruct() {
        $this->mnm->getSQL ( "DROP TEMPORARY TABLE IF EXISTS {$this->table_name}") ;
    }

    public function addWD ( $q , $ext_id ) {
        if ( !isset($q) or !isset($ext_id) ) return ;
        $ext_id = trim($ext_id) ;
        if ( $ext_id=='' ) return ;
        $q = preg_replace ( "|\D|" , "" , "$q" ) ;
        if ( $q=='' or $q=='0' ) return ;
        $q = $q*1;
        $ext_id = $this->mnm->escape($ext_id) ;
        $this->wd_cache[] = "{$q},\"{$ext_id}\",1" ;
        if ( count($this->wd_cache)>=10000 ) $this->flushWD() ;
    }

    public function flushWD() {
        if ( count($this->wd_cache)==0 ) return ;
        $sql = "INSERT IGNORE INTO {$this->table_name} (q,ext_id,in_wd) VALUES (" . implode("),(",$this->wd_cache) . ")" ;
        $this->mnm->getSQL($sql);
        $this->wd_cache = [] ;
    }

    public function addFromMnM() {
        $sql = "SELECT /* ".__METHOD__." */
 entry.q,entry.ext_id,1 FROM entry WHERE entry.q IS NOT NULL AND entry.q>0 AND entry.user!=0 AND entry.user IS NOT NULL AND entry.catalog={$this->catalog} AND entry.ext_id NOT LIKE 'fake_id_%'" ;
		$sql = "INSERT INTO {$this->table_name} (q,ext_id,in_mnm) {$sql} ON DUPLICATE KEY UPDATE in_mnm=1" ;
		$this->mnm->getSQL($sql);
	}

	public function get_mnm_dupes() {
		$ret = [] ;
$sql = "SELECT /*NO_RO*/
/* ".__METHOD__." */
 q,group_concat(ext_id SEPARATOR \"\\n\") AS ext_ids,count(*) AS cnt FROM {$this->table_name} WHERE in_mnm=1 GROUP BY q HAVING cnt>1" ;
		$result = $this->mnm->getSQL($sql);
		while($o = $result->fetch_object()) {
			$ret[] = [ $o->q*1 , explode("\n",utf8_encode($o->ext_ids)) ] ;
			$this->bad_qs_to_sync[$o->q] = 1 ;
		}
		return $ret ;
	}

	public function get_diff() {
		$qs = [] ;
		$sql = "SELECT /*NO_RO*/ /* ".__METHOD__." */ * FROM {$this->table_name} WHERE in_wd!=in_mnm" ;
		$result = $this->mnm->getSQL($sql);
		while($o = $result->fetch_object()) {
			$q = "{$o->q}" ;
			if ( !isset($qs[$q]) ) $qs[$q] = [ $q , [] , [] ] ;
			if ( $o->in_wd ) $qs[$q][1][] = utf8_encode($o->ext_id) ;
			if ( $o->in_mnm ) $qs[$q][2][] = utf8_encode($o->ext_id) ;
		}
		$ret = [] ;
		foreach ( $qs AS $k => $v ) $ret[] = $v ;
		return $ret ;
	}

	public function compare_wd_mnm ( $wd_value ) {
		$ret = [] ;
		$mnm_value = 1 - $wd_value ;
		$sql = "SELECT /*NO_RO*/ /* ".__METHOD__." */ * FROM {$this->table_name} WHERE in_wd={$wd_value} AND in_mnm={$mnm_value}" ;
		$result = $this->mnm->getSQL($sql);
		while($o = $result->fetch_object()) {
			$q = "{$o->q}" ;
			if ( isset($this->bad_qs_to_sync[$q]) ) continue ;
			$ret[] = [ $q , utf8_encode($o->ext_id) ] ;
		}
		return $ret ;
	}
}

class RenderATOM {
	private $mnm ;
	private $out ;
	private $wil ;
	private $catalogs = [] ;

	function __construct ( $mnm , $out ) {
		require_once ( "/data/project/mix-n-match/public_html/php/wikidata.php" ) ;
		$this->mnm = $mnm ;
		$this->out = $out ;
		$this->wil = new \WikidataItemList ;
		$this->load_catalogs() ;
		$this->load_wikidata_items();
	}

	protected function escape ( $s ) {
		return htmlspecialchars($s, ENT_XML1, 'UTF-8') ; ;
	}

	protected function load_catalogs() {
		foreach ( $this->out['data']['events'] AS $e ) $catalogs[$e->catalog] = $e->catalog ;
		if ( count($catalogs) > 0 ) {
			$tmp_catalog = new Catalog ( 0 , $this->mnm ) ;
			$this->catalogs = $tmp_catalog->loadCatalogs ( $catalogs ) ;
		}
	}

	protected function load_wikidata_items() {
		$items = [] ;
		foreach ( $this->out['data']['events'] AS $e ) {
			if ( $e->event_type == 'match' and $e->q > 0 ) $items['Q'.$e->q] = 'Q'.$e->q ;
		}
		$this->wil->loadItems ( $items ) ;
	}

	protected function render_item ( $q ) {
		$img = '' ;
		$label = $q ;
		$i = $this->wil->getItem ( $q ) ;
		if ( isset($i) ) {
			$label = $this->escape($i->getLabel()) ;
			if ( $i->hasClaims('P18') ) {
				$fn = $this->mnm->tfc->urlEncode($i->getFirstString('P18')) ;
				$url = "https://commons.wikimedia.org/wiki/Special:Redirect/file/{$fn}?width=160&height=160" ;
				$url = $this->escape($url) ;
				$page_url = htmlspecialchars("https://commons.wikimedia.org/wiki/File:{$fn}", ENT_XML1, 'UTF-8') ;
				$img = "<a href='{$page_url}'><img border='0' src='{$url}' /></a>" ;
			}
		}
		$xml = "<p>Entry was matched to <a href='https://www.wikidata.org/wiki/{$q}'>{$label}</a>" ;
		if ( $label != $q ) $xml .= " <small>[{$q}]</small>" ;
		$xml .= ".</p>" ;
		if ( $img != '' ) $xml .= "<p>{$img}</p>" ;
		return $xml ;
	}

	protected function render_entry_external_id ( $e ) {
		$xml = '<p>External ID: ' ;
		if ( $e->ext_url != '' ) {
			$ext_url = htmlspecialchars($e->ext_url);
			$xml .= "<a href='{$ext_url}'>" ;
		}
		$xml .= $this->escape($e->ext_id) ;
		if ( $e->ext_url != '' ) $xml .= "</a>" ;
		$xml .= "</p>\n" ;
		return $xml ;
	}

	protected function render_entry_catalog ( $e ) {
		if ( !isset($this->catalogs[$e->catalog]) ) return '' ;
		$cat = (object) $this->catalogs[$e->catalog] ;
		$xml = "<p>In catalog #{$e->catalog}: <a href='{$this->mnm->root_url}/#/catalog/{$e->catalog}'>{$cat->name}</a>" ;
		if ( isset($cat->wd_prop) and !isset($cat->wd_qual) ) $xml .= ", set as property <a href='https://www.wikidata.org/wiki/P{$cat->wd_prop}'>P{$cat->wd_prop}</a>" ;
		$xml .= "</p>" ;
		return $xml ;
	}

	protected function render_entry_content ( $e ) {
		$xml = '' ;
		if ( $e->event_type == 'match' ) {
			if ( $e->q == 0 ) {
				$xml .= "<p>Entry was marked as <i>NOT ON WIKIDATA</i></p>" ;
			} else if ( $e->q == -1 ) {
				$xml .= "<p>Entry was marked as <i>NOT APPLICABLE</i></p>" ;
			} else {
				$xml .= $this->render_item ( "Q{$e->q}" ) ;
			}
		}
		if ( $e->ext_desc != '' ) {
			$xml .= '<p>Description: <i>' . $this->escape($e->ext_desc) . "</i></p>\n" ;
		}
		$xml .= $this->render_entry_external_id ( $e ) ;
		$xml .= $this->render_entry_catalog ( $e ) ;

		$xml = "<content type=\"xhtml\"><div xmlns=\"http://www.w3.org/1999/xhtml\">{$xml}</div></content>\n" ;
		return $xml ;
	}

	protected function render_entry ( $e ) {
		$xml = "<entry>\n" ;
		$xml .= '<title>' ;
		if ( $e->event_type == 'match' ) $xml .= "New match for " ;
		else if ( $e->event_type == 'remove_q' ) $xml .= "Match was removed for " ;
		$xml .= '"' . $this->escape($e->ext_name) . '"' ;
		$xml .= "</title>\n" ;
		$xml .= "<link rel=\"alternate\" href=\"{$this->mnm->root_url}/#/entry/{$e->id}\" />\n" ;
		$xml .= "<id>urn:uuid:" . $this->mnm->getUUID() . "</id>\n" ;
		$ts = new \DateTime ( $e->timestamp ) ;
		$xml .= '<updated>' . $ts->format(\DateTime::ATOM) . '</updated>' ;
		$xml .= $this->render_entry_content ( $e ) ;
		$xml .= $this->render_entry_author ( $e->user ) ;
		$xml .= "</entry>\n" ;
		return $xml ;
	}

	protected function render_entry_author ( $user ) {
		if ( isset($this->out['data']['users'][$user]) ) {
			$user_name = $this->out['data']['users'][$user]->name ;
			$user_url = "https://www.wikidata.org/wiki/User:" . urlencode($user_name) ;
		} else {
			$user_name = 'Mix\'n\'Match system' ;
$user_url = 'https: //mix-n-match.toolforge.org' ;
		}
		$xml = '<author>' ;
		$xml .= '<name>' . $this->escape($user_name) . '</name>' ;
		$xml .= '<uri>' . $this->escape($user_url) . '</uri>\n' ;
		$xml .= '</author>' ;
		return $xml ;
	}

	protected function render_header () {
		$time = new \DateTime;
		$ts = $time->format(\DateTime::ATOM);
		return '<?xml version="1.0" encoding="utf-8"?>

		<feed xmlns="http://www.w3.org/2005/Atom">
		<title>Mix\'n\'match</title>
		<subtitle>Recent updates by humans (auto-matching not shown)</subtitle>
		<link href="'.$this->mnm->root_url.'/api.php?query=rc_atom" rel="self" />
		<link href="'.$this->mnm->root_url.'/" />
		<id>urn:uuid:' . $this->mnm->getUUID() . '</id>
		<updated>' . $ts . '</updated>' ;
	}

	public function render () {
		$xml = $this->render_header() ;
		foreach ( $this->out['data']['events'] AS $e ) {
			$xml .= $this->render_entry($e);
		}
		$xml .= '</feed>' ;
		return $xml ;
	}

}
*/

use crate::app_state::AppState;

#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
struct API {
    app: AppState,
}

impl API {
    pub const fn new(app: AppState) -> Self {
        Self { app }
    }
}

/*

class API {
    protected $mnm ;
    protected $user ;
    protected $headers = [] ;
    protected $testing = false ;
    protected $prevent_callback = false ;
    protected $large_properties = ['214','268'] ;
    protected $content_type = 'Content-type: application/json; charset=UTF-8' ;
    const CONTENT_TYPE_TEXT_PLAIN = 'Content-type: text/plain; charset=UTF-8';
    const CONTENT_TYPE_TEXT_HTML = 'Content-type: text/html; charset=UTF-8';
    const CONTENT_TYPE_ATOM_XML = 'Content-type: application/atom+xml; charset=UTF-8';
    protected $query_blacklist = [ '' , 'catalog' , 'catalogs' , 'catalog_details' , 'get_user_info' , 'get_entries_by_q_or_value' , 'update_overview' , 'random' , 'redirect' , 'get_sync' , 'get_jobs' , 'get_entry' ] ;
    protected $code_fragment_allowed_user_ids = [ 2 ] ;

    function __construct ( $mnm = '' ) {
        $this->mnm = is_object($mnm) ? $mnm : new MixNMatch ;
    }

    public function query ( $query ) {
        $this->testing = isset($_REQUEST['testing']) ;
        $fn = 'query_'.$query ;
        if ( method_exists($this,$fn) ) {
            $this->user = $this->get_request ( 'tusc_user' , -1 ) ;
            $out = [ 'status' => 'OK' , 'data' => [] ] ;
            try {
                $this->$fn ( $out ) ;
            } catch (\Exception $e) {
                $out = $this->error($e->getMessage()) ;
            }
            return $out ;
        } else {
            if ( isset($_REQUEST['oauth_verifier']) ) return $this->oauth_validation() ;
            return $this->error ( "Unknown query '{$query}'" ) ;
        }
    }

    public function render ( $out , $callback = '' ) {
        if ( isset($_REQUEST['autoclose']) ) {
            print "<html>
            <script>
            window.close('','_parent','');
            </script>
            </html>";
            exit(0);
        }
        header($this->content_type);
        foreach ( $this->headers AS $h ) header($h) ;
        if ( !$this->prevent_callback && $callback != '' ) print $callback.'(' ;
        if ( is_array($out) ) print json_encode ( $out ) ;
        else print $out ; # Raw string
        if ( !$this->prevent_callback && $callback != '' ) print ')' ;
        $this->mnm->tfc->flush();
        ob_end_flush() ;
        exit(0);
    }

    public function log_use ( $query ) {
        if ( preg_match('|^["\)\/]|',$query) or preg_match('/^(procedure|redirect|http)/',$query) ) exit(0);
        if ( !in_array ( $query , $this->query_blacklist ) ) $this->mnm->tfc->logToolUse ( '' , $query ) ;
    }

    ################################################################################
    # Internal helper functions

    protected function error ( $error_message ) {
        return [ 'status' => $error_message ] ;
    }

    protected function get_escaped_request ( $varname , $default = '' ) {
        $ret = $this->get_request ( $varname , $default ) ;
        return $this->mnm->escape ( $ret ) ;
    }

    protected function get_catalog () {
        $catalog = $this->get_request_int ( 'catalog' ) ;
        if ( $catalog <= 0 ) throw new \Exception('Invalid catalog ID');
        return $catalog ;
    }

    protected function add_sql_to_out ( &$out , $sql , $subkey = '' , $out_key = '' ) {
        $result = $this->mnm->getSQL ( $sql ) ;
        if ( !isset($out['data']) ) $out['data'] = [] ;
        if ( $subkey != '' and !isset($out['data'][$subkey]) ) $out['data'][$subkey] = [] ;
        while($o = $result->fetch_object()) {
            if ( $out_key == '' ) {
                if ( $subkey == '' ) $out['data'][] = $o ;
                else $out['data'][$subkey][] = $o ;
            } else {
                if ( $subkey == '' ) $out['data'][$o->$out_key] = $o ;
                else $out['data'][$subkey][$o->$out_key] = $o ;
            }
        }
    }

    protected function add_entries_and_users_from_sql ( &$out , $sql , $associative = true ) {
        $users = [] ;
        $out['data']['entries'] = [] ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()){
            if ( $associative ) $out['data']['entries'][$o->id] = $o ;
            else $out['data']['entries'][] = $o ;
            if ( isset ( $o->user ) ) $users[$o->user] = 1 ;
        }
        $out['data']['users'] = $this->get_users ( $users ) ;
    }

    protected function render_atom ( &$out ) {
        $ra = new RenderATOM ( $this->mnm , $out ) ;
        $out = $ra->render() ;
        $this->content_type = self::CONTENT_TYPE_ATOM_XML ;
    }


    protected function generateOverview ( &$out ) {
        $sql = "SELECT /* ".__METHOD__." 1 */ overview.* FROM overview,catalog WHERE catalog.id=overview.catalog and catalog.active>=1" ; // overview is "manually" updated, but fast; vw_overview is automatic, but slow
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()){
            foreach ( $o AS $k => $v ) $out['data'][$o->catalog][$k] = $v ;
        }

        $tmp_catalog = new Catalog ( 0 , $this->mnm ) ;
        $catalogs = $tmp_catalog->loadCatalogs([],true) ;
        foreach ( $catalogs AS $catalog_id => $o ) {
            foreach ( $o AS $k => $v ) $out['data'][$catalog_id][$k] = $v ;
        }


        $sql = "SELECT /* ".__METHOD__." 2 */ user.name AS username,catalog.id from catalog,user where owner=user.id AND active>=1" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()){
            foreach ( $o AS $k => $v ) $out['data'][$o->id][$k] = $v ;
        }

        $sql = "SELECT /* ".__METHOD__." 3 */ catalog.id AS id,last_update,do_auto_update,autoscrape.json AS json from catalog,autoscrape WHERE active>=1 AND catalog.id=autoscrape.catalog" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()) {
            $out['data'][$o->id]['has_autoscrape'] = 1 ;
            if ( $o->do_auto_update != 1 ) continue ;
            $out['data'][$o->id]['scrape_update'] = 1 ;
            $out['data'][$o->id]['autoscrape_json'] = $o->json ;
            $lu = $o->last_update ;
            $lu = substr($lu,0,4).'-'.substr($lu,4,2).'-'.substr($lu,6,2).' '.substr($lu,8,2).':'.substr($lu,10,2).':'.substr($lu,12,2) ;
            $out['data'][$o->id]['last_scrape'] = $lu ;
        }
    }

    protected function generateOverviewSingleCatalog ( &$out , $catalog_id ) {
        $sql = "SELECT /* ".__METHOD__." 1 */  overview.* FROM overview WHERE {$catalog_id}=overview.catalog" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()){
            foreach ( $o AS $k => $v ) $out['data'][$o->catalog][$k] = $v ;
        }

        $tmp_catalog = new Catalog ( 0 , $this->mnm ) ;
        $catalogs = $tmp_catalog->loadCatalogs([$catalog_id],true) ;
        foreach ( $catalogs AS $catalog_id => $o ) {
            foreach ( $o AS $k => $v ) $out['data'][$catalog_id][$k] = $v ;
        }

        if ( !isset($out['data'][$catalog_id]['active']) ) $out['data'][$catalog_id]['active'] = "0";


        $sql = "SELECT /* ".__METHOD__." 2 */ user.name AS username,catalog.id from catalog,user where owner=user.id AND catalog.id={$catalog_id}" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()){
            foreach ( $o AS $k => $v ) $out['data'][$catalog_id][$k] = $v ;
        }

        // DEACTIVATED slow
        // $sql = "SELECT /* ".__METHOD__." 3 */ min(timestamp) AS earliest,max(timestamp) AS latest FROM entry USE INDEX(catalog_only) WHERE catalog={$catalog_id}" ;
        // $result = $this->mnm->getSQL ( $sql ) ;
        // while($o = $result->fetch_object()){
        // 	$out['data'][$catalog_id]['earliest_match'] = $o->earliest ;
        // 	$out['data'][$catalog_id]['latest_match'] = $o->latest ;
        // }

        $sql = "SELECT /* ".__METHOD__." 4 */ autoscrape.catalog AS id,last_update,do_auto_update,autoscrape.json AS json from autoscrape WHERE autoscrape.catalog={$catalog_id}" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()) {
            $out['data'][$o->id]['has_autoscrape'] = 1 ;
            if ( $o->do_auto_update != 1 ) continue ;
            $out['data'][$o->id]['scrape_update'] = 1 ;
            $out['data'][$o->id]['autoscrape_json'] = $o->json ;
            $lu = $o->last_update ;
            $lu = substr($lu,0,4).'-'.substr($lu,4,2).'-'.substr($lu,6,2).' '.substr($lu,8,2).':'.substr($lu,10,2).':'.substr($lu,12,2) ;
            $out['data'][$o->id]['last_scrape'] = $lu ;
        }
    }

    protected function oauth_validation () {
        require_once '/data/project/magnustools/public_html/php/Widar.php' ;
        $widar = new \Widar ( 'mix-n-match' ) ;
        $widar->attempt_verification_auto_forward ( $this->mnm->root_url ) ;
        throw new \Exception("No valid OAuth validation") ;
    }

    protected function insert_ignore ( $table , $data ) {
        $keys = array_keys($data);
        $values = [] ;
        foreach ( $keys AS $k ) $values[] = $this->mnm->escape($data[$k]) ;
        $sql = "INSERT IGNORE INTO `{$table}` (`" . implode ( "`,`" , $keys ) . "`) VALUES ('" . implode ( "','" , $values ) . "')" ;
        $this->mnm->getSQL ( $sql ) ;
    }

    protected function get_request_int ( $key , $default = 0 ) {
        return (int) $this->get_request ( $key , $default ) ;
    }



    ################################################################################
    # Queries

    protected function query_update_ext_urls ( &$out ) {
        $username = $this->get_request ( 'username' , '' ) ;
        $username = str_replace ( '_' , ' ' , $username ) ;
        $sql = "SELECT /* ".__METHOD__." */ * FROM user WHERE name='".$this->mnm->escape($username)."' LIMIT 1" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        $found = ($o = $result->fetch_object()) ;
        if ( !$found ) throw new \Exception("No such user '{$username}'") ;
        if ( !$o->is_catalog_admin ) throw new \Exception("'{$username}'' is not a catalog admin") ;

        $catalog_id = $this->get_request_int ( 'catalog') ;
        if ( $catalog_id<=0 ) throw new \Exception("Bad or missing catalog parameter") ;
        $url = $this->get_request ( 'url' , '' ) ;
        if ( $url=='' ) throw new \Exception("url parameter is empty") ;

        $parts = explode('$1',$url);
        if ( count($parts)!=2 ) throw new \Exception("Bad $1 replacement for '{$url}'") ;
        $before = $this->mnm->escape($parts[0]);
        $after = $this->mnm->escape($parts[1]);
        $sql = "UPDATE entry SET ext_url=concat('{$before}',ext_id,'{$after}') WHERE catalog={$catalog_id}";
        $out['data']['sql'] = $sql;
        $this->mnm->getSQL ( $sql ) ;
    }

    protected function query_get_source_headers ( &$out ) {
        //$user_id = $this->check_and_get_user_id ( $this->get_request ( 'username' , '' ) ) ;
        $update_info = json_decode ( $this->get_request ( 'update_info' , '' ) ) ;
        $uc = new UpdateCatalog ( 0 , $this->mnm , $update_info ) ;
        $uc->setTesting ( true , false ) ;
        #print_r ( $uc->update_catalog() ) ;
        $out['data'] = $uc->get_header_row() ;
    }

    protected function query_test_import_source ( &$out ) {
        $update_info = json_decode ( $this->get_request ( 'update_info' , '' ) ) ;
        $uc = new UpdateCatalog ( 0 , $this->mnm , $update_info ) ;
        $uc->setTesting ( true , false ) ;
        $out['data'] = $uc->update_catalog() ;
    }

    protected function query_import_source ( &$out ) {
        #error_reporting(E_ERROR|E_CORE_ERROR|E_ALL|E_COMPILE_ERROR);
        #ini_set('display_errors', 'On');
        $catalog_id = $this->get_request_int ( 'catalog') ;
        $user_id = $this->check_and_get_user_id ( $this->get_request ( 'username' , '' ) ) ;
        $seconds = $this->get_request_int ( 'seconds' ) ;
        $update_info = json_decode ( $this->get_request ( 'update_info' , '{}' ) ) ;
        $meta = json_decode ( $this->get_request ( 'meta' , '{}' ) ) ;

        if ( $catalog_id == 0 ) {
            if ( $update_info->default_type??'' == 'Q5' ) $meta->type = 'biography' ;
            $meta->note = 'Created via import' ;
            $catalog = new Catalog ( 0 , $this->mnm ) ;
            $catalog_id = $catalog->createNew ( $meta , $user_id ) ;
        }

        $uc = new UpdateCatalog ( $catalog_id , $this->mnm , $update_info ) ;
        $uc->store_update_info ( $user_id , 'Via web interface' ) ;
        $this->mnm->queue_job($catalog_id,'update_from_tabbed_file',0,'',$seconds,$user_id);
        $out['catalog_id'] = $catalog_id ;
    }

    protected function query_upload_import_file ( &$out ) {
        $data = [
            'uuid' => $this->mnm->getUUID() ,
            'user' => $this->check_and_get_user_id ( $this->get_request ( 'username' , '' ) ) ,
            'timestamp' => $this->mnm->getCurrentTimestamp() ,
            'type' => strtolower ( $this->get_escaped_request ( 'data_format' , 'CSV' ) )
        ] ;
        $out['uuid'] = $data['uuid'] ;
        $uc = new UpdateCatalog ( 0 , $this->mnm , [] ) ;
        $target_file = $uc->import_file_path($data['uuid']) ;
        $uploaded_file = $_FILES["import_file"]["tmp_name"] ;
        if ( !move_uploaded_file ( $uploaded_file , $target_file ) ) {
            $post_max_size = ini_get('post_max_size');
            $upload_max_filesize = ini_get('upload_max_filesize');
            throw new \Exception("Could not move uploaded file. File size probably too large ({$post_max_size}/{$upload_max_filesize}).") ;
        }
        $this->insert_ignore ( 'import_file' , $data ) ;
    }

    protected function query_get_entry_by_extid ( &$out ) {
        $catalog = $this->get_catalog() ;
        $ext_id = $this->get_escaped_request ( 'extid' , '' ) ;
        $this->add_sql_to_out ( $out , "SELECT /* ".__METHOD__." */ * FROM entry WHERE catalog={$catalog} AND ext_id='{$ext_id}'" , 'entries' , 'id' ) ;
        $this->add_extended_entry_data($out) ;
    }

    protected function query_catalogs ( &$out ) {
        $this->generateOverview ( $out ) ;
    }

    protected function query_single_catalog ( &$out ) {
        $catalog_id = $this->get_request_int ( 'catalog_id' ) ;
        $this->generateOverviewSingleCatalog ( $out , $catalog_id ) ;
    }

    protected function query_widar ( &$out ) {
        require_once '/data/project/magnustools/public_html/php/Widar.php' ;
        $widar = new \Widar ( 'mix-n-match' ) ;
        $widar->authorization_callback = $this->mnm->root_url.'/api.php' ;
        $widar->render_reponse ( true ) ;
        exit(0);
    }

    protected function query_catalog_overview ( &$out ) {
        $catalogs = explode ( ',' , $this->get_request ( 'catalogs' , '' ) ) ;
        foreach ( $catalogs AS $catalog_id ) {
            $catalog = new Catalog ( $catalog_id , $this->mnm ) ;
            try {
                $catalog->outputOverview ( $out ) ;
            } catch (\Exception $e) {
                $this->mnm->last_error = $e->getMessage() ;
            }
        }
    }

    protected function query_get_user_info ( &$out ) {
        $username = $this->get_escaped_request ( 'username' , '' ) ;
        $username = str_replace ( '_' , ' ' , $username ) ;
        $sql = "SELECT /* ".__METHOD__." */ * FROM user WHERE name='{$username}' LIMIT 1" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        if($o = $result->fetch_object()) $out['data'] = $o ;
        else throw new \Exception("No user '{$username}' found") ;
    }

    protected function query_add_aliases ( &$out ) {
        $user_id = $this->check_and_get_user_id ( $this->get_request ( 'username' , '' ) ) ;
        $text = trim ( $this->get_request ( 'text' , '' ) ) ;
        $catalog_id = $this->get_request_int ( 'catalog' ) ;
        if ( $catalog_id <= 0 or $text == '' ) throw new \Exception("Catalog ID or text missing") ;
        $catalog = $this->mnm->loadCatalog($catalog_id,true) ;
        $rows = explode ( "\n" , $text ) ;
        unset ( $text ) ;
        foreach ( $rows as $row ) {
            $parts = explode ( "\t" , trim ( $row ) ) ;
            if ( count($parts) < 2 or count($parts) > 3 ) continue ;
            if ( count($parts) < 3 ) $parts[] = '' ;
            if ( $parts[2] == '' ) $parts[2] = $catalog->data()->search_wp ; # Default language
            $ext_id = $this->mnm->escape ( trim($parts[0]) ) ;
            $label = $this->mnm->escape ( trim(str_replace('|','',$parts[1])) ) ;
            $language = $this->mnm->escape ( trim(strtolower($parts[2])) ) ;
            $subquery = "SELECT /* ".__METHOD__." */ id FROM entry WHERE catalog={$catalog_id} AND ext_id='{$ext_id}' LIMIT 1" ;
            $sql = "INSERT IGNORE INTO `aliases` (entry_id,language,label,added_by_user) VALUES (({$subquery}),'{$language}','{$label}',{$user_id})" ;
            $this->mnm->getSQL($sql) ;
        }
    }

    protected function query_get_jobs ( &$out ) {
        $catalog_id = $this->get_request('catalog',0)*1 ; // 0 is valid here
        $start = $this->get_request('start',0)*1 ;
        $max = $this->get_request('max',50)*1 ;

        if ( $catalog_id==0 ) {
            $out['stats'] = [];
            $sql = "SELECT /* ".__METHOD__." */ `status`,count(*) AS `cnt` FROM `jobs` WHERE `status`!='BLOCKED' GROUP BY `status` ORDER BY `status`" ;
            $result = $this->mnm->getSQL ( $sql ) ;
            while($o = $result->fetch_object()) $out['stats'][] = [$o->status,$o->cnt];
        }

        $conditions = [] ;
        if ( $catalog_id > 0 ) $conditions[] = "catalog={$catalog_id}" ;
        $sql = "SELECT /* ".__METHOD__." */ jobs.*,(SELECT user.name FROM user WHERE user.id=`jobs`.`user_id`) AS user_name FROM jobs" ;
        $sql .= " WHERE `status`!='BLOCKED'";
        if ( count($conditions) > 0 ) $sql.= " AND (" . implode ( ") AND (" , $conditions ) . ")" ;
        $sql .= " ORDER BY FIELD(status,'RUNNING','FAILED','TODO','LOW_PRIORITY','PAUSED','DONE'), last_ts DESC,next_ts DESC" ;
        if ( $max > 0 ) $sql .= " LIMIT {$max}" ;
        if ( $start > 0 ) $sql .= " OFFSET {$start}" ;
        $this->add_sql_to_out ( $out , $sql , '' ) ;
    }

    protected function get_existing_job_actions() {
        $ret = [];
        $sql = "SELECT DISTINCT `action` FROM `jobs` UNION SELECT DISTINCT `action` FROM `job_sizes`";
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()) $ret[] = $o->action;
        return $ret ;
    }

    protected function query_start_new_job ( &$out ) {
        $catalog_id = $this->get_catalog();
        $action = trim(strtolower($this->get_request('action','')));
        if ( !preg_match('/^[a-z_]+$/',$action) ) throw new \Exception("Bad action: '{$action}'") ;
        $actions = $this->get_existing_job_actions();
        if ( !in_array($action, $actions) )  throw new \Exception("Unknown action: '{$action}'") ;

        $user_id = $this->check_and_get_user_id ( $this->get_request ( 'username' , '' ) ) ;

        # Default seconds
        $seconds = 0 ;
        if ( $action == 'autoscrape' ) $seconds = 2629800*3 ; # Three months

        # Save info from previous job, if any
        $job = $this->mnm->get_job($catalog_id,$action) ;
        if ( $job->status=='BLOCKED' ) throw new \Exception("Job is blocked") ;
        if ( isset($job) and isset($job->seconds) ) $seconds = $job->seconds ;

        $this->mnm->queue_job ( $catalog_id , $action , 0 , '' , $seconds , $user_id ) ;
    }

    protected function query_update_overview ( &$out ) {
        $catalog = $this->get_request ( 'catalog' , '' ) ;
        if ( $catalog == '' ) $catalogs = $this->mnm->getAllCatalogIDs() ;
        else $catalogs = explode ( ',' , $catalog ) ;
        $this->mnm->updateCatalogs ( $catalogs ) ;
    }

    protected function get_numeric_array_from_comma_separated_string ( $s ) {
        $ret = explode ( ',' , $s ) ;
        #PHP7.4
        #$ret = array_map(fn($value) => $value * 1, $ret) ;
        #$ret = array_filter($ret, function($v, $k) {return $v>0 }, ARRAY_FILTER_USE_BOTH);
        $ret2 = [] ;
        foreach ( $ret AS $k => $v ) {
            $v = (int)$v * 1;
            if ( $v > 0 ) $ret2[$v] = $v ;
        }
        return $ret2 ;
    }

    protected function query_cersei_forward() {
        $scraper_id = $this->get_request_int ( 'scraper' ) ;
        $sql = "SELECT /* ".__METHOD__." */ `catalog_id` FROM `cersei` WHERE `cersei_scraper_id`={$scraper_id}" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        if($o = $result->fetch_object()) {
            $url = "https://mix-n-match.toolforge.org/#/catalog/{$o->catalog_id}" ;
            header('Location: '.$url);
            exit(0);
        }
        throw new \Exception("No catalog associated with CERSEI scraper {$scraper_id}") ;
    }

    protected function query_entries_query_given_name ( $given_name_gender , $given_name , &$joins , &$where ) {
        if ( $given_name_gender == 'yes' ) {
            $joins[] = "INNER JOIN `entry2given_name` egn ON egn.`entry_id`=e.`id`" ;
        } else if ( $given_name_gender == 'no' ) {
            $joins[] = "LEFT JOIN `entry2given_name` egn ON egn.`entry_id`=e.`id`" ;
            $where[] = "egn.`id` IS NULL" ;
        } else if ( $given_name_gender!='any' or $given_name!='' ) {
            $joins[] = "INNER JOIN `entry2given_name` egn ON egn.`entry_id`=e.`id`" ;
            if ( $given_name!='' ) {
                $joins[] = "INNER JOIN given_name gn ON gn.id=egn.given_name_id" ;
                $where[] = "gn.name='{$given_name}'" ;
            } else {
                $joins[] = "INNER JOIN given_name gn FORCE INDEX (id) ON gn.id=egn.given_name_id" ;
                $where[] = "gn.gender='{$given_name_gender}'" ;
            }
        }
    }

    protected function query_entries_query_year ( $has_date , $year_before , $year_after , $table , &$joins , &$where ) {
        if ( $table == 'pd_b' ) $column = 'year_born' ;
        if ( $table == 'pd_d' ) $column = 'year_died' ;
        if ( $has_date=='yes' ) {
            $year_before = preg_replace('|\D|','',$year_before) ;
            $year_after = preg_replace('|\D|','',$year_after) ;
            $joins[] = "INNER JOIN `person_dates` `{$table}` ON `{$table}`.`entry_id`=e.`id`" ;
            if ( $year_before.$year_after == '' ) {
                $where[] = "`{$table}`.`{$column}`!=''" ;
            } else {
                if ( $year_before!='' ) $where[] = "`{$table}`.`{$column}`<".str_pad($year_before,4,'0') ;
                if ( $year_after !='' ) $where[] = "`{$table}`.`{$column}`>".str_pad($year_after,4,'0') ;
            }
        } else if ( $has_date=='no' ) {
            $joins[] = "LEFT JOIN `person_dates` `{$table}` ON `{$table}`.`entry_id`=e.`id`" ;
            $where[] = "`{$table}`.`entry_id` IS NULL OR `{$table}`.`{$column}`=''" ;
        }
    }

    protected function query_entries_query_catalogs ( $catalogs_yes , $catalogs_no , &$where ) {
        $sql = "SELECT /* ".__METHOD__." */ `id` FROM `catalog` WHERE `active`!=1" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()) $catalogs_no[$o->id*1] = $o->id*1 ;
        $catalogs_no = array_unique ( $catalogs_no ) ;
        if ( count($catalogs_yes) > 0 ) {
            foreach ( $catalogs_no AS $id ) unset($catalogs_yes[$id]) ;
            $where[] = "e.`catalog` IN (".implode(',',$catalogs_yes).")" ;
        } else {
            $where[] = "e.`catalog` NOT IN (".implode(',',$catalogs_no).")" ;
        }
    }

    protected function query_entries_query_aux ( $auxiliary , &$where ) {
        foreach ( $auxiliary AS $aux ) {
            $prop = preg_replace('|\D|','',$aux->property) ; # Numeric only
            if ( $prop == '' ) continue ;
            if ( trim($aux->value) == '' ) {
                $where[] = "EXISTS (SELECT * FROM `auxiliary` WHERE e.`id`=`entry_id` AND `aux_p`={$prop})" ;
            } else {
                $v = $this->mnm->escape($aux->value) ;
                $where[] = "EXISTS (SELECT * FROM `auxiliary` WHERE e.`id`=`entry_id` AND `aux_p`={$prop} AND `aux_name`='{$v}')" ;
            }
        }
    }

    protected function query_entries_query_status ( $unmatched , $prelim_matched , $fully_matched , &$where ) {
        if ( $unmatched and $prelim_matched and $fully_matched ) {
            # No filter, return all
        } else if ( !$unmatched and !$prelim_matched and !$fully_matched ) {
            throw new \Exception("ERROR: No possible matching status left") ;
        } else {
            $parts = [] ;
            if ( $unmatched ) $parts[] = "e.`q` IS NULL" ;
            if ( $prelim_matched ) $parts[] = "e.`user`=0" ;
            if ( $fully_matched ) $parts[] = "e.`user`>0" ;
            $where[] = implode(" OR ",$parts) ;
        }
    }

    protected function query_entries_query_location ( $has_location , &$joins , &$where ) {
        if ( $has_location == 'yes' ) {
            $joins[] = "INNER JOIN `location` l on l.`entry_id`=e.`id`" ;
        } else if ( $has_location == 'no' ) {
            $joins[] = "LEFT JOIN `location` l on l.`entry_id`=e.`id`" ;
            $where[] = "l.`id` IS NULL" ;
        }
    }

    protected function query_entries_query_types ( $entry_types , &$where ) {
        if ( count($entry_types) == 0 ) return ;
        $types = [] ;
        foreach ( $entry_types AS $type ) {
            $type = trim($type) ;
            if ( preg_match('|^Q\d+$|',$type) ) $types[] = $type ;
        }
        if ( count($types) > 0 ) $where[] = "e.`type` IN ('".implode("','",$types)."')" ;
    }

    protected function query_entries_query ( &$out ) {
        $offset = $this->get_request_int ( 'offset' ) ;
        $unmatched = $this->get_request_int ( 'unmatched' ) ;
        $prelim_matched = $this->get_request_int ( 'prelim_matched' ) ;
        $fully_matched = $this->get_request_int ( 'fully_matched' ) ;
        $has_location = $this->get_request ( 'has_location' , 'any' ) ;
        $has_birth_date = $this->get_request ( 'has_birth_date' , 'any' ) ;
        $has_death_date = $this->get_request ( 'has_death_date' , 'any' ) ;
        $birth_year_before = $this->get_request ( 'birth_year_before' ) ;
        $birth_year_after = $this->get_request ( 'birth_year_after' ) ;
        $death_year_before = $this->get_request ( 'death_year_before' ) ;
        $death_year_after = $this->get_request ( 'death_year_after' ) ;
        $auxiliary = json_decode($this->get_request ( 'aux' , '[]' )) ;
        $given_name = trim ( $this->get_escaped_request ( 'given_name' ) ) ;
        $given_name_gender = $this->get_escaped_request ( 'given_name_gender' , 'any' ) ;
        $catalogs_yes = $this->get_numeric_array_from_comma_separated_string ( $this->get_request ( 'catalogs_yes' ) ) ;
        $catalogs_no = $this->get_numeric_array_from_comma_separated_string ( $this->get_request ( 'catalogs_no' ) ) ;
        $entry_types = explode ( ',' , $this->get_request ( 'entry_types' ) ) ;

        $tables = ['`entry` e'] ;
        $where = [] ;
        $joins = [] ;

        $this->query_entries_query_types ( $entry_types , $where ) ;
        $this->query_entries_query_aux ( $auxiliary , $where ) ;
        $this->query_entries_query_year ( $has_birth_date , $birth_year_before , $birth_year_after , 'pd_b' , $joins , $where ) ;
        $this->query_entries_query_year ( $has_death_date , $death_year_before , $death_year_after , 'pd_d' , $joins , $where ) ;
        $this->query_entries_query_given_name ( $given_name_gender , $given_name , $joins , $where ) ;
        $this->query_entries_query_catalogs ( $catalogs_yes , $catalogs_no , $where ) ;
        $this->query_entries_query_status ( $unmatched , $prelim_matched , $fully_matched , $where ) ;
        $this->query_entries_query_location ( $has_location , $joins , $where ) ;

        $tables = implode(',',$tables) ;
        $where = "(".implode(") AND (",$where).")" ;
        $joins = implode(" ",$joins) ;
        $sql = "SELECT /* ".__METHOD__." */ e.* FROM {$tables} {$joins} WHERE {$where} LIMIT 50 OFFSET {$offset}" ;
        $out['sql'] = $sql ;
        $this->add_entries_and_users_from_sql ( $out , $sql ) ;
        $this->add_extended_entry_data($out) ;
    }

    protected function query_entries_via_property_value ( &$out ) {
        $property = preg_replace('|\D|','',$this->get_escaped_request ( 'property' ))*1;
        $value = trim ( $this->get_escaped_request ( 'value' ) ) ;
        $sql = "SELECT /* ".__METHOD__." */ * FROM `entry`
            WHERE entry.catalog IN (SELECT id FROM catalog WHERE active=1 AND wd_prop={$property} AND wd_qual IS NULL)
            AND `ext_id`='{$value}'
            UNION
            SELECT * FROM `entry`
            WHERE `id` IN (SELECT DISTINCT entry_id FROM auxiliary WHERE aux_p={$property} AND aux_name='{$value}')
            AND `catalog` IN (SELECT `id` FROM `catalog` WHERE `active`=1)";
        $this->add_sql_to_out ( $out , $sql , 'entries' , 'id' ) ;
        $this->add_extended_entry_data($out) ;
        $users = ['!!!!'];
        foreach ( $out['data']['entries'] AS $e ) {
            if ( isset ( $e->user ) ) {
                $users[$e->user] = $e->user;
            }
        }
        $out['data']['users'] = $this->get_users ( $users ) ;
    }

    protected function query_random_person_batch ( &$out ) {
        $gender = trim ( $this->get_escaped_request ( 'gender' , '' ) ) ;
        $has_desc = $this->get_request_int ( 'has_desc' ) ;
        $max = 50 ;
        $rand = $this->mnm->rand() ;
        $where = [];
        $where[] = "entry2given_name.random>={$rand}" ;
        $where[] = "e1.id=entry_id" ;
        $where[] = "e1.q IS NULL" ;
        if ( $gender == '' ) {
            $from = 'entry e1,entry2given_name' ;
        } else {
            $from = 'entry e1,entry2given_name,given_name FORCE INDEX (id)' ;
            $where[] = "given_name.id=given_name_id" ;
            $where[] = "given_name.gender='{$gender}'" ;
        }
        if ( $has_desc ) {
            $where[] = "e1.ext_desc!=''" ;
            $where[] = "e1.ext_desc!='person'" ;
            $where[] = "e1.ext_desc!=e1.ext_name" ;
        }
        $sql = "SELECT /* ".__METHOD__." */ e1.*,(select count(*) FROM entry e2 WHERE e1.ext_name=e2.ext_name) AS name_count FROM {$from} WHERE " . implode(' AND ',$where) . " ORDER BY entry2given_name.random LIMIT {$max}";
        $out['sql'] = $sql ;
        $result = $this->mnm->getSQL ( $sql ) ;
        $out['data'] = [] ;
        while($o = $result->fetch_object()) $out['data'][] = $o ;
    }

    protected function query_sparql_list ( &$out ) {
        $out['data'] = ['entries'=>[],'users'=>[] ] ;

        $label2q = [] ;
        $labels = [] ;
        $sparql = $this->get_request ( 'sparql' , '' ) ;
        $j = $this->mnm->tfc->getSPARQL ( $sparql ) ;
        $vars = $j->head->vars ;
        $var1 = $vars[0] ;
        $var2 = $vars[1] ;
        foreach ( $j->results->bindings AS $b ) {
            $v1 = $b->$var1 ;
            $v2 = $b->$var2 ;
            if ( $v1->type == 'uri' and $v2->type == 'literal' ) {
                $q = preg_replace ( '/^.+\/Q/' , 'Q' , $v1->value ) ;
                $label = $v2->value ;
            } else if ( $v2->type == 'uri' and $v1->type == 'literal' ) {
                $q = preg_replace ( '/^.+\/Q/' , 'Q' , $v2->value ) ;
                $label = $v1->value ;
            } else continue ;
            $label2q[$label] = $q ;
            $labels[] = $this->mnm->escape ( $label ) ;
        }

        $sql = "SELECT /* ".__METHOD__." */ * FROM entry WHERE (user=0 OR q is null) AND ext_name IN ('" . implode("','",$labels) . "')" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()){
            if ( !isset($label2q[$o->ext_name]) ) continue ; // Paranoia
            $o->user = 0 ;
            $o->q = substr ( $label2q[$o->ext_name] , 1 ) * 1 ;
            $o->timestamp = '20180304223800' ;
            $out['data']['entries'][$o->id] = $o ;
        }


        $out['data']['users'] = $this->get_users ( [0] ) ;
    }

    protected function query_catalog_details ( &$out ) {
        $catalog = $this->get_catalog();
        $this->add_sql_to_out ( $out , "SELECT /* ".__METHOD__." 1 */ `type`,count(*) AS cnt FROM entry WHERE catalog=$catalog GROUP BY type ORDER BY cnt DESC" , 'type' ) ;
        $this->add_sql_to_out ( $out , "SELECT /* ".__METHOD__." 2 */ substring(timestamp,1,6) AS ym,count(*) AS cnt FROM entry WHERE catalog=$catalog AND timestamp IS NOT NULL AND user!=0 GROUP BY ym ORDER BY ym" , 'ym' ) ;
        $this->add_sql_to_out ( $out , "SELECT /* ".__METHOD__." 3 */ name AS username,entry.user AS uid,count(*) AS cnt FROM entry,user WHERE catalog=$catalog AND entry.user=user.id AND user!=0 AND entry.user IS NOT NULL GROUP BY uid ORDER BY cnt DESC" , 'user' ) ;
    }

    protected function query_remove_all_multimatches ( &$out ) {
        $user_id = $this->check_and_get_user_id ( $this->user ) ;
        $entry_id = $this->get_request_int ( 'entry' , -1 ) ;
        $sql = "DELETE FROM `multi_match` WHERE entry_id={$entry_id}" ;
        $this->mnm->getSQL ( $sql ) ;
    }

    protected function query_match_q ( &$out ) {
        $user_id = $this->check_and_get_user_id ( $this->user ) ;
        $entry = $this->get_request_int ( 'entry' , -1 ) ;
        $q = $this->get_request_int ( 'q' , -1 ) ;

        if ( !$this->mnm->setMatchForEntryID ( $entry , $q , $user_id , false ) ) throw new \Exception("Problem with setting the match: {$this->mnm->last_error}") ;
        $sql = "SELECT /* ".__METHOD__." */ *,entry.type AS entry_type FROM entry,catalog WHERE entry.id=$entry and entry.catalog=catalog.id" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()) $out['entry'] = $o ;
    }

    protected function query_match_q_multi ( &$out ) {
        $catalog = $this->get_catalog() ;
        $user_id = $this->check_and_get_user_id ( $this->user ) ;
        $data = json_decode ( $this->get_request('data','[]') ) ;

        $out['not_found'] = 0 ;
        $out['not_found_list'] = [] ;
        $out['no_changes_written'] = [] ;
        foreach ( $data AS $d ) {
            if ( $this->mnm->setMatchForCatalogExtID ( $catalog , $d[1] , $d[0] , $user_id , true , false ) ) continue ;
            if ( preg_match ( "/^External ID '(.*?)'.* not found\.$/" , $this->mnm->last_error , $m ) ) {
                $out['not_found']++ ;
                if ( count($out['not_found_list']<100) ) $out['not_found_list'][] = $m[1];
            }
            else if ( preg_match ( '/^No changes written\.$/' , $this->mnm->last_error ) ) $out['no_changes_written'][] = [ 'ext_id' => $d[1] , 'new_q' => $d[0] , 'entry' => $this->mnm->last_entry ] ;
            else throw new \Exception("Problem with setting the match: {$this->mnm->last_error}") ;
        }
    }

    protected function query_remove_q ( &$out ) {
        $user_id = $this->check_and_get_user_id ( $this->user ) ;
        $entry_id = $this->get_request_int ( 'entry' , -1 ) ;
        if ( !$this->mnm->removeMatchForEntryID ( $entry_id ,  $user_id ) ) throw new \Exception($this->mnm->last_error) ;
    }

    protected function get_entry_object_from_id ( $entry_id ) {
        try {
            $entry = new Entry ( $entry_id , $this->mnm ) ;
            return $entry->core_data() ;
        } catch (Exception $e) {
            # Ignore, return undef
        }
    }

    protected function query_remove_all_q ( &$out ) {
        $user_id = $this->check_and_get_user_id ( $this->user ) ;
        $entry_id = $this->get_request_int ( 'entry' , -1 ) ;
        $entry = $this->get_entry_object_from_id($entry_id);
        $catalog = $entry->catalog*1 ;
        $q = $entry->q*1 ;
        if ( !isset($q) or $q == null ) return ;
        $sql = "UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `catalog`={$catalog} AND `user`=0 AND `q`={$q}" ;
        $this->mnm->getSQL ( $sql ) ;
    }

    # This returns a list of "not in wikidata" entries that could be creates. Likely not used.
    protected function query_create ( &$out ) {
        $catalog = $this->get_catalog() ;
        $out['total'] = [] ;
        $sql = "SELECT /* ".__METHOD__." */ ext_id,ext_name,ext_desc,ext_url,type FROM entry WHERE " ;
        if ( 1 ) $sql .= "q=-1 AND user>0 AND catalog={$catalog} ORDER BY ext_name" ; // Blank
        else $sql .= "q IS null AND catalog={$catalog} ORDER BY ext_name" ; // non-assessed, careful!
        $this->add_sql_to_out ( $out , $sql ) ;
    }

    protected function query_sitestats ( &$out ) {
        $out['total'] = [] ;
        $catalog = $this->get_request ( 'catalog' , '' ) ; # Blank is a valid option

        $sql = "SELECT /* ".__METHOD__." 1 */ DISTINCT catalog,q FROM entry WHERE user>0 AND q IS NOT NULL" ;
        if ( $catalog != '' ) $sql .= " AND catalog=" . $this->mnm->escape ( $catalog ) ;
        $catalogs = [] ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()) $catalogs[$o->catalog][] = $o->q ;

        $dbwd = $this->mnm->openWikidataDB() ;
        foreach ( $catalogs AS $cat => $qs ) {
            $qs = implode ( ',' , $qs ) ;
            $sql = "SELECT /* ".__METHOD__." 2 */ ips_site_id,count(*) AS cnt FROM wb_items_per_site WHERE ips_item_id IN ($qs) GROUP BY ips_site_id" ;
            $result = $this->mnm->tfc->getSQL ( $dbwd , $sql , 5 ) ;
            while($o = $result->fetch_object()){
                $out['data'][$o->ips_site_id][$cat] = $o->cnt ;
            }
        }
    }

    protected function query_get_common_names ( &$out ) {
        $catalog = $this->get_catalog();
        $limit = $this->get_request_int ( 'limit' , 50 ) ;
        $offset = $this->get_request_int ( 'offset' , 0 ) ;
        $min = $this->get_request_int ( 'min' , 3 ) ;
        $max = $this->get_request_int ( 'max' , 15 ) + 1 ;
        $type_q = $this->get_request ( 'type' , '' ) ;
        $other_cats_desc = $this->get_request_int ( 'other_cats_desc' ) ;
        if ( !preg_match('|^Q\d+$|',$type_q) ) $type_q = ''; # Paranoia

        $cond1 = $other_cats_desc ? " AND e2.ext_desc!=''" : '' ;
        $not_like = "ext_name NOT LIKE '_. %' AND ext_name NOT LIKE '%?%' AND ext_name NOT LIKE '_ %'" ;
        $sql = "SELECT /* ".__METHOD__." */ (SELECT count(*) FROM entry e2 WHERE e1.ext_name=e2.ext_name {$cond1}) AS cnt,e1.* FROM entry e1 WHERE catalog={$catalog} AND q IS NULL AND {$not_like}";
        if ( $type_q!='' ) $sql .= " AND `type`='{$type_q}'"; # Safe
        $sql .= " HAVING cnt>{$min} AND cnt<{$max} LIMIT {$limit} OFFSET {$offset}" ;
        $this->add_sql_to_out ( $out , $sql , 'entries' , 'id' ) ;
    }

    protected function query_get_wd_props ( &$out ) {
        $props = [] ;
        $sql = "SELECT /* ".__METHOD__." */ DISTINCT wd_prop FROM catalog WHERE wd_prop!=0 AND wd_prop IS NOT NULL AND wd_qual IS NULL AND active=1 ORDER BY wd_prop" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while ( $o = $result->fetch_object() ) $props[] = $o->wd_prop*1 ;
        $out = $props ;
    }

    # Might not be in use
    protected function query_same_names ( &$out ) {
        $out['data']['entries'] = [] ;
        $sql = "SELECT /* ".__METHOD__." 1 */ ext_name,count(*) AS cnt,SUM(if(q IS NOT NULL OR q=0, 1, 0)) AS matched FROM entry " ;
        if ( rand(0,10) > 5 ) $sql .= " WHERE ext_name>'M' " ; // Hack to get more results
        $sql .= " GROUP BY ext_name HAVING cnt>1 AND cnt<10 AND matched>0 AND matched<cnt LIMIT 10000" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        $tmp = [] ;
        while($o = $result->fetch_object()) $tmp[] = $o ;

        $ext_name = $tmp[array_rand($tmp)]->ext_name ;
        $out['data']['name'] = $ext_name ;

        $sql = "SELECT /* ".__METHOD__." 2 */ * FROM entry WHERE ext_name='" . $this->mnm->escape($ext_name) . "'" ;
        $this->add_entries_and_users_from_sql ( $out , $sql , false ) ;
    }

    protected function query_top_missing ( &$out ) {
        $catalogs = $this->get_request ( 'catalogs' , '' ) ;
        $catalogs = preg_replace ( '/[^0-9,]/' , '' , $catalogs ) ;
        if ( $catalogs == '' ) throw new \Exception("No catalogs given") ;
        $this->add_sql_to_out ( $out , "/*top_missing*/ SELECT ext_name,count(DISTINCT catalog) AS cnt FROM entry WHERE catalog IN ({$catalogs}) AND (q IS NULL or user=0) GROUP BY ext_name HAVING cnt>1 ORDER BY cnt DESC LIMIT 500" ) ;
    }

    # Might not be in use
    protected function query_disambig ( &$out ) {
        $catalog = $this->get_request ( 'catalog' , 0 ) ; # Empty is an option; TODO 0?
        if ( $catalog != '' ) $catalog *= 1 ;

        $qs = '' ;
        $sql = "SELECT /* ".__METHOD__." 1 */ DISTINCT q FROM entry WHERE q IS NOT NULL and q>0 and user!=0" ;
        if ( $catalog != '' ) $sql .= " AND catalog=$catalog" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()) {
            if ( $qs != '' ) $qs .= "," ;
            $qs .= "'Q{$o->q}'" ;
        }
        $out['data']['qs'] = count($qs) ; # TODO count of string?

        $out['data']['entries'] = [] ;
        if ( $qs == '' ) return ; # No candidates

        $sql = "SELECT /* ".__METHOD__." 2 */ DISTINCT page_title FROM page,pagelinks,linktarget WHERE pl_target_id=lt_id AND page_namespace=0 AND page_title IN ($qs) and pl_from=page_id AND lt_namespace=0 AND lt_title='Q4167410' ORDER BY rand() LIMIT 50" ;

        $qs = [] ;
        $dbwd = $this->mnm->openWikidataDB() ;
        $result = $this->mnm->tfc->getSQL ( $dbwd , $sql , 5 ) ;
        while($o = $result->fetch_object()) $qs[] = $o->epp_entity_id ;
        if ( count($qs) == 0 ) return ; # No candidates

        $sql = "SELECT /* ".__METHOD__." 3 */ * FROM entry WHERE q IN (" . implode(',',$qs) . ") and user!=0" ;
        $this->add_entries_and_users_from_sql ( $out , $sql , false ) ;
    }

    protected function query_locations ( &$out ) {
        $bbox = $this->get_request ( 'bbox' , '' ) ;
        $bbox = preg_replace ( '/[^0-9,\.\-]/' , '' , $bbox ) ;
        $bbox = explode ( ',' , $bbox ) ;
        $out['bbox'] = $bbox ;
        if ( count($bbox) != 4 ) throw new \Exception("Required parameter bbox does not have 4 comma-separated numbers") ;
        $sql = "SELECT /* ".__METHOD__." */ entry.*,location.entry_id,location.lat,location.lon FROM entry,location WHERE location.entry_id=entry.id" ;
        $sql .= " AND lon>={$bbox[0]} AND lon<={$bbox[2]} AND lat>={$bbox[1]} AND lat<={$bbox[3]} LIMIT 5000" ;
        $this->add_sql_to_out ( $out , $sql ) ;
    }

    protected function query_get_catalog_info ( &$out ) {
        $catalog_id = $this->get_catalog();
        $sql = "SELECT /* ".__METHOD__." */ * FROM catalog WHERE id={$catalog_id}" ;
        $this->add_sql_to_out ( $out , $sql ) ;
    }

    # Likely not used anymore, superseded by query_download2()
    protected function query_download ( &$out ) {
        $out = '' ;
        $catalog = $this->get_catalog();
        $filename = '' ;
        $result = $this->mnm->getSQL ( "SELECT /* ".__METHOD__." 1 */ * FROM catalog WHERE id=$catalog" ) ;
        while($o = $result->fetch_object()) $filename = str_replace ( ' ' , '_' , $o->name ) . ".tsv" ;
        $users = [] ;
        $result = $this->mnm->getSQL ( "SELECT /* ".__METHOD__." 2 */ * FROM user" ) ;
        while($o = $result->fetch_object()) $users[$o->id] = $o->name ;
        $this->content_type = self::CONTENT_TYPE_TEXT_PLAIN ;
        $this->headers[] = 'Content-Disposition: attachment;filename="' . $filename . '"';
        $out = "Q\tID\tURL\tName\tUser\n" ;
        $sql = "SELECT /* ".__METHOD__." 3 */ * FROM entry WHERE catalog=$catalog AND q IS NOT NULL AND q > 0 AND user!=0" ;
        $result = $this->mnm->getSQL ( $sql ) ;
        while($o = $result->fetch_object()){
            $user = isset($o->user) ? $users[$o->user] : '' ;
            $out .= "{$o->q}\t{$o->ext_id}\t{$o->ext_url}\t{$o->ext_name}\t{$user}\n" ;
        }
    }

    protected function query_download2 ( &$out ) {
        $catalogs = preg_replace ( '/[^0-9,]/' , '' , $this->get_request ( 'catalogs' , '' ) ) ;
        $format = $this->get_request ( 'format' , 'tab' ) ;
        $columns = (object) json_decode($this->get_request('columns','{}')) ;
        $hidden = (object) json_decode($this->get_request('hidden','{}')) ;
        $as_file = $this->get_request ( 'as_file' , 0 ) ;


        $sql = 'SELECT /* ".__METHOD__." */
 entry.id AS entry_id,entry.catalog,ext_id AS external_id' ;
		if ( $columns->exturl ) $sql .= ',ext_url AS external_url,ext_name AS `name`,ext_desc AS description,`type` AS entry_type,entry.user AS mnm_user_id' ;
		$sql .= ',(CASE WHEN q IS NULL THEN NULL else concat("Q",q) END) AS q' ;
		$sql .= ',`timestamp` AS matched_on' ;

		if ( $columns->username ) $sql .= ',user.name AS matched_by_username' ;
		if ( $columns->aux ) $sql .= ',(SELECT group_concat(concat("{`P",aux_p,"`,`",aux_name,"`,`",in_wikidata,"`}") separator "|") FROM auxiliary WHERE auxiliary.entry_id=entry.id GROUP BY auxiliary.entry_id) AS auxiliary_data' ;
		if ( $columns->dates ) $sql .= ',person_dates.born,person_dates.died,person_dates.in_wikidata AS dates_in_wikidata' ;
		if ( $columns->location ) $sql .= ',location.lat,location.lon' ;
		if ( $columns->multimatch ) $sql .= ',multi_match.candidates AS multi_match_candidates' ;

		$sql .= ' FROM entry' ;

		if ( $columns->dates ) $sql .= ' LEFT JOIN person_dates ON (entry.id=person_dates.entry_id)' ;
		if ( $columns->location ) $sql .= ' LEFT JOIN location ON (entry.id=location.entry_id)' ;
		if ( $columns->multimatch ) $sql .= ' LEFT JOIN multi_match ON (entry.id=multi_match.entry_id)' ;
		if ( $columns->username ) $sql .= ' LEFT JOIN user ON (entry.user=user.id)' ;

		$sql .= " WHERE entry.catalog IN ({$catalogs})" ;
		if ( $hidden->any_matched ) $sql .= " AND entry.q IS NULL" ;
		if ( $hidden->firmly_matched ) $sql .= " AND (entry.q IS NULL OR entry.user=0)" ;
		if ( $hidden->user_matched ) $sql .= " AND (entry.user IS NULL OR entry.user IN (0,3,4))" ;
		if ( $hidden->unmatched ) $sql .= " AND entry.q IS NOT NULL" ;
		if ( $hidden->no_multiple ) $sql .= " AND EXISTS (SELECT * FROM multi_match WHERE entry.id=multi_match.entry_id)" ;
		if ( $hidden->name_date_matched ) $sql .= " AND entry.user!=3" ;
		if ( $hidden->automatched ) $sql .= " AND entry.user!=0" ;
		if ( $hidden->aux_matched ) $sql .= " AND entry.user!=4" ;

		if ( $format != 'json' ) $this->content_type = self::CONTENT_TYPE_TEXT_PLAIN ;

		if ( $as_file ) {
			$filename = "mix-n-match.{$catalogs}." . date('YmdHis') . ".{$format}" ;
			$this->headers[] = 'Content-Disposition: attachment;filename="' . $filename . '"' ;
		}

		$out = '' ;

		$first_row = true ;

		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_assoc()){
			if ( $first_row ) {
				if ( $format == 'tab' ) $out .= '#' . implode ( "\t" , array_keys ( $o ) ) . "\n" ;
				if ( $format == 'json' ) $out .= "[\n" ;
			} else {
				if ( $format == 'json' ) $out .= ",\n" ;
			}
			$first_row = false ;

			if ( $format == 'json' ) {
				$out .= json_encode ( $o ) ;
			} else { # Default: tab
				$p = [] ;
				foreach ( $o AS $k => $v ) $p[] = preg_replace ( '/\s/' , ' ' , $v ) ; # Ensure no tabs/newlines in value
				$out .= implode ( "\t" , $p ) . "\n" ;
			}
		}

if ( $first_row ) {
// Nothing was written
			if ( $format == 'json' ) $out .= "[\n" ;
		}

		if ( $format == 'json' ) $out .= "\n]" ;
		}

	protected function query_redirect ( &$out ) {
		$catalog = $this->get_catalog();
		$ext_id = $this->get_escaped_request ( 'ext_id' , '' ) ;
		$url = '' ;
		$sql = "SELECT /* ".__METHOD__." */ ext_url FROM entry WHERE catalog=$catalog AND ext_id='{$ext_id}'" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $url = $o->ext_url ;
		$this->prevent_callback = true ;
		$this->content_type = self::CONTENT_TYPE_TEXT_HTML ;
		$out = '<html><head><META http-equiv="refresh" content="0;URL='.$url.'"></head><body></body></html>' ;
	}

	protected function query_proxy_entry_url ( &$out ) {
		$entry_id = $this->get_request ( 'entry_id' , '' ) ;
		$entry = $this->get_entry_object_from_id ( $entry_id ) ;
		if ( !isset($entry) ) throw new \Exception("No such entry ID '{$entry_id}'") ;
		$this->prevent_callback = true ;
		$this->content_type = self::CONTENT_TYPE_TEXT_HTML ;
		$out = file_get_contents ( $entry->ext_url ) ;
	}

	protected function query_get_property_cache ( &$out ) {
		$out['data']['prop2item'] = [] ;
		$out['data']['item_label'] = [] ;

		$sql = "SELECT /* ".__METHOD__." */ DISTINCT prop_group,property,item FROM `property_cache` WHERE `property` in (SELECT DISTINCT `wd_prop` FROM catalog WHERE `active`=1 AND `wd_prop` IS NOT NULL AND `wd_qual` IS NULL)" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) {
			$out['data']['prop2item']["{$o->prop_group}"][] = [$o->property*1,$o->item*1] ;
		}

		$sql = "SELECT /* ".__METHOD__." */ DISTINCT `item`,`label` FROM `property_cache`" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $out['data']['item_label']["{$o->item}"] = $o->label ;

	}

	protected function query_quick_compare_list ( &$out ) {
		$out['data'] = [];
		$sql = "SELECT * FROM vw_catalogs_for_quick_compare";
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) {
			$out['data'][] = $o;
		}
	}

	protected function query_quick_compare ( &$out , $retries_left=3 ) {
		$catalog_id = $this->get_request_int ( 'catalog' , 0 ) * 1 ;
		$entry_id = $this->get_request_int ( 'entry_id' , 0 ) ;
		$require_image = $this->get_request_int ( 'require_image' , 0 ) == 1 ;
		$require_coordinates = $this->get_request_int ( 'require_coordinates' , 0 ) == 1 ;

		$out['data'] = ['entries'=>[]] ;

		$max_distance_m = PHP_INT_MAX;
		if ( $catalog_id>0 ) {
			$catalog = new Catalog ( $catalog_id , $this->mnm ) ;
			$image_pattern = $catalog->data()->image_pattern??'';

			# Determinate max distance
			if ( isset($catalog->data()->location_distance) ) {
				if ( preg_match('|^(\d+)m$',$catalog->data()->location_distance,$m) ) $max_distance_m = $m[1]*1;
				if ( preg_match('|^(\d+)km$',$catalog->data()->location_distance,$m) ) $max_distance_m = $m[1]*1000;
			}
		}
		$max_distance_m = $this->get_request_int ( 'max_distance_m' , $max_distance_m );
		$out['max_distance_m'] = $max_distance_m;


		# Auto-matches
		$r = rand()/getrandmax();
		$max_results = 10;
		$sql = "SELECT entry.*,catalog.search_wp AS language";
		if ( $require_image ) $sql .= ",kv1.kv_value image_url";
		if ( $require_coordinates ) $sql .= ",lat,lon";
		$sql .= " FROM entry,catalog ";
		if ( $require_image ) $sql .= ",kv_entry kv1 ";
		if ( $require_coordinates ) $sql .= ",location ";
		if ( $entry_id>0 ) $sql .= "WHERE entry.id={$entry_id} AND catalog.id=entry.catalog"; # Live testing
		else {
			$sql .= "WHERE catalog.id=entry.catalog AND catalog.active=1 AND user=0 ";
			if ( $catalog_id>0 ) $sql .= "AND entry.catalog={$catalog_id} ";
			else $sql .= "AND entry.catalog NOT IN (819) "; # HARDCODED catalogs with simple automatches
			if ( $retries_left>1 ) $sql .= "AND random>={$r} " ;
			if ( $require_image ) $sql .= "AND kv1.entry_id=entry.id AND kv1.kv_key='image_url' AND kv1.kv_value!='' " ;
			if ( $require_coordinates ) $sql .= "AND location.entry_id=entry.id " ;
			if ( $retries_left>1 ) $sql .= "ORDER BY random ";
			$sql .= "LIMIT {$max_results}" ;
		}
		$out['sql'] = $sql;
		if ( $catalog_id==0) return;
		$items = [];
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) {
			$out['data']['entries'][$o->id] = $o;
			if ( isset($o->q) ) $items[] = 'Q'.$o->q;
		}

		# Get auto-matched items
		$wil = new \WikidataItemList;
		$wil->loadItems ( $items ) ;
		$valid_q = [];
		foreach ( $items as $q ) {
			$i = $wil->getItem($q);
			if ( !isset($i) ) continue;
			if ( $require_image and !$i->hasClaims('P18') ) continue;
			if ( $require_coordinates and !$i->hasClaims('P625') ) continue;
			$valid_q[$q] = $q ;
		}
		$this->add_extended_entry_data($out);

		# Remove entries without valid auto-match
		foreach ( $out['data']['entries'] AS $entry_id => $v ) {
			if ( $require_coordinates && !isset($v->lat) ) {
				unset($out['data']['entries'][$entry_id]);
				continue ;
			}
			$q = 'Q'.$v->q;
			if ( !isset($valid_q[$q]) ) {
				unset($out['data']['entries'][$entry_id]);
				continue ;
			}
			$i = $wil->getItem($q);
			$out['data']['entries'][$entry_id]->item = [
				'q' => $i->getQ(),
				'label' => $i->getLabel($v->language),
				'description' => $i->getDesc($v->language),
			];
			$claims = $i->getClaims('P625');
			if ( count($claims)>0 ) {
				$claim = $claims[0];
				if ( isset($claim->mainsnak) and isset($claim->mainsnak->datavalue) and isset($claim->mainsnak->datavalue->value) ) {
					$lat_item = $claim->mainsnak->datavalue->value->latitude*1;
					$lon_item = $claim->mainsnak->datavalue->value->longitude*1;
					$lat_entry = $v->lat*1;
					$lon_entry = $v->lon*1;
					$out['data']['entries'][$entry_id]->item['coordinates'] = ['lat'=>$lat_item,'lon'=>$lon_item];
					$distance_m = $this->getDistanceInMeters($lat_item,$lon_item,$lat_entry,$lon_entry);
					if ( $distance_m > $max_distance_m ) {
						unset($out['data']['entries'][$entry_id]);
						continue ;
					}
					$out['data']['entries'][$entry_id]->distance_m = $distance_m;
				}
			}

			# Item image
			$item_image = $i->getFirstString('P18');
			if ( $item_image!='' ) $out['data']['entries'][$entry_id]->item['image'] = $item_image;

			# Entry image
			if ( isset($v->image_url) ) {
				$image_url = $v->image_url;
				if ( !isset($image_url) or $image_url=='' ) {
					if ( $require_image ) {
						unset($out['data']['entries'][$entry_id]);
						continue;
					}
				} else {
					$out['data']['entries'][$entry_id]->ext_img = $image_url;
				}
			}
		}

		if ( count($out['data']['entries'])==0 ) {
			if ( $retries_left>0 ) return $this->query_quick_compare($out,$retries_left-1);
			$out['status'] = "No results found";
		}
	}


	protected function getDistanceInMeters($lat1, $lon1, $lat2, $lon2) {
	    $theta = $lon1 - $lon2;
	    $dist = sin(deg2rad($lat1)) * sin(deg2rad($lat2)) +  cos(deg2rad($lat1)) * cos(deg2rad($lat2)) * cos(deg2rad($theta));
	    $dist = acos($dist);
	    $dist = rad2deg($dist);
	    $miles = $dist * 60 * 1.1515;
	    $meters = $miles * 1609.34;
	    return $meters;
	}

	protected function query_mnm_unmatched_relations ( &$out ) {
		$out['data']['entries'] = [] ;
		$property = $this->get_request_int ( 'property' , 0 ) ;
		$offset = $this->get_request_int ( 'offset' , 0 ) ;
		$limit = 25;
		$prop_sql = '';
		if ( $property>0 ) $prop_sql = "AND property={$property}";
		$sql = "SELECT /* query_mnm_unmatched_relations */ entry.id,count(*) AS cnt
			FROM mnm_relation,entry
			WHERE target_entry_id=entry.id
			{$prop_sql}
			AND (q is null or user=0)
			GROUP BY entry.id
			ORDER BY cnt DESC
			LIMIT {$limit}
			OFFSET {$offset}";
		$entry_ids = [];
// $out['sql1'] = $sql;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) {
			$entry_ids[] = $o->id;
			$out['data']['entry2cnt'][$o->id] = $o->cnt;
		}
		if ( count($entry_ids)==0 ) return;

		$out['data']['entry_order'] = $entry_ids;
		$sql = "SELECT /* query_mnm_unmatched_relations */ * FROM entry WHERE id IN (".implode(',',$entry_ids).")" ;
// $out['sql2'] = $sql;
		$this->add_entries_and_users_from_sql ( $out , $sql ) ;
		$this->add_extended_entry_data($out) ;
	}

	protected function query_get_code_fragments ( &$out ) {
		$catalog = $this->get_catalog();
		$out['data'] = ['user_allowed'=>0,'all_functions'=>[]] ;
		$username = $this->get_request ( 'username' , '' ) ;
if ( $username != '' ) {
// TODO check user name
			$user_id = $this->mnm->getOrCreateUserID ( $username ) ;
			$out['data']['user_allowed'] = in_array($user_id, $this->code_fragment_allowed_user_ids) ? 1 : 0 ;
		}
		$this->add_sql_to_out ( $out , "SELECT /* ".__METHOD__." 1 */ * FROM `code_fragments` WHERE `catalog`={$catalog} ORDER BY `function`" , 'fragments' ) ;
		$result = $this->mnm->getSQL ( "SELECT /* ".__METHOD__." 2 */ DISTINCT `function` FROM `code_fragments`" ) ;
		while($o = $result->fetch_object()) $out['data']['all_functions'][] = $o->function ;
	}

	protected function query_save_code_fragment ( &$out ) {
		$username = $this->get_request ( 'username' , '' ) ;
		$user_id = $this->check_and_get_user_id ( $username ) ;
		if ( !in_array($user_id, $this->code_fragment_allowed_user_ids) ) throw new \Exception("Not allowed, ask Magnus") ;
		$fragment = json_decode ( $this->get_request ( 'fragment' , '{}' ) ) ;

		$catalog = $fragment->catalog * 1 ;
		if ( $catalog <= 0 ) return ;
		$cfid = $this->mnm->saveCodeFragment ( $fragment , $catalog ) ;
		if ( $fragment->function == 'PERSON_DATE' ) {
			$job_id = $this->mnm->queue_job($catalog,'update_person_dates');
			$this->mnm->queue_job($catalog,'match_person_dates',$job_id);
		} else if ( $fragment->function == 'AUX_FROM_DESC' ) {
			$this->mnm->queue_job($catalog,'generate_aux_from_description');
		} else if ( $fragment->function == 'DESC_FROM_HTML' ) {
			$this->mnm->queue_job($catalog,'update_descriptions_from_url');
		}
	}

	protected function query_test_code_fragment ( &$out ) {
		$out['data'] = [] ;
		$username = $this->get_request ( 'username' , '' ) ;
		$user_id = $this->check_and_get_user_id ( $username ) ;
		if ( !in_array($user_id, $this->code_fragment_allowed_user_ids) ) throw new \Exception("Not allowed, ask Magnus") ;
		$entry_id = $this->get_request_int ( 'entry_id' ) ;
		if ( $entry_id <= 0 ) throw new \Exception("No entry_id") ;
		$entry = $this->get_entry_object_from_id ( $entry_id ) ;
		$fragment = json_decode ( $this->get_request ( 'fragment' , '{}' ) ) ;

		if ( $fragment->function == 'DESC_FROM_HTML' ) $test_harness = new HTMLtoDescription ( $entry->catalog , $this->mnm ) ;
		else if ( $fragment->function == 'PERSON_DATE' ) $test_harness = new PersonDates ( $entry->catalog , $this->mnm ) ;
		else if ( $fragment->function == 'AUX_FROM_DESC' ) $test_harness = new DescriptionToAux ( $entry->catalog , $this->mnm ) ;
		else if ( $fragment->function == 'BESPOKE_SCRAPER' ) $test_harness = new BespokeScraper ( $entry->catalog , $this->mnm ) ;
		else throw new \Exception("Bad fragment function '{$fragment->function}'") ;
		$fragment->success = true ;
		$fragment->is_active = true ;
		if ( isset($fragment->json) and $fragment->json !== null and $fragment->json != '' ) $fragment->json = json_decode ( $fragment->json ) ;
		else $fragment->json = json_decode ( '{}' ) ;

		$test_harness->setCodeFragment ( $fragment ) ;
		if ( $fragment->function == 'BESPOKE_SCRAPER' ) $entry = (object) [ 'id' => $entry->ext_id , 'url' => $entry->ext_url , 'catalog' => $entry->catalog ] ;
		$commands = $test_harness->processEntry ( $entry ) ;
		$out['data'] = json_decode ( json_encode ( $commands ) ) ;
	}

	protected function query_all_issues ( &$out ) {
		$mode = $this->get_request ( 'mode' , '' ) ;
		if ( !in_array($mode,['duplicate_items','mismatched_items','time_mismatch']) ) throw new \Exception("Unsupported mode") ;
		$out['data'] = [];
		$table = "vw_issues_".$mode ;
		$sql = "SELECT * FROM {$table}" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) {
			$o->$mode = json_decode($o->$mode);
			$out['data'][] = $o;
		}
	}

	protected function query_suggest ( &$out ) {
		$this->content_type = self::CONTENT_TYPE_TEXT_PLAIN ;
		$out = '' ;
		$ts = date ( 'YmdHis' ) ;
		$catalog = $this->get_request_int('catalog') ;
		$overwrite = $this->get_request_int('overwrite') ;
		$suggestions = $this->get_request ( 'suggestions' , '' ) ;
		$suggestions = explode ( "\n" , $suggestions ) ;
		$cnt = 0 ;
		foreach ( $suggestions AS $s ) {
			if ( trim($s) == '' ) continue ;
			$s = explode ( '|' , $s ) ;
			if ( count($s) != 2 ) {
				$out .= "Bad row : " . implode('|',$s) . "\n" ;
				continue ;
			}
			$extid = trim ( $s[0] ) ;
			$q = preg_replace ( '/\D/' , '' , $s[1] ) ;
			$sql = "UPDATE entry SET q=$q,user=0,`timestamp`='$ts' WHERE catalog=$catalog AND ext_id='" . $this->mnm->escape($extid) . "'" ;
			if ( $overwrite == 1 ) $sql .= " AND (user=0 OR q IS NULL)" ;
			else $sql .= " AND (q IS NULL)" ;
			$result = $this->mnm->getSQL ( $sql ) ;
			$cnt += $this->mnm->dbm->affected_rows ;
		}
		$out .= "$cnt entries changed" ;
	}

	protected function query_random ( &$out ) {
		$catalogs = [] ;
		$sql = "SELECT /* ".__METHOD__." 1 */ `id` FROM `catalog` WHERE `active`=1" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $catalogs[$o->id] = $o->id ;

		$submode = $this->get_request ( 'submode' , '' ) ;
		$catalog = $this->get_request_int ( 'catalog' ) ;
		$id = $this->get_request_int ( 'id' ) ; # For testing
		$type = $this->mnm->escape ( $this->get_request ( 'type' , '' ) ) ;

		$noq_in_catalog = 0 ;
		if ( $catalog > 0 ) {
			$sql = "SELECT /* ".__METHOD__." 2 */ * FROM overview WHERE catalog={$catalog}" ;
			$result = $this->mnm->getSQL ( $sql ) ;
			while($o = $result->fetch_object()) $noq_in_catalog = $o->noq ;
		}

		$cnt = 0 ;
		$fail = 0 ;
		unset($out['data']);
		while ( 1 ) {
			if ( $fail ) break ; # No more results
			$r = $this->mnm->rand() ;
			if ( $cnt > 10 ) {
				$r = 0 ;
				$fail = 1 ;
			}
			$sql = "SELECT /* ".__METHOD__." */ * FROM entry" ;
			#if ( $noq_in_catalog > 2000 )
			$sql .= " FORCE INDEX (random_2)" ;
			$sql .= " WHERE random>=$r " ;
			if ( $submode == 'prematched' ) $sql .= " AND user=0" ;
			else if ( $submode == 'no_manual' ) $sql .= " AND ( user=0 or q is null )" ;
else $sql .= " AND q IS NULL" ; // Default: unmatched
			if ( $catalog > 0 ) $sql .= " AND catalog=$catalog" ;
			if ( $type != '' ) $sql .= " AND `type`='$type'" ;
			$sql .= " ORDER BY random" ;
			if ( $catalog == 0 ) $sql .= " LIMIT 10" ;
			else $sql .= " LIMIT 1" ;
			if ( $id!=0 ) $sql = "SELECT * FROM entry WHERE id=" . ($_REQUEST['id']*1) ; # For testing
			if ( $this->testing ) {
				$out['sql'] = $sql ;
				break ;
			}
			$result = $this->mnm->getSQL ( $sql ) ;
			while($o = $result->fetch_object()) {
if ( !isset($catalogs[$o->catalog]) ) continue ; // Make sure catalog is active
				$out['data'] = $o ;
				break ;
			}
			if ( isset ( $out['data'] ) ) break ;
			$cnt++ ;
		}

		if ( isset($out['data']) ) {
			$id = $out['data']->id ;
			$sql = "SELECT /* ".__METHOD__." */ * FROM person_dates WHERE entry_id=$id" ;
			$result = $this->mnm->getSQL ( $sql ) ;
			while($o = $result->fetch_object()){
				if ( $o->born != '' ) $out['data']->born = $o->born ;
				if ( $o->died != '' ) $out['data']->died = $o->died ;
			}
		}
	}

	protected function query_get_entries_by_q_or_value ( &$out ) {
		$q = $this->get_request ( 'q' , '' ) ;
		$json = (array) json_decode ( $this->get_request ( 'json' , '[]' ) ) ;
		$sql_parts = [ "q=" . preg_replace('/\D/','',$q) ] ;

		if ( count($json) > 0 ) {
			$props = implode ( ',' , array_keys($json) ) ;
			$props = preg_replace ( '/[^0-9,]/' , '' , $props ) ;
			$sql = "SELECT /* ".__METHOD__." */ id,wd_prop FROM catalog WHERE wd_qual IS NULL AND active=1 AND wd_prop IN ($props)" ;
			$prop2catalog = [] ;
			$result = $this->mnm->getSQL ( $sql ) ;
			while ( $o = $result->fetch_object() ) $prop2catalog['P'.$o->wd_prop][] = $o->id ;
			foreach ( $json AS $prop => $values ) {
				if ( count($values) == 0 ) continue ;
				if ( !isset($prop2catalog[$prop]) or count($prop2catalog[$prop]) == 0 ) continue ;
				foreach ( $values AS $k => $v ) $values[$k] = $this->mnm->escape ( $v ) ;
				$sql_parts[] = "catalog IN (".implode(',',$prop2catalog[$prop]).") AND ext_id IN ('".implode("','",$values)."')" ;
			}
		}

		$sql = "SELECT /* ".__METHOD__." 1 */  * FROM entry WHERE ((" . implode ( ') OR (' , $sql_parts ) . '))' ;
		$sql .= ' AND catalog NOT IN (SELECT id FROM catalog WHERE active!=1) ORDER BY catalog,id' ;
		$this->add_sql_to_out ( $out , $sql , 'entries' , 'id' ) ;
		if ( count($out['data']['entries']) == 0 ) return ;

		$catalog = [] ;
		foreach ( $out['data']['entries'] AS $e ) $catalogs[$e->catalog] = 1 ;
		$sql = "SELECT /* ".__METHOD__." 2 */ * FROM catalog WHERE id IN (" . implode(',',array_keys($catalogs)) . ")" ;
		$this->add_sql_to_out ( $out , $sql , 'catalogs' , 'id' ) ;

		$description_aux = [] ;
		$sql = "SELECT /* ".__METHOD__." 3 */ * FROM `description_aux`" ;
		$this->add_sql_to_out ( $out , $sql , 'description_aux' , 'id' ) ;
	}

	protected function query_missingpages ( &$out ) {
		$out['data'] = ['entries'=>[],'users'=>[]] ;
		$catalog = $this->get_catalog();
		$site = trim ( $this->get_escaped_request ( 'site' , '' ) ) ;
		if ( $site == '' ) throw new \Exception("site parameter required") ;

		$sql = "SELECT /* query_missingpages 1 */ DISTINCT q FROM entry WHERE q>0 AND user>0 AND catalog=$catalog AND q IS NOT NULL" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $qs[''.$o->q] = $o->q ;

		if ( count($qs) > 0 ) {
			$dbwd = $this->mnm->openWikidataDB() ;
			$sql = "SELECT /* query_missingpages 2 */ DISTINCT ips_item_id FROM wb_items_per_site WHERE ips_item_id IN (" . implode(',',$qs) . ") AND ips_site_id='$site'" ;
			$result = $this->mnm->tfc->getSQL ( $dbwd , $sql , 5 ) ;
			while($o = $result->fetch_object()) unset ( $qs[''.$o->ips_item_id] ) ;
		}
		if ( count($qs) == 0 ) return ;

		$sql = "SELECT /* query_missingpages 3 */ * FROM entry WHERE user>0 AND catalog=$catalog AND q IN (" . implode(',',$qs) . ")" ;
		$this->add_entries_and_users_from_sql ( $out , $sql ) ;
	}

	protected function query_catalog ( &$out ) {
		$catalog = $this->get_catalog() ;
		$entry = $this->get_request ( 'entry' , -1 ) ;
		$meta = json_decode ( $this->get_request ( 'meta' , '{}' ) ) ;
		if ( !is_object($meta) ) throw new \Exception("meta needs to be a JSON object") ;
		if ( !isset($meta->show_nowd) ) $meta->show_nowd = 0 ;

		$sql = "SELECT /* query_catalog */ * FROM entry WHERE catalog={$catalog}" ;
		if ( $meta->show_multiple == 1 ) {
			$sql .= " AND EXISTS ( SELECT * FROM multi_match WHERE entry_id=entry.id ) AND ( user<=0 OR user is null )" ;
		} else if ( $meta->show_noq+$meta->show_autoq+$meta->show_userq+$meta->show_nowd == 0 and $meta->show_na == 1 ) {
			$sql .= " AND q=0" ;
		} else if ( $meta->show_noq+$meta->show_autoq+$meta->show_userq+$meta->show_na == 0 and $meta->show_nowd == 1 ) {
			$sql .= " AND q=-1" ;
		} else {
			if ( $meta->show_noq != 1 ) $sql .= " AND q IS NOT NULL" ;
			if ( $meta->show_autoq != 1 ) $sql .= " AND ( q is null OR user!=0 )" ;
			if ( $meta->show_userq != 1 ) $sql .= " AND ( user<=0 OR user is null )" ;
			if ( $meta->show_na != 1 ) $sql .= " AND ( q!=0 or q is null )" ;
//			if ( $meta->show_nowd != 1 ) $sql .= " AND ( q=-1 )" ;
		}

		if ( isset($_REQUEST['type']) ) $sql .= " AND `type`='" . $this->mnm->escape($_REQUEST['type']) . "'" ;
		if ( isset($_REQUEST['title_match']) ) $sql .= " AND `ext_name` LIKE '%" . $this->mnm->escape($_REQUEST['title_match']) . "%'" ;
		$sql .= " LIMIT " . $this->mnm->escape(''.($meta->per_page??50)) ;
		$sql .= " OFFSET " . $this->mnm->escape(''.($meta->offset??0)) ;

		$this->add_entries_and_users_from_sql ( $out , $sql ) ;
		$this->add_extended_entry_data($out) ;
	}

	protected function query_get_entry ( &$out ) {
		$catalog = $this->get_request_int ( 'catalog' ) ; # Optional
		$entry_ids = $this->get_request ( 'entry' , '' ) ; # Optional
		$sql = "SELECT /* query_get_entry */ * FROM entry WHERE " ;
		$ext_ids = $this->get_request ( 'ext_ids' , '' ) ;
		if ( $ext_ids != '' ) {
			if ( $catalog <= 0 ) throw new \Exception("catalog is required when using ext_ids") ;
			$ext_ids = json_decode ( $ext_ids ) ;
			$x = [] ;
			foreach ( $ext_ids AS $eid ) $x[] = $this->mnm->escape($eid) ;
			$ext_ids = '"' . implode ( '","' , $x ) . '"' ;
			$sql .= "catalog={$catalog} AND ext_id IN ($ext_ids)" ;
		} else {
			$entry_ids = trim ( preg_replace ( '|[^0-9,]|' , '' , $entry_ids ) ) ;
			if ( $entry_ids == '' ) throw new \Exception("entry is required") ;
			$sql .= "id IN ({$entry_ids})" ;
		}
		$this->add_entries_and_users_from_sql ( $out , $sql ) ;
		$this->add_extended_entry_data($out) ;
	}

	protected function query_edit_catalog ( &$out ) {
		$catalog = $this->get_catalog();
		$data = json_decode ( $this->get_request ( 'data' , '' ) ) ;
		if ( !isset($data) or $data == null or !isset($data->name) ) throw new \Exception("Bad data") ;
		$username = $this->get_request ( 'username' , '' ) ;
		$username = str_replace ( '_' , ' ' , $username ) ;
		$sql = "SELECT /* query_edit_catalog 1 */ * FROM user WHERE name='".$this->mnm->escape($username)."' LIMIT 1" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		$found = ($o = $result->fetch_object()) ;
		if ( !$found ) throw new \Exception("No such user '{$username}'") ;
		if ( !$o->is_catalog_admin ) throw new \Exception("'{$username}'' is not a catalog admin") ;
		$sql = "UPDATE catalog SET " ;
		$sql .= "`name`='" . $this->mnm->escape($data->name) . "'," ;
		$sql .= "`url`='" . $this->mnm->escape($data->url) . "'," ;
		$sql .= "`desc`='" . $this->mnm->escape($data->desc) . "'," ;
		$sql .= "`type`='" . $this->mnm->escape($data->type) . "'," ;
		$sql .= "`search_wp`='" . $this->mnm->escape($data->search_wp) . "'," ;
		$sql .= "`wd_prop`=" . ((isset($data->wd_prop) and $data->wd_prop*1>0)?$data->wd_prop*1:'null') . "," ;
		$sql .= "`wd_qual`=" . ((isset($data->wd_qual) and $data->wd_qual*1>0)?$data->wd_qual*1:'null') . "," ;
		$sql .= "`active`='" . ($data->active?1:0) . "'" ;
		$sql .= " WHERE id={$catalog}" ;
		$this->mnm->getSQL ( $sql ) ;
		$this->mnm->updateCatalogs ( [$catalog] ) ;
	}

	protected function query_resolve_issue ( &$out ) {
		$issue_id = $this->get_request_int ( 'issue_id' ) ;
		if ( $issue_id <= 0 ) throw new \Exception("Bad issue ID") ;
		$user_id = $this->check_and_get_user_id ( $this->get_request('username','') ) ;
		$ts = $this->mnm->getCurrentTimestamp() ;
		$sql = "UPDATE issues SET `status`='DONE',`user_id`={$user_id},`resolved_ts`='{$ts}' WHERE id={$issue_id}" ;
		if ( !$this->mnm->getSQL($sql) ) throw new \Exception("SQL update failed") ;
	}

	protected function query_prep_new_item ( &$out ) {
		$entry_ids = $this->get_numeric_array_from_comma_separated_string ( $this->get_request('entry_ids') ) ;
		$default_entry = $this->get_request_int ( 'default_entry' ) ;
		if ( count($entry_ids) == 0 ) throw new \Exception("No entry_ids parameter") ;
		$out['default_entry'] = $default_entry ;
		$all_commands = $this->mnm->getCreateItemCommandsForEntries ( $entry_ids , null , false , $default_entry ) ;
		$out['all_commands'] = $all_commands ;
		$commands = implode ( "\n" , $all_commands ) ;
		$qs = new \QuickStatements () ;
		$qs->use_command_compression = true ;
		$commands = $qs->importData ( $commands , 'v1' , false ) ;
		$commands = $qs->compressCommands ( $commands['data']['commands'] ) ;
		$out['data'] = json_encode ( $commands[0]['data'] , JSON_HEX_QUOT|JSON_HEX_APOS ) ;
	}

	protected function query_get_flickr_key ( &$out ) {
		$out['data'] = file_get_contents("/data/project/mix-n-match/flickr.key");
	}

	protected function query_creation_candidates ( &$out ) {
		$max_tries = 250 ;
		$run_expensive_query = false ;

		$min = $this->get_request_int ( 'min' , '3' ) ;
		$mode = trim ( $this->get_request ( 'mode' , '' ) ) ;
		$ext_name_required = trim ( $this->get_request ( 'ext_name' , '' ) ) ;
		$birth_year = trim ( $this->get_request('birth_year','') ) ;
		$death_year = trim ( $this->get_request('death_year','') ) ;
		$prop = trim ( $this->get_request('prop','') ) ;
		$require_unset = $this->get_request_int ( 'require_unset' ) ;
		$require_catalogs = preg_replace ( '/[^0-9,]/' , '' , $this->get_request('require_catalogs','') ) ;
		$catalogs_required = $this->get_request_int ( 'min_catalogs_required' ) ;
		$table = 'common_names' ;
		if ( $mode != '' ) $table .= '_' .  $this->mnm->escape ( $mode ) ;

		$tries = 0 ;
		$users = [] ;
		while ( 1 ) {
			if ( $tries++ >= $max_tries ) throw new \Exception("No results after {$max_tries} attempts, giving up: {$sql}") ;
			$out['data'] = ['entries'=>[]] ;
			$users = [] ;

			if ( $ext_name_required != '' ) {
				$ext_name_safe = $this->mnm->escape ( $ext_name_required ) ;
				$sql = "SELECT /* ".__METHOD__." */ '{$ext_name_safe}' AS ext_name,20 AS cnt" ;
			} else if ( $mode == 'artwork' ) {
				$sql = "SELECT /* ".__METHOD__." */ name AS ext_name,cnt,entry_ids FROM $table WHERE " . ($min>0?" cnt>=$min":'1=1') . " ORDER BY rand() LIMIT 1" ;
			} else if ( $mode == 'dates' ) {
				$sql = "SELECT /* ".__METHOD__." */ name AS ext_name,cnt,entry_ids FROM $table WHERE " . ($min>0?" cnt>=$min":'1=1') . " ORDER BY rand() LIMIT 1" ;
			} else if ( $mode == 'birth_year' ) {
				$sql = "SELECT /* ".__METHOD__." */ name AS ext_name,cnt,entry_ids FROM $table WHERE " . ($min>0?" cnt>=$min":'1=1') . " ORDER BY rand() LIMIT 1" ;
			} else if ( $mode == 'dynamic_name_year_birth' ) {
				$r = $this->mnm->rand() ;
				$sql = "SELECT /* ".__METHOD__." */ ext_name,year_born,count(*) AS cnt,group_concat(entry_id) AS ids FROM vw_dates WHERE ext_name=(SELECT ext_name FROM entry where random>={$r} AND `type`='Q5' AND q IS NULL ORDER BY random LIMIT 1) GROUP BY year_born, ext_name HAVING cnt>=2" ;
			} else if ( $mode == 'taxon' ) {
				$sql = "SELECT /* ".__METHOD__." */ name AS ext_name,cnt FROM $table WHERE " . ($min>0?" cnt>=$min":'1=1') . " ORDER BY rand() LIMIT 1" ;
			} else if ( $mode == 'random_prop' ) {
				$r = $this->mnm->rand() ;
				if ( $prop != '' ) $prop *= 1 ;
				if ( $min < 2 ) $min = 2;
				#$sql = "select /* ".__METHOD__." */ aux_name,group_concat(entry_id) AS entry_ids,count(if(entry_is_matched=0,1,0)) as cnt from auxiliary WHERE aux_p={$prop} AND entry_is_matched=0 GROUP BY aux_name HAVING cnt>={$min} ORDER BY rand() LIMIT 1" ;
				$sql = "SELECT /* ".__METHOD__." */ aux_name,entry_ids,cnt FROM aux_candidates WHERE cnt>={$min}" ;
				if ( $prop != '' ) $sql .= " AND aux_p={$prop}" ;
				$sql .= " ORDER BY rand() LIMIT 1" ;
				$out['sqlx'] = $sql ;
			} else if ( $mode != '' ) {
				throw new \Exception("mode '{$mode}' not recognized") ;
			} else if ( $require_catalogs != '' ) {
				if ( $run_expensive_query ) $sql = "SELECT /* ".__METHOD__." */ ext_name,count(DISTINCT catalog) AS cnt FROM entry WHERE catalog IN ({$require_catalogs}) AND (q IS NULL or user=0) GROUP BY ext_name HAVING cnt>=3 ORDER BY rand() LIMIT 1" ;
				else throw new \Exception("require_catalogs but not running expensive query") ;
			} else $sql = "SELECT /* ".__METHOD__." */ name AS ext_name,cnt FROM $table WHERE " . ($min>0?" cnt>=$min AND":'') . " cnt<15 ORDER BY rand() LIMIT 1" ;

			$result = $this->mnm->getSQL ( $sql ) ;
			$tmp = [] ;
			while($o = $result->fetch_object()) $tmp[] = $o ;

			if ( count($tmp) == 0 ) continue ;

			$names = [];
			$r = array_rand($tmp) ;
			if ( isset($tmp[$r]->ext_name) ) {
				$ext_name = $tmp[$r]->ext_name ;
				$out['data']['name'] = $ext_name ;
				$names = [ $ext_name ] ;
				if ( preg_match ( '/^(\S+) (.+) (\S+)$/' , $ext_name , $m ) ) {
					$names[] = $m[1].'-'.$m[2].' '.$m[3] ;
					$names[] = $m[1].' '.$m[2].'-'.$m[3] ;
				}
				foreach ( $names AS $k => $v ) $names[$k] = $this->mnm->escape ( $v ) ;
			}

			if ( $mode == 'dates' or $mode == 'birth_year' or $mode == 'random_prop' or $mode=='artwork' ) {
				$out['x'] = $tmp[0];
				$out['y'] = $sql;
				$sql = "/*creation_candidates*/ SELECT * FROM entry WHERE id IN ({$tmp[0]->entry_ids}) AND catalog IN (SELECT id FROM catalog WHERE catalog.active=1)" ;
			} else {
				$sql = "/*creation_candidates*/ SELECT * FROM entry WHERE catalog IN (SELECT id FROM catalog WHERE catalog.active=1) AND ext_name IN ('" . implode("','",$names) . "') AND (q is null OR q!=-1)" ;
				if ( $mode == 'taxon' ) $sql .= " AND `type`='Q16521'" ;
				if ( $birth_year.$death_year!='' ) {
					$parts = [ "entry_id=entry.id" ] ;
					if ( $birth_year!='' ) $parts[] = "year_born={$birth_year}" ;
					if ( $death_year!='' ) $parts[] = "year_died={$death_year}" ;
					$sql .= " AND EXISTS (SELECT * FROM person_dates WHERE " . implode(" AND ",$parts) . ")" ;
				}
			}
			$out["sql"] = $sql ;
			$result = $this->mnm->getSQL ( $sql ) ;
			$required_catalogs_found = [] ;
			$found_unset = 0 ;
			while($o = $result->fetch_object()) {
				$out['data']['entries'][$o->id] = $o ;
				if ( isset ( $o->user ) ) $users[$o->user] = 1 ;
				if ( in_array($o->catalog, explode(',',$require_catalogs)) ) $required_catalogs_found[$o->catalog]++ ;
				if ( $o->user == 0 or !isset($o->q) or $o->q === null ) $found_unset++ ;
			}
			if ( $ext_name_required == '' ) {
				if ( $found_unset < $require_unset ) continue ;
				if ( count($required_catalogs_found) < $catalogs_required ) continue ;
			}

			if ( $min == 0 or count($out['data']['entries']) >= $min ) break ;
if ( isset($ext_name) and $ext_name != '' ) break ; // Only one possible query to run
		}

		$out['data']['users'] = $this->get_users ( $users ) ;
		$this->add_extended_entry_data($out) ;
		$out['data']['entries'] = array_values ( $out['data']['entries'] ) ;
	}

	protected function query_search ( &$out ) {
		$max_results = $this->get_request_int('max',100) ;
		$what = $this->get_request ( 'what' , '' ) ;
		$description_search = $this->get_request_int ( 'description_search' ) ;
		$no_label_search = $this->get_request_int ( 'no_label_search' ) ;
		$exclude = preg_replace ( '/[^0-9,]/' , '' , $this->get_request ( 'exclude' , '' ) ) ;
		$include = preg_replace ( '/[^0-9,]/' , '' , $this->get_request ( 'include' , '' ) ) ;

		$exclude = ( $exclude == '' ) ? []  : explode ( ',' , $exclude ) ;
		$include = ( $include == '' ) ? []  : explode ( ',' , $include ) ;
		$sql = "SELECT /* ".__METHOD__." */ id FROM catalog WHERE `active`!=1" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $exclude[] = $o->id ;
		$exclude = implode ( ',' , $exclude ) ;
		$include = implode ( ',' , $include ) ;

		$what = preg_replace ( '/[\-]/' , ' ' , $what ) ;
		if ( preg_match ( '/^\s*[Qq]{0,1}(\d+)\s*$/' , $what , $m ) ) $sql = "SELECT /* ".__METHOD__." */ * FROM entry WHERE q=".$m[1] ;
		else {
			$s = [] ;
			$what2 = explode ( " " , $what ) ;
			foreach ( $what2 AS $w ) {
				if ( in_array(trim(strtolower($w)),['the','a']) ) continue;
				$w = $this->mnm->escape ( trim ( $w ) ) ;
				if ( strlen($w)>=3 and strlen($w)<=84 ) $s[] = $w ;
			}
			$sql_parts = [] ;
			if ( !$no_label_search ) $sql_parts[] = "MATCH(ext_name) AGAINST('+".implode(",+",$s)."' IN BOOLEAN MODE)" ;
			if ( $description_search ) $sql_parts[] = "MATCH(ext_desc) AGAINST('+".implode(",+",$s)."' IN BOOLEAN MODE)" ;
			$sql = "SELECT /* ".__METHOD__." */ * FROM entry WHERE ((" . implode(") OR (",$sql_parts) . "))" ;
			if ( $exclude != '' ) $sql .= " AND catalog NOT IN ($exclude)" ;
			if ( $include != '' ) $sql .= " AND catalog IN ($include)" ;
			$sql .= " LIMIT $max_results" ;
		}
		$out['sql'] = $sql;
$this->mnm->dbm->set_charset('utf8mb4'); // Say what?!?
		$this->add_entries_and_users_from_sql ( $out , $sql ) ;
	}

	protected function query_rc ( &$out ) {
		$limit = 100 ;
		$ts = $this->get_request ( 'ts' , '' ) ;
		$catalog = $this->get_request_int ( 'catalog' ) ;
		$events = [] ;

		$sql = "SELECT /* ".__METHOD__." */ * FROM entry WHERE user!=0 AND user!=3 AND user!=4 AND timestamp IS NOT null" ;
		if ( $ts != '' ) $sql .= " AND timestamp >= '" . $this->mnm->escape($ts) . "'" ;
		if ( $catalog != 0 ) $sql .= " AND catalog={$catalog}" ;
		$sql .= " ORDER BY timestamp DESC LIMIT $limit" ;
		$min_ts = '' ;
		$max_ts = $ts ;
		$result = $this->mnm->getSQL ( $sql ) ;
		$users = [] ;
		while($o = $result->fetch_object()){
			$o->event_type = 'match' ;
			$events[$o->timestamp.'-'.$o->id] = $o ;
			if ( $min_ts == '' ) $min_ts = $o->timestamp ;
			$max_ts = $o->timestamp ;
			$users[$o->user] = 1 ;
		}

		$sql = "SELECT /* ".__METHOD__." */ entry.id AS id,catalog,ext_id,ext_url,ext_name,ext_desc,action AS event_type,log.user AS user,log.timestamp AS timestamp FROM log,entry WHERE log.entry_id=entry.id AND log.timestamp BETWEEN '$max_ts' AND '$min_ts'" ;
		if ( $catalog != 0 ) $sql .= " AND catalog={$catalog}" ;
		$out['sql'] = $sql ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()){
			$events[$o->timestamp.'-'.$o->id] = $o ;
			$users[$o->user] = 1 ;
		}
		krsort ( $events ) ;
		while ( count ( $events ) > $limit ) array_pop ( $events ) ;
		$out['data']['events'] = $events ;
		$out['data']['users'] = $this->get_users ( $users ) ;
	}

	protected function query_rc_atom ( &$out ) {
		$this->query_rc ( $out ) ;
		$this->render_atom ( $out ) ;
	}

	protected function query_get_issues ( &$out ) {
		$issue_type = trim ( strtoupper ( $this->get_request ( 'type' , '' ) ) ) ;
		$limit = $this->get_request_int ( 'limit' , 50 ) ;
		$offset = $this->get_request_int ( 'offset' ) ;
		$catalogs = preg_replace ( '/[^0-9,]/' , '' , $this->get_request ( 'catalogs' , '' ) ) ;

		if ( $catalogs != '' ) $sql_catalogs = " AND `catalog` IN ({$catalogs})" ;
		else $sql_catalogs = '' ;

		if ( $issue_type != '' ) $type_filter = " AND `type`='" . $this->mnm->escape($issue_type) . "'" ;
		else $type_filter = '';

		$sql = "SELECT /* ".__METHOD__." */ count(*) AS `cnt` FROM `issues` WHERE `status`='OPEN' {$sql_catalogs} {$type_filter}" ;
		$out['sql'] = [$sql] ;
		$result = $this->mnm->getSQL ( $sql ) ;
		$o = $result->fetch_object() ;
		$open_issues = $o->cnt * 1 ;
		if ( $open_issues == 0 ) return ; # No open issues
		$min_open_issues = $limit * 2 ;
		$out['data'] = ['open_issues'=>$open_issues,'issues'=>[],'entries'=>[]] ;
		$entries = [] ;
		while ( count($out['data']['issues']) < $limit ) {
			if ( $open_issues < $min_open_issues ) $r = 0 ;
			else $r = $this->mnm->rand() ;
			$sql = "SELECT /* ".__METHOD__." */ * FROM `issues` WHERE `status`='OPEN' AND random>={$r}" ;
			$sql .= $sql_catalogs ;
			$sql .= $type_filter ;
			$sql .= " ORDER BY `random` LIMIT {$limit} OFFSET {$offset}" ;
			$out['sql'][] = $sql ;
			$result = $this->mnm->getSQL ( $sql ) ;
			while($o = $result->fetch_object()) {
				if ( isset($o->json) and $o->json !== null and $o->json != '' ) $o->json = json_decode ( $o->json ) ;
				$out['data']['issues'][$o->id] = $o ;
				$entries[$o->entry_id] = $o->entry_id ;
			}
			if ( $open_issues < $min_open_issues ) break ;
		}
		if ( count($entries) == 0 ) return ;
		$sql = "SELECT /* ".__METHOD__." */ * FROM entry WHERE id IN (".implode(',',$entries).")" ;
		$this->add_entries_and_users_from_sql ( $out , $sql ) ;
	}

	protected function query_autoscrape_test ( &$out ) {
		$json = $this->get_request ( 'json' , '' ) ;
		$as = new AutoScrape ;

		if ( !$as->loadFromJSON ( $json , 0 ) ) throw new \Exception($as->error) ;
		$as->max_urls_requested = 1 ;
		$as->log2array = true ;
		$as->runTest() ;
		if ( isset($as->error) AND $as->error != null ) $out['status'] = $as->error ;
		$out['data']['html'] = $as->last_html ;
		$out['data']['log'] = $as->logs ;
		$out['data']['results'] = $as->entries ;
		$out['data']['last_url'] = $as->getLastURL() ;
		$out['data']['html'] = utf8_encode ( $out['data']['html'] ) ;
		foreach ( $out['data']['results'] AS $k => $v ) {
// TODO?
		}
	}

	protected function query_save_scraper ( &$out ) {
		$scraper = json_decode ( $this->get_request ( 'scraper' , '{}' ) ) ;
		$levels = json_decode ( $this->get_request ( 'levels' , '{}' ) ) ;
		$options = json_decode ( $this->get_request ( 'options' , '{}' ) ) ;
		$meta = json_decode ( $this->get_request ( 'meta' , '{}' ) ) ;
		$user_id = $this->check_and_get_user_id ( $this->user ) ;

		$cid = trim($meta->catalog_id) ;
		$exists = false ;
if ( !preg_match ( '/^\d+$/' , $cid ) ) {
// Create new catalog
			if ( $scraper->resolve->type->use == 'Q5' ) $meta->type = 'biography' ;
			$meta->note = 'Created via scraper import' ;
			$catalog = new Catalog ( 0 , $this->mnm ) ;
			$cid = $catalog->createNew ( $meta , $user_id ) ;
			$exists = true ;
} else {
// Check if catalog exists
			$sql = "SELECT /* ".__METHOD__." */ * FROM catalog WHERE id=$cid" ;
			$result = $this->mnm->getSQL ( $sql ) ;
			while($o = $result->fetch_object()) $exists = true ;
		}
		if ( !$exists ) throw new \Exception("Catalog #{$cid} does not exist") ;

		$out['data']['catalog'] = $cid ;
		$j = [ 'levels' => $levels , 'options' => $options , 'scraper' => $scraper ] ;
		$j = $this->mnm->escape(json_encode($j)) ;

		$existing = (object) ['bla'=>'test'] ;
		$sql = "SELECT /* ".__METHOD__." */ * FROM autoscrape WHERE catalog=$cid" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $existing = $o ;

		if ( isset($existing->owner) ) {
			if ( $existing->owner != $user_id ) throw new \Exception("A different user created the existing scraper") ;
			$sql = "UPDATE autoscrape SET `json`='$j',`status`='IMPORT' WHERE catalog=$cid" ;
			$result = $this->mnm->getSQL ( $sql ) ;
		} else {
			$sql = "INSERT INTO autoscrape (`catalog`,`json`,`owner`,`notes`,`status`) VALUES ($cid,\"$j\",$user_id,'Created via scraper import','IMPORT') ON DUPLICATE KEY UPDATE json='$j',status='IMPORT'" ;
			$result = $this->mnm->getSQL ( $sql ) ;
		}
		$this->mnm->queue_job ( $cid , 'autoscrape' , 0 , '' , 0 , 0 , 'HIGH_PRIORITY' ) ;
	}

	protected function query_get_top_groups ( &$out ) {
		$this->add_sql_to_out ( $out , "SELECT /* ".__METHOD__." */ `top_missing_groups`.*,`user`.`name` AS `user_name` FROM `top_missing_groups`,`user` WHERE `top_missing_groups`.`user`=`user`.`id` AND `current`=1 ORDER BY `name`" ) ;
	}

	protected function query_set_top_group ( &$out ) {
		$data = [
			'name' => trim($this->get_escaped_request ( 'group_name' , '' )) ,
			'catalogs' => trim($this->get_escaped_request ( 'catalogs' , '' )) ,
			'user' => $this->check_and_get_user_id ( $this->get_request ( 'username' , '' ) ) ,
			'timestamp' => $this->mnm->getCurrentTimestamp() ,
			'current' => 1 ,
			'based_on' => $this->get_request_int ( 'group_id' ) ,
		] ;
		if ( $data['based_on'] > 0 ) $this->mnm->getSQL ( "UPDATE `top_missing_groups` SET `current`=0 WHERE `id`={$data['based_on']}" ) ;
		$this->insert_ignore ( 'top_missing_groups' , $data ) ;
	}

	protected function query_remove_empty_top_group ( &$out ) {
		$group_id = $this->get_request_int ( 'group_id' );
		$sql = "UPDATE `top_missing_groups` SET `current`=0 WHERE `catalogs`='' AND `id`={$group_id}" ;
		$this->mnm->getSQL ( $sql ) ;
	}

	protected function query_get_locations_in_catalog ( &$out ) {
		$this->add_sql_to_out ( $out , "SELECT /* ".__METHOD__." */ * FROM `vw_location` WHERE `catalog`=".$this->get_catalog() ) ;
	}

	protected function query_get_missing_properties ( &$out ) {
		$this->add_sql_to_out ( $out , "SELECT /* ".__METHOD__." */ * FROM `props_todo`" ) ;
	}

	protected function query_set_missing_properties_status ( &$out ) {
		$username = trim($this->get_request ( 'username' , '' )) ;
		$user_id = $this->check_and_get_user_id ( $username ) ;
		$status = trim($this->get_escaped_request ( 'status' )) ;
		$note = trim($this->get_escaped_request ( 'note' )) ;
		$row_id = $this->get_request_int ( 'row_id' ) ;
		if ( $row_id <=0 ) throw new \Exception("Bad/missing row ID");
		if ( $status == '' ) throw new \Exception("Invalid status");
		$sql = "UPDATE `props_todo` SET `status`='{$status}',`note`='{$note}',`user_id`={$user_id} WHERE `id`={$row_id}" ;
		$this->mnm->getSQL ( $sql ) ;
	}

	protected function query_get_sync ( &$out ) {
		$catalog = $this->get_catalog();
		set_time_limit(600); # 10min

// Load prop/qual
		$result = $this->mnm->getSQL ( "SELECT /* ".__METHOD__." */ * FROM catalog WHERE id=$catalog" ) ;
		while($o = $result->fetch_object()){
			$prop = $o->wd_prop ;
			$qual = $o->wd_qual ;
		}
		if ( !isset($prop) ) $prop = '' ;
		if ( !isset($qual) ) $qual = '' ;
		if ( $prop == '' ) throw new \Exception("No Wikidata property defined for this catalog") ;
		if ( $qual != '' ) throw new \Exception("This does not work for the old, qualifier-based catalogs") ;

		$sc = new SetCompare ( $this->mnm , $catalog );

// Get Wikidata state
		$query = "SELECT ?q ?prop { ?q wdt:P$prop ?prop }" ;
		foreach ( $this->mnm->tfc->getSPARQL_TSV($query) as $o ) {
			$q = $this->mnm->tfc->parseItemFromURL($o['q']) ;
			$sc->addWD ( $q , $o['prop'] ) ;
		}
		$sc->flushWD() ;

// Get Mix-n-match state
		$sc->addFromMnM() ;

// Report
		if ( $catalog == 3296 ) { # TESTING
			# OK $out['data']['mm_dupes'] = $sc->get_mnm_dupes() ; # More than one ext_id for the same q in MnM
			# BROKEN $out['data']['different'] = $sc->get_diff() ; # Different ext_ids for the same q between mnm and wd
			# BROKEN $out['data']['wd_no_mm'] = $sc->compare_wd_mnm ( 1 ) ;
			# BROKEN $out['data']['mm_no_wd'] = $sc->compare_wd_mnm ( 0 ) ;
		} else {
			$out['data']['mm_dupes'] = $sc->get_mnm_dupes() ; # More than one ext_id for the same q in MnM
			$out['data']['different'] = $sc->get_diff() ; # Different ext_ids for the same q between mnm and wd
			$out['data']['wd_no_mm'] = $sc->compare_wd_mnm ( 1 ) ;
			$out['data']['mm_no_wd'] = $sc->compare_wd_mnm ( 0 ) ;
		}

		$out['data']['mm_double'] = [] ;
		$sql = "SELECT /* ".__METHOD__." */ q,count(*) AS cnt,group_concat(id) AS ids FROM entry WHERE q>0 AND catalog=$catalog AND q IS NOT NULL AND user IS NOT NULL AND user>0 GROUP BY q HAVING cnt>1" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $out['data']['mm_double']["{$o->q}"] = explode ( ',' , $o->ids ) ;
	}

	protected function query_get_entry_reader_view ( &$out ) {
		$entry_id = $this->get_request_int ( 'entry' , -1 ) ;
		$sql = "SELECT /* ".__METHOD__." */ ext_url FROM entry WHERE id={$entry_id}";
		$result = $this->mnm->getSQL ( $sql ) ;
		if ($o = $result->fetch_object()) {
			$url = str_replace(' ','%20',$o->ext_url);
			$out['data']['url'] = $url;

			$autoscrape = new AutoScrape;
			$html = $autoscrape->getContentFromURL($url);
// $out['data']['html'] = $html;

			$config = new \fivefilters\Readability\Configuration([
				'fixRelativeURLs' => true,
				'originalURL'     => $url,
				'substituteEntities' => true,
				'normalizeEntities' => true,
				'summonCthulhu'   => true, # remove all <script> nodes via regex
			]);
			$readability = new \fivefilters\Readability\Readability($config);
			try {
			    $readability->parse($html);
			    $content = ''.$readability->getContent();
			    $content = preg_replace("|\s+|",' ',$content);
			    $content = str_replace("> <",'><',$content);
			    $out['data']['reader_view'] = $content;
			} catch (\fivefilters\Readability\ParseException $e) {
			    $out['status'] = 'Could not reader-ify HTML: '.$e->getMessage();
			}
		}
	}

	################################################################################
	# Public helper functions

	public function get_request ( $varname , $default = '' ) {
		return $this->mnm->tfc->getRequest ( $varname , $default ) ;
	}

	private function normalize_user_name ( $user_name ) {
		return str_replace ( ' ' , '_' , trim($user_name) ) ;
	}

	public function check_and_get_user_id ( $username ) {
		require_once '/data/project/magnustools/public_html/php/Widar.php' ;
		$widar = new \Widar ( 'mix-n-match' ) ;
		$oauth_user_name = $widar->get_username();
		if ( $this->normalize_user_name($username)!=$this->normalize_user_name($oauth_user_name) ) throw new \Exception ("OAuth user name problem") ;
		$user_id = $this->mnm->getOrCreateUserID ( $username ) ;
		if ( $user_id <= 0 ) throw new \Exception("OAuth login failure, please log in again") ;
		$user = new User ( $user_id , $this->mnm );
		if ( $user->isBlocked() ) throw new \Exception("You are blocked on Wikidata") ;
		return $user_id ;
	}

	public function get_users ( $users ) {
		if ( count ( $users ) == 0 ) return [] ;
		$ret = [] ;
		$sql = "SELECT /* ".__METHOD__." */ * FROM user WHERE id IN (" . implode(',',array_keys($users)) . ")" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $ret[$o->id] = $o ;
		return $ret ;
	}

	public function add_extended_entry_data ( &$out ) {
		if ( !isset($out) or !isset($out['data']) or !isset($out['data']['entries']) ) return ;
		if ( count ( $out['data']['entries'] ) == 0 ) return ;

		$keys = implode ( ',' , array_keys ( $out['data']['entries'] ) ) ;

// Person birth/death dates
		$sql = "SELECT /* ".__METHOD__." */ * FROM person_dates WHERE entry_id IN ($keys)" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()){
			if ( $o->born != '' ) $out['data']['entries'][$o->entry_id]->born = $o->born ;
			if ( $o->died != '' ) $out['data']['entries'][$o->entry_id]->died = $o->died ;
		}

// Location data
		$sql = "SELECT /* ".__METHOD__." */ * FROM location WHERE entry_id IN ($keys)" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()){
			$out['data']['entries'][$o->entry_id]->lat = $o->lat ;
			$out['data']['entries'][$o->entry_id]->lon = $o->lon ;
		}

// Multimatch
		$sql = "SELECT /* ".__METHOD__." */ * FROM multi_match WHERE entry_id IN ($keys)" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()){
			$a = [] ;
			foreach ( explode(',',$o->candidates) AS $c ) $a[] = 'Q'.$c ;
			$out['data']['entries'][$o->entry_id]->multimatch = $a ;
		}

// Aux
		$sql = "SELECT /* ".__METHOD__." */ * FROM auxiliary WHERE entry_id IN ($keys)" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $out['data']['entries'][$o->entry_id]->aux[] = $o ;

// Aliases
		$sql = "SELECT /* ".__METHOD__." */ * FROM aliases WHERE entry_id IN ($keys) ORDER BY entry_id,language,label" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $out['data']['entries'][$o->entry_id]->aliases[] = $o ;

// Language descriptions
		$sql = "SELECT /* ".__METHOD__." */ * FROM descriptions WHERE entry_id IN ($keys) ORDER BY entry_id,language,label" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $out['data']['entries'][$o->entry_id]->descriptions[] = $o ;

// Entry keyvalue pairs
		$sql = "SELECT /* ".__METHOD__." */ * FROM kv_entry WHERE entry_id IN ($keys)" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) {
			$k = $o->kv_key;
			$out['data']['entries'][$o->entry_id]->$k = [$o->kv_value,$o->done];
		}

// Relations
		$sql = "SELECT /* ".__METHOD__." */ property,mnm_relation.entry_id AS source_entry_id,entry.* FROM mnm_relation,entry WHERE entry.id=mnm_relation.target_entry_id AND mnm_relation.entry_id IN ($keys)" ;
		$result = $this->mnm->getSQL ( $sql ) ;
		while($o = $result->fetch_object()) $out['data']['entries'][$o->source_entry_id]->relation[] = $o ;
	}

} ;

?>
*/
