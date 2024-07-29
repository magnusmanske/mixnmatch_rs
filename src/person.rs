use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref SANITIZE_NAME_RES: Vec<Regex> = vec![
        Regex::new(r"^(Sir|Mme|Dr|Mother|Father)\.{0,1} ").expect("Regex failure"),
        Regex::new(r"\b[A-Z]\. /").expect("Regex failure"),
        Regex::new(r" (\&) ").expect("Regex failure"),
        Regex::new(r"\(.+?\)").expect("Regex failure"),
        Regex::new(r"\s+").expect("Regex failure"),
    ];
    static ref SIMPLIFY_NAME_RES: Vec<Regex> = vec![
        Regex::new(r"\s*\(.*?\)\s*").expect("Regex failure"),
        Regex::new(r"[, ]+(Jr\.{0,1}|Sr\.{0,1}|PhD\.{0,1}|MD|M\.D\.)$").expect("Regex failure"),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+")
            .expect("Regex failure"),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+")
            .expect("Regex failure"),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+")
            .expect("Regex failure"),
        Regex::new(r"\s*(Ritter|Freiherr)\s+").expect("Regex failure"),
        Regex::new(r"\s+").expect("Regex failure"),
    ];
    static ref SIMPLIFY_NAME_TWO_RE: Regex =
        Regex::new(r"^(\S+) .*?(\S+)$").expect("Regex failure");
}

#[derive(Debug, Clone)]
pub struct Person {}

impl Person {
    pub fn sanitize_simplify_name(name: &str) -> String {
        let name = Self::sanitize_name(name);
        let name = Self::simplify_name(&name);
        name
    }

    fn sanitize_name(name: &str) -> String {
        let mut name = name.to_string();
        for re in SANITIZE_NAME_RES.iter() {
            name = re.replace_all(&name, " ").to_string();
        }
        name.trim().to_string()
    }

    fn simplify_name(name: &str) -> String {
        let mut name = name.to_string();
        for re in SIMPLIFY_NAME_RES.iter() {
            name = re.replace_all(&name, " ").to_string();
        }
        name = SIMPLIFY_NAME_TWO_RE.replace_all(&name, "$1 $2").to_string();
        name.trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_name() {
        assert_eq!(
            Person::sanitize_name("Sir John Doe"),
            "John Doe".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Mme. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Dr. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Mother Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Father Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe (actor)"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            Person::sanitize_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
    }

    #[test]
    fn test_simplify_name() {
        assert_eq!(
            Person::simplify_name("Jane Doe (actor)"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Jane Doe, Jr."),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Jane Doe, Sr."),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Jane Doe, PhD"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Jane Doe, MD"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Jane Doe, M.D."),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Sir Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Baron Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Baronesse Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Graf Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Gräfin Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Prince Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Princess Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Dr. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Prof. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            Person::simplify_name("Rev. Jane Doe"),
            "Jane Doe".to_string()
        );
    }
}
