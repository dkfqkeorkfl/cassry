use std::collections::HashMap;
use secrecy::{SecretString, ExposeSecret};

/// Parses a string into a HashMap of SecretStrings
/// Format: Each secret should start with "!!key:" followed by its value
/// The value can contain newlines and continues until the next "!!" or end of input
/// Example:
/// !!a1:This is a value
/// that can span multiple lines
/// !!b1:Another value
pub fn parse(input: &str) -> anyhow::Result<HashMap<String, SecretString>> {
    let mut secrets = HashMap::new();
    let mut current_pos = 0;

    while let Some(key_start) = input[current_pos..].find("!!") {
        let key_start = current_pos + key_start;
        
        // Find the next ":" after "!!"
        let colon_pos = anyhow::Context::context(
            input[key_start..].find(':'),
            format!("Invalid format: Missing ':' separator after '!!' at position {}", key_start)
        )? + key_start;

        // Extract key
        let key = input[key_start + 2..colon_pos].trim().to_string();
        if key.is_empty() {
            return Err(anyhow::anyhow!(
                "Invalid format: Empty key at position {}",
                key_start
            ));
        }

        // Find the start of the next key or end of input
        let next_key_start = input[colon_pos + 1..].find("!!")
            .map(|pos| colon_pos + 1 + pos)
            .unwrap_or(input.len());

        // Extract value (including any newlines)
        let value = input[colon_pos + 1..next_key_start].trim().to_string();
        
        // Store the secret
        secrets.insert(key, SecretString::from(value));
        
        // Move to the next key
        current_pos = next_key_start;
    }

    Ok(secrets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_input() {
        let input = "!!a1:a2\n!!b1:b2";
        let secrets = parse(input).unwrap();

        assert_eq!(secrets.len(), 2);
        assert_eq!(secrets.get("a1").unwrap().expose_secret(), "a2");
        assert_eq!(secrets.get("b1").unwrap().expose_secret(), "b2");
    }

    #[test]
    fn test_parse_multiline_value() {
        let input = "!!a1:This is a value\nthat spans\nmultiple lines\n!!b1:Another value";
        let secrets = parse(input).unwrap();

        assert_eq!(secrets.len(), 2);
        assert_eq!(secrets.get("a1").unwrap().expose_secret(), "This is a value\nthat spans\nmultiple lines");
        assert_eq!(secrets.get("b1").unwrap().expose_secret(), "Another value");
    }

    #[test]
    fn test_parse_empty_input() {
        let input = "";
        let secrets = parse(input).unwrap();
        assert!(secrets.is_empty());
    }

    #[test]
    fn test_parse_invalid_format() {
        let test_cases = vec![
            ("a1:a2", "Missing ':' separator"),
            ("!!a1", "Missing ':' separator"),
            ("!!:a2", "Empty key"),
        ];

        for (input, expected_error) in test_cases {
            let result = parse(input);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains(expected_error));
        }
    }

    #[test]
    fn test_parse_with_whitespace() {
        let input = "  !!a1  :  a2  \n  !!b1  :  b2  ";
        let secrets = parse(input).unwrap();

        assert_eq!(secrets.len(), 2);
        assert_eq!(secrets.get("a1").unwrap().expose_secret(), "a2");
        assert_eq!(secrets.get("b1").unwrap().expose_secret(), "b2");
    }
}
