//! Cheap sentence splitter. Splits on `.!?` followed by whitespace and a
//! capital letter or digit. Doesn't try to be clever about abbreviations —
//! Edge TTS handles "Dr." or "U.S.A." correctly within a sentence; the
//! risk of an over-split is that one sentence renders as two utterances
//! with a slightly longer pause between them. Acceptable for v1.

pub fn split(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        buf.push(c);
        if matches!(c, '.' | '!' | '?') {
            // Consume any run of further terminators (e.g. "?!").
            while let Some(&next) = chars.peek()
                && matches!(next, '.' | '!' | '?')
            {
                buf.push(next);
                chars.next();
            }
            // Whitespace + uppercase/digit ⇒ sentence boundary.
            let mut tail = chars.clone();
            let mut spaces = String::new();
            while let Some(&n) = tail.peek()
                && n.is_whitespace()
            {
                spaces.push(n);
                tail.next();
            }
            if !spaces.is_empty()
                && tail
                    .peek()
                    .is_some_and(|n| n.is_uppercase() || n.is_ascii_digit())
            {
                let trimmed = buf.trim().to_string();
                if !trimmed.is_empty() {
                    out.push(trimmed);
                }
                buf.clear();
                // Skip whitespace; keep the next character for the new sentence.
                for _ in 0..spaces.chars().count() {
                    chars.next();
                }
            }
        }
    }
    let trimmed = buf.trim().to_string();
    if !trimmed.is_empty() {
        out.push(trimmed);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_simple() {
        assert_eq!(
            split("Hello there. This is a test. Goodbye."),
            vec!["Hello there.", "This is a test.", "Goodbye."],
        );
    }

    #[test]
    fn keeps_single_sentence() {
        assert_eq!(
            split("Just one sentence here."),
            vec!["Just one sentence here."]
        );
    }

    #[test]
    fn handles_question_and_exclaim() {
        assert_eq!(
            split("Really? Yes! Of course."),
            vec!["Really?", "Yes!", "Of course."],
        );
    }

    #[test]
    fn handles_run_of_punctuation() {
        assert_eq!(split("Wait?! Okay."), vec!["Wait?!", "Okay."]);
    }

    #[test]
    fn empty_input() {
        assert!(split("").is_empty());
        assert!(split("   ").is_empty());
    }

    #[test]
    fn no_split_lowercase_after() {
        // Decimal-like — don't split inside numbers.
        assert_eq!(
            split("It costs 1.5 dollars."),
            vec!["It costs 1.5 dollars."]
        );
    }
}
