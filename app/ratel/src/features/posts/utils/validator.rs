use crate::features::posts::types::PostError;
use crate::features::posts::*;

pub fn validate_title(title: &str) -> Result<()> {
    let len = title.chars().count();
    if len < 3 || len > 50 {
        return Err(PostError::ContentTooShort.into());
    }
    Ok(())
}

pub fn validate_content(body: &ContentBody) -> Result<()> {
    if body.char_count() < 10 {
        return Err(Error::ValidationTooShortContents);
    }

    Ok(())
}
