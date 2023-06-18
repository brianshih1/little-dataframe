use super::field::Field;

pub struct Schema {}

impl<F> FromIterator<F> for Schema
where
    F: Into<Field>,
{
    fn from_iter<T: IntoIterator<Item = F>>(iter: T) -> Self {
        todo!()
    }
}
