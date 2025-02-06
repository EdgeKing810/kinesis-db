// Add a restore policy enum
#[derive(Debug, PartialEq)]
pub enum RestorePolicy {
    RecoverPending,
    RecoverAll,
    Discard,
}