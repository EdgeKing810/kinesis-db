use super::{database::engine::DBEngine, transaction::transaction::Transaction};

// Add a CommitGuard to ensure proper cleanup
pub struct CommitGuard<'a> {
    engine: &'a mut DBEngine,
    tx: Option<Transaction>,
    success: bool,
}

impl<'a> CommitGuard<'a> {
    pub fn new(engine: &'a mut DBEngine, tx: Transaction) -> Self {
        CommitGuard {
            engine,
            tx: Some(tx),
            success: false,
        }
    }

    pub fn commit(mut self) -> Result<(), String> {
        if let Some(tx) = self.tx.take() {
            let result = self.engine.commit_internal(tx);
            self.success = result.is_ok();
            result
        } else {
            Err("Transaction already consumed".to_string())
        }
    }
}

impl<'a> Drop for CommitGuard<'a> {
    fn drop(&mut self) {
        if !self.success {
            if let Some(tx) = self.tx.take() {
                if let Err(e) = self.engine.rollback_failed_commit(&tx) {
                    eprintln!("Failed to rollback transaction: {}", e);
                }
            }
        }
    }
}