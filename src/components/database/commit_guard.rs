use crate::components::transaction::transaction::Transaction;

use super::engine::DBEngine;

pub struct CommitGuard<'a> {
    engine: &'a mut DBEngine, // The engine to commit the transaction to
    tx: Option<Transaction>,  // The transaction to commit
    success: bool,            // Flag to track if the transaction was successfully committed
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
        // Commit the transaction and set the success flag
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
    // Rollback the transaction if it was not successfully committed
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
