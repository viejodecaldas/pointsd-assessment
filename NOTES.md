# PostgreSQL implementation
````
Implement Store.Award and Store.Reverse as transactions against shared PostgreSQL.
Enforce UNIQUE (payment_id, kind) on ledger_entries; payment_id is the identity, not event_id.
Use READ COMMITTED and acquire pg_advisory_xact_lock for payment_id before any reads.
Use stable, namespaced lock keys shared by every replica; hash collisions only add contention.
Award then takes a transaction advisory lock for (member_id, settlement_month_UTC).
Always acquire payment before member/month locks, and require all writers to follow this order.
After locking, check for an existing award; a duplicate succeeds without inserting.
Sum award entries only for that member/month, then insert min(requested, max(cap-used, 0)).
Persist zero-point awards too, so processed payments cannot earn on a later delivery.
Reverse checks for an existing reversal, then reads the original award under the payment lock.
Insert its negative points with its original member and month, never the reversal payload's values.
Missing or zero-point awards return a business outcome: log and Ack without a debit.
Reversals never reduce the monthly award sum, so they cannot restore cap room.
Commit before Ack; on storage errors Nack, rolling back any still-open transaction.
If commit succeeds but its response or Ack is lost, redelivery observes the existing entry.
Retry deadlocks/transaction failures via Nack; no in-process lock coordinates production replicas.
Payment locks serialize duplicates/reversals; member/month locks serialize cap allocation.
Other members proceed independently; MemoryStore models each transaction with a short mutex.
No database or driver is required here; Balance remains SUM(points) over stored entries.
