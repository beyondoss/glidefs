//! The objects a disk's purge may delete, as a decision proven with Verus and a capability only it mints. A pack is a store object under
//! `{db}/exports/{pool}/chunks/` (`ContentStore`'s pack keys), shared by every disk forked in the pool; the collector (`pack_liveness`) is the
//! only deleter of one. `ExportRouter::purge_s3_state` deletes a disk's manifest, its snapshots and owner markers, its snapshot index entries
//! and its definition, and each object it deletes (each concrete key, including every object a listing under a prefix returns) first passes
//! [`purge_gate`], which refuses a pack key; the deleters take the [`PurgeTarget`] it mints.
//!
//! * [`not_a_pack`]: `true` exactly when the key is not under `{db}/exports/{pool}/chunks/` for any slash-free `pool`.
//! * [`thm_the_keys_a_purge_deletes_are_not_packs`]: the manifest, snapshot, owner marker, index and definition keys of a disk (the shapes
//!   `manifest_s3_key`, `snapshot_s3_key`, `snapshot_owner_s3_key`, `index/snapshots/{name}@...` and `exports/{name}/export.json` build) are
//!   all accepted, and a pack key (`ContentStore::chunk_pack_key`'s shape) never is.
//!
//! Mutation (`scripts/verify-kernels.sh --features mut_purge_gate_admits_packs` must go red): the gate admits every key.

use vstd::prelude::*;

verus! {

/// `/exports/`
pub open spec fn exports_lit() -> Seq<u8> {
    seq![47u8, 101u8, 120u8, 112u8, 111u8, 114u8, 116u8, 115u8, 47u8]
}

/// `/chunks/`
pub open spec fn chunks_lit() -> Seq<u8> {
    seq![47u8, 99u8, 104u8, 117u8, 110u8, 107u8, 115u8, 47u8]
}

pub open spec fn no_slash(s: Seq<u8>) -> bool {
    forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i] != 47u8
}

/// A pack's key: `{db}/exports/{pool}/chunks/{chunk}/{pack}.pack` for a slash-free pool.
pub open spec fn pack_key(db: Seq<u8>, pool: Seq<u8>, tail: Seq<u8>) -> Seq<u8> {
    db + exports_lit() + pool + chunks_lit() + tail
}

/// The key is some pool's pack key.
pub open spec fn is_pack(db: Seq<u8>, key: Seq<u8>) -> bool {
    exists|pool: Seq<u8>, tail: Seq<u8>| no_slash(pool) && key == #[trigger] pack_key(db, pool, tail)
}

/// The keys a purge deletes, built as `ContentStore` and the router build them, from a pool, a disk name (no slash: `validate_export_name`)
/// and a sequence or owner segment.
pub open spec fn manifest_key(db: Seq<u8>, pool: Seq<u8>, name: Seq<u8>) -> Seq<u8> {
    db + exports_lit() + pool + seq![47u8, 109u8, 97u8, 110u8, 105u8, 102u8, 101u8, 115u8, 116u8, 115u8, 47u8] + name
}

pub open spec fn snapshots_key(db: Seq<u8>, pool: Seq<u8>, name: Seq<u8>, rest: Seq<u8>) -> Seq<u8> {
    db + exports_lit() + pool + seq![47u8, 115u8, 110u8, 97u8, 112u8, 115u8, 104u8, 111u8, 116u8, 115u8, 47u8] + name + seq![47u8] + rest
}

pub open spec fn owner_marker_key(db: Seq<u8>, pool: Seq<u8>, name: Seq<u8>, rest: Seq<u8>) -> Seq<u8> {
    db + exports_lit() + pool
        + seq![47u8, 115u8, 110u8, 97u8, 112u8, 115u8, 104u8, 111u8, 116u8, 45u8, 111u8, 119u8, 110u8, 101u8, 114u8, 115u8, 47u8]
        + name + seq![47u8] + rest
}

/// `{db}/exports/{name}/export.json`
pub open spec fn definition_key(db: Seq<u8>, name: Seq<u8>) -> Seq<u8> {
    db + exports_lit() + name + seq![47u8, 101u8, 120u8, 112u8, 111u8, 114u8, 116u8, 46u8, 106u8, 115u8, 111u8, 110u8]
}

/// `{db}/index/snapshots/{name}@{rest}`
pub open spec fn index_key(db: Seq<u8>, name: Seq<u8>, rest: Seq<u8>) -> Seq<u8> {
    db + seq![47u8, 105u8, 110u8, 100u8, 101u8, 120u8, 47u8, 115u8, 110u8, 97u8, 112u8, 115u8, 104u8, 111u8, 116u8, 115u8, 47u8]
        + name + seq![64u8] + rest
}


/// `key` begins with `{db}/exports/`.
pub open spec fn has_prefix(db: Seq<u8>, key: Seq<u8>) -> bool {
    key.len() >= db.len() + 9int && key.subrange(0, db.len() as int) =~= db && key.subrange(db.len() as int, db.len() + 9int) =~= exports_lit()
}

proof fn no_pack_without_prefix(db: Seq<u8>, key: Seq<u8>)
    requires
        !has_prefix(db, key),
    ensures
        !is_pack(db, key),
{
    if is_pack(db, key) {
        let (pool, tail) = choose|pool: Seq<u8>, tail: Seq<u8>| no_slash(pool) && key == #[trigger] pack_key(db, pool, tail);
        assert(key == db + exports_lit() + pool + chunks_lit() + tail);
        assert(key.subrange(0, db.len() as int) =~= db);
        assert(key.subrange(db.len() as int, db.len() + 9int) =~= exports_lit());
        assert(has_prefix(db, key));
    }
}

proof fn not_pack_by_first_slash(db: Seq<u8>, key: Seq<u8>, start: int, i: int, r: bool)
    requires
        start == db.len() + 9,
        has_prefix(db, key),
        start <= i <= key.len(),
        forall|k: int| start <= k < i ==> #[trigger] key[k] != 47u8,
        i == key.len() || key[i] == 47u8,
        r == (i == key.len() || !(i + 8 <= key.len() && key.subrange(i, i + 8) =~= chunks_lit())),
    ensures
        r == !is_pack(db, key),
{
    if !r {
        let pool = key.subrange(start, i);
        let tail = key.skip(i + 8);
        assert(no_slash(pool)) by {
            assert forall|k: int| 0 <= k < pool.len() implies #[trigger] pool[k] != 47u8 by {
                assert(pool[k] == key[start + k]);
            }
        }
        assert(key =~= pack_key(db, pool, tail)) by {
            assert(key.subrange(0, start) =~= db + exports_lit());
            assert(key =~= key.subrange(0, start) + key.subrange(start, i) + key.subrange(i, i + 8) + key.skip(i + 8));
        }
        assert(is_pack(db, key));
    } else {
        if is_pack(db, key) {
            let (pool, tail) = choose|pool: Seq<u8>, tail: Seq<u8>| no_slash(pool) && key == #[trigger] pack_key(db, pool, tail);
            let q = start + pool.len();
            assert(key == db + exports_lit() + pool + chunks_lit() + tail);
            assert(key.subrange(q, q + 8) =~= chunks_lit());
            assert(key[q] == 47u8);
            assert forall|k: int| start <= k < q implies key[k] != 47u8 by {
                assert(key[k] == pool[k - start]);
            }
            assert(i == q);
        }
    }
}

/// Whether `s[pos..]` begins with the `lit` (all of it inside `s`).
fn matches_at(s: &[u8], pos: usize, lit: &[u8]) -> (r: bool)
    requires
        pos <= s.len(),
    ensures
        r == (pos + lit.len() <= s.len() && s@.subrange(pos as int, pos + lit.len()) =~= lit@),
{
    if lit.len() > s.len() - pos {
        return false;
    }
    let mut j: usize = 0;
    while j < lit.len()
        invariant
            j <= lit.len(),
            pos + lit.len() <= s.len(),
            forall|k: int| 0 <= k < j ==> #[trigger] s@[pos + k] == lit@[k],
        decreases lit.len() - j,
    {
        if s[pos + j] != lit[j] {
            proof {
                assert(s@.subrange(pos as int, pos + lit.len())[j as int] == s@[pos + j as int]);
                assert(!(s@.subrange(pos as int, pos + lit.len()) =~= lit@));
            }
            return false;
        }
        j += 1;
    }
    proof {
        assert(s@.subrange(pos as int, pos + lit.len()) =~= lit@) by {
            assert forall|k: int| 0 <= k < lit.len() implies s@.subrange(pos as int, pos + lit.len())[k] == lit@[k] by {
                assert(s@[pos + k] == lit@[k]);
            }
        }
    }
    true
}

#[cfg(not(feature = "mut_purge_gate_admits_packs"))]
pub fn not_a_pack(db: &[u8], key: &[u8]) -> (r: bool)
    ensures
        r == !is_pack(db@, key@),
{
    // Not under `{db}/exports/`: no pack lives there.
    let exports: [u8; 9] = [47u8, 101u8, 120u8, 112u8, 111u8, 114u8, 116u8, 115u8, 47u8];
    let chunks: [u8; 8] = [47u8, 99u8, 104u8, 117u8, 110u8, 107u8, 115u8, 47u8];
    proof {
        assert(exports@ =~= exports_lit());
        assert(chunks@ =~= chunks_lit());
    }
    if !matches_at(key, 0, db) {
        proof { no_pack_without_prefix(db@, key@); }
        return true;
    }
    if !matches_at(key, db.len(), exports.as_slice()) {
        proof { no_pack_without_prefix(db@, key@); }
        return true;
    }
    let start = db.len() + 9;
    // The pool is the segment up to the next slash.
    let mut i: usize = start;
    while i < key.len() && key[i] != 47u8
        invariant
            start <= i <= key.len(),
            start == db.len() + 9,
            has_prefix(db@, key@),
            forall|k: int| start <= k < i ==> #[trigger] key@[k] != 47u8,
        decreases key.len() - i,
    {
        i += 1;
    }
    let r = i == key.len() || !matches_at(key, i, chunks.as_slice());
    proof { not_pack_by_first_slash(db@, key@, start as int, i as int, r); }
    r
}

/// Mutation: the gate admits every key.
#[cfg(feature = "mut_purge_gate_admits_packs")]
pub fn not_a_pack(db: &[u8], key: &[u8]) -> (r: bool)
    ensures
        r == !is_pack(db@, key@),
{
    true
}


/// A slash-free head and a tail that starts with a slash split a sequence one way only.
proof fn split_unique(p1: Seq<u8>, p2: Seq<u8>, x: Seq<u8>, y: Seq<u8>)
    requires
        no_slash(p1),
        no_slash(p2),
        x.len() > 0,
        y.len() > 0,
        x[0] == 47u8,
        y[0] == 47u8,
        p1 + x == p2 + y,
    ensures
        p1 == p2,
        x == y,
{
    if p1.len() < p2.len() {
        assert((p1 + x)[p1.len() as int] == x[0]);
        assert((p2 + y)[p1.len() as int] == p2[p1.len() as int]);
        assert(false);
    } else if p2.len() < p1.len() {
        assert((p2 + y)[p2.len() as int] == y[0]);
        assert((p1 + x)[p2.len() as int] == p1[p2.len() as int]);
        assert(false);
    } else {
        assert(p1 =~= p2) by {
            assert forall|k: int| 0 <= k < p1.len() implies p1[k] == p2[k] by {
                assert((p1 + x)[k] == p1[k]);
                assert((p2 + y)[k] == p2[k]);
            }
        }
        assert((p1 + x).len() == p1.len() + x.len());
        assert((p2 + y).len() == p2.len() + y.len());
        assert(x.len() == y.len());
        assert(x =~= y) by {
            assert forall|k: int| 0 <= k < x.len() implies x[k] == y[k] by {
                assert(p1.len() == p2.len());
                assert((p1 + x)[p1.len() + k] == (p2 + y)[p1.len() + k]);
                assert((p1 + x)[p1.len() + k] == x[k]);
                assert((p2 + y)[p2.len() + k] == y[k]);
            }
        }
    }
}

/// `db/exports/{pool}{x}` with `x` a slash-led tail that does not begin `/chunks/` is no pack's key.
proof fn tail_that_is_not_chunks_is_no_pack(db: Seq<u8>, pool: Seq<u8>, x: Seq<u8>)
    requires
        no_slash(pool),
        x.len() > 0,
        x[0] == 47u8,
        !(x.len() >= 8 && x.subrange(0, 8) =~= chunks_lit()),
    ensures
        !is_pack(db, db + exports_lit() + pool + x),
{
    if is_pack(db, db + exports_lit() + pool + x) {
        let (p2, tail) = choose|pool2: Seq<u8>, tail: Seq<u8>| no_slash(pool2) && db + exports_lit() + pool + x == #[trigger] pack_key(db, pool2, tail);
        let y = chunks_lit() + tail;
        let base = db + exports_lit();
        assert(base + (pool + x) =~= db + exports_lit() + pool + x);
        assert(base + (p2 + y) =~= pack_key(db, p2, tail));
        assert(pool + x == p2 + y) by {
            assert forall|k: int| 0 <= k < (pool + x).len() implies (pool + x)[k] == (p2 + y)[k] by {
                assert((base + (pool + x))[base.len() + k] == (pool + x)[k]);
                assert((base + (p2 + y))[base.len() + k] == (p2 + y)[k]);
            }
            assert((pool + x).len() == (p2 + y).len());
        }
        assert(y[0] == 47u8);
        split_unique(pool, p2, x, y);
        assert(x.subrange(0, 8) =~= chunks_lit());
    }
}

pub open spec fn manifests_word() -> Seq<u8> { seq![47u8, 109u8, 97u8, 110u8, 105u8, 102u8, 101u8, 115u8, 116u8, 115u8, 47u8] }
pub open spec fn snapshots_word() -> Seq<u8> { seq![47u8, 115u8, 110u8, 97u8, 112u8, 115u8, 104u8, 111u8, 116u8, 115u8, 47u8] }
pub open spec fn owners_word() -> Seq<u8> {
    seq![47u8, 115u8, 110u8, 97u8, 112u8, 115u8, 104u8, 111u8, 116u8, 45u8, 111u8, 119u8, 110u8, 101u8, 114u8, 115u8, 47u8]
}
pub open spec fn definition_tail() -> Seq<u8> { seq![47u8, 101u8, 120u8, 112u8, 111u8, 114u8, 116u8, 46u8, 106u8, 115u8, 111u8, 110u8] }
pub open spec fn index_word() -> Seq<u8> { seq![47u8, 105u8, 110u8, 100u8, 101u8, 120u8, 47u8, 115u8, 110u8, 97u8, 112u8, 115u8, 104u8, 111u8, 116u8, 115u8, 47u8] }

/// A pack is refused; every key a purge deletes is admitted.
pub proof fn thm_the_keys_a_purge_deletes_are_not_packs(db: Seq<u8>, pool: Seq<u8>, name: Seq<u8>, rest: Seq<u8>, tail: Seq<u8>)
    requires
        no_slash(pool),
        no_slash(name),
    ensures
        is_pack(db, pack_key(db, pool, tail)),
        !is_pack(db, manifest_key(db, pool, name)),
        !is_pack(db, snapshots_key(db, pool, name, rest)),
        !is_pack(db, owner_marker_key(db, pool, name, rest)),
        !is_pack(db, definition_key(db, name)),
        !is_pack(db, index_key(db, name, rest)),
{
    assert(is_pack(db, pack_key(db, pool, tail)));
    let x1 = manifests_word() + name;
    assert(manifest_key(db, pool, name) =~= db + exports_lit() + pool + x1);
    tail_that_is_not_chunks_is_no_pack(db, pool, x1);
    let x2 = snapshots_word() + name + seq![47u8] + rest;
    assert(snapshots_key(db, pool, name, rest) =~= db + exports_lit() + pool + x2);
    tail_that_is_not_chunks_is_no_pack(db, pool, x2);
    let x3 = owners_word() + name + seq![47u8] + rest;
    assert(owner_marker_key(db, pool, name, rest) =~= db + exports_lit() + pool + x3);
    tail_that_is_not_chunks_is_no_pack(db, pool, x3);
    assert(definition_key(db, name) =~= db + exports_lit() + name + definition_tail());
    tail_that_is_not_chunks_is_no_pack(db, name, definition_tail());
    // The index keys leave `{db}` for `/index/`, not `/exports/`.
    let k = index_key(db, name, rest);
    assert(k.subrange(db.len() as int, db.len() + 9int) =~= seq![47u8, 105u8, 110u8, 100u8, 101u8, 120u8, 47u8, 115u8, 110u8]);
    assert(!has_prefix(db, k)) by {
        assert(k[db.len() + 1int] == 105u8);
        assert(exports_lit()[1] == 101u8);
    }
    no_pack_without_prefix(db, k);
}

} // verus!

/// A store object a purge may delete: minted only by [`purge_gate`], which refuses a pack's key. The purge's deleters take it, so none can
/// delete an object the gate did not pass (glidefs's `purge_gate_structure` pins every `delete` in them). A private field, no derive.
pub struct PurgeTarget {
    key: String,
}

impl PurgeTarget {
    /// The object's key.
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// The one mint: `Some` for a key that is no pack's (`db` is the store's `db_path`, the root `{db}/exports/{pool}/chunks/` hangs from).
pub fn purge_gate(db: &str, key: &str) -> Option<PurgeTarget> {
    if not_a_pack(db.as_bytes(), key.as_bytes()) { Some(PurgeTarget { key: key.to_string() }) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pack_key_is_refused_and_a_purge_key_is_not() {
        assert!(purge_gate("db", "db/exports/vol/chunks/0001/00000000000000ab.pack").is_none());
        assert!(purge_gate("db", "db/exports/other-pool/chunks/0000/0000000000000001.pack").is_none());
        assert!(purge_gate("db", "db/exports/vol/manifests/vol").is_some());
        assert!(purge_gate("db", "db/exports/vol/snapshots/vol/00000000000000000007").is_some());
        assert!(purge_gate("db", "db/exports/vol/snapshot-owners/vol/cap-a/00000000000000000007").is_some());
        assert!(purge_gate("db", "db/exports/vol/export.json").is_some());
        assert!(purge_gate("db", "db/index/snapshots/vol@1").is_some());
        // A volume named `chunks` has its definition at `{db}/exports/chunks/export.json`, which is no pack's key.
        assert!(purge_gate("db", "db/exports/chunks/export.json").is_some());
    }
}
