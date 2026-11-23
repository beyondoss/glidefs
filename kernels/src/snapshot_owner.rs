//! The snapshot owner tag: how a list-by-tag answers and how a retried cut answers. Pure over bytes and sequences, proven with
//! Verus (`scripts/verify-kernels.sh`), called verbatim by `glidefs`'s `ContentStore::list_snapshots_owned` and
//! `WriteCache::snapshot_owned`.
//!
//! A cut under owner tag `o` writes the marker `(o, seq)` before its manifest `seq`, so a marker is the record that a cut was
//! attempted under `o`, and a manifest is that it finished.
//!
//! * [`owned_sequences`]: the sequences of exactly the markers whose owner is `o` (not a prefix of it, not another owner's).
//! * [`retry_answer`]: the highest sequence among the owner's markers that has a manifest; `None` (cut afresh) when no marker
//!   has one, so a bare marker of a crashed cut never answers.
//! * Safety `list_is_exactly_the_tagged_cuts` (the model): after any cut, `o`'s listing gains the cut's own sequence and nothing
//!   else, and no other owner's listing changes.
//! * Safety `a_retried_cut_returns_the_same_sequence`: from any state satisfying `inv`, once a cut under `o` answered `a`, every later
//!   cut under `o` answers `a` and changes nothing; `inv` is preserved by every cut (any owner) and every crashed cut.
//!
//! Premises (axioms of the proof, not assumed in it): cuts of one export are serialized (`WriteCache::snapshot_owned` takes the
//! check under `flush_lock`: glidefs test `concurrent_owned_cuts_answer_one_sequence`); the write counter never decreases, so a cut's
//! sequence is at least every marker's; a sequence an owner cut is not deleted (the owner deletes it, and the listing drops it with it).
//!
//! Mutations (`verify-kernels.sh --features mut_x` must go red, and the named glidefs test fail): `mut_list_ignores_owner`,
//! `mut_retry_answers_bare_marker`, `mut_retry_answers_lowest`.

use vstd::prelude::*;

verus! {

/// One owner marker as listed: the owner segment of its key and its sequence.
pub struct Marker<'a> {
    pub owner: &'a [u8],
    pub sequence: u64,
}

/// A sequence an owner cut, and whether its manifest exists.
pub type Owned = (u64, bool);

fn bytes_eq(a: &[u8], b: &[u8]) -> (r: bool)
    ensures
        r == (a@ == b@),
{
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len()
        invariant
            a.len() == b.len(),
            0 <= i <= a.len(),
            forall|j: int| 0 <= j < i ==> a@[j] == b@[j],
        decreases a.len() - i,
    {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    proof {
        assert(a@ =~= b@);
    }
    true
}

pub open spec fn marker_matches(m: Marker<'_>, owner: Seq<u8>) -> bool {
    m.owner@ == owner
}

/// The sequences of the markers `owner` cut: exactly those whose owner segment is `owner`, in listing order.
#[cfg(not(feature = "mut_list_ignores_owner"))]
pub fn owned_sequences(markers: &[Marker<'_>], owner: &[u8]) -> (r: Vec<u64>)
    ensures
        forall|s: u64| r@.contains(s) <==> exists|i: int|
            0 <= i < markers.len() && marker_matches(markers@[i], owner@) && markers@[i].sequence == s,
{
    let mut r: Vec<u64> = Vec::new();
    let mut i = 0;
    while i < markers.len()
        invariant
            0 <= i <= markers.len(),
            forall|s: u64| r@.contains(s) <==> exists|k: int|
                0 <= k < i && marker_matches(markers@[k], owner@) && markers@[k].sequence == s,
        decreases markers.len() - i,
    {
        let ghost old = r@;
        if bytes_eq(markers[i].owner, owner) {
            r.push(markers[i].sequence);
            proof {
                broadcast use vstd::seq_lib::group_seq_properties;
                assert(r@ == old.push(markers@[i as int].sequence));
                assert forall|s: u64| r@.contains(s) <==> exists|k: int|
                    0 <= k < i + 1 && marker_matches(markers@[k], owner@) && markers@[k].sequence == s by {
                    if s == markers@[i as int].sequence {
                        assert(marker_matches(markers@[i as int], owner@));
                    }
                    if exists|k: int| 0 <= k < i + 1 && marker_matches(markers@[k], owner@) && markers@[k].sequence == s {
                        let k = choose|k: int| 0 <= k < i + 1 && marker_matches(markers@[k], owner@) && markers@[k].sequence == s;
                        if k < i as int {
                            assert(old.contains(s));
                        }
                    }
                }
            }
        } else {
            proof {
                assert forall|s: u64| r@.contains(s) <==> exists|k: int|
                    0 <= k < i + 1 && marker_matches(markers@[k], owner@) && markers@[k].sequence == s by {
                    if exists|k: int| 0 <= k < i + 1 && marker_matches(markers@[k], owner@) && markers@[k].sequence == s {
                        let k = choose|k: int| 0 <= k < i + 1 && marker_matches(markers@[k], owner@) && markers@[k].sequence == s;
                        assert(k < i as int);
                    }
                }
            }
        }
        i += 1;
    }
    r
}

/// Mutation: every owner's markers.
#[cfg(feature = "mut_list_ignores_owner")]
pub fn owned_sequences(markers: &[Marker<'_>], owner: &[u8]) -> (r: Vec<u64>)
    ensures
        forall|s: u64| r@.contains(s) <==> exists|i: int|
            0 <= i < markers.len() && marker_matches(markers@[i], owner@) && markers@[i].sequence == s,
{
    let mut r: Vec<u64> = Vec::new();
    let mut i = 0;
    while i < markers.len()
        invariant
            0 <= i <= markers.len(),
        decreases markers.len() - i,
    {
        r.push(markers[i].sequence);
        i += 1;
    }
    r
}

/// `r` answers a retry over `owned`: the highest sequence that has a manifest, `None` when none has.
pub open spec fn is_answer(owned: Seq<Owned>, r: Option<u64>) -> bool {
    match r {
        Some(s) => (exists|i: int| 0 <= i < owned.len() && owned[i] == (s, true))
            && (forall|j: int| 0 <= j < owned.len() && owned[j].1 ==> owned[j].0 <= s),
        None => forall|j: int| 0 <= j < owned.len() ==> !owned[j].1,
    }
}

/// The answer to a retried owned cut: the highest manifested sequence among the owner's markers, or `None` (cut afresh).
#[cfg(not(any(feature = "mut_retry_answers_bare_marker", feature = "mut_retry_answers_lowest")))]
pub fn retry_answer(owned: &[Owned]) -> (r: Option<u64>)
    ensures
        is_answer(owned@, r),
{
    let mut best: Option<u64> = None;
    let mut i = 0;
    while i < owned.len()
        invariant
            0 <= i <= owned.len(),
            is_answer(owned@.subrange(0, i as int), best),
        decreases owned.len() - i,
    {
        let (s, has) = owned[i];
        if has {
            let better = match best {
                Some(b) => s > b,
                None => true,
            };
            if better {
                best = Some(s);
            }
        }
        i += 1;
        proof {
            let sub = owned@.subrange(0, i as int);
            let prev = owned@.subrange(0, i as int - 1);
            assert(sub =~= prev.push(owned@[i as int - 1]));
            assert forall|j: int| 0 <= j < sub.len() && sub[j].1 implies
                (match best { Some(b) => sub[j].0 <= b, None => false }) by {
                if j < prev.len() {
                    assert(sub[j] == prev[j]);
                }
            }
            if let Some(b) = best {
                assert(exists|k: int| 0 <= k < sub.len() && sub[k] == (b, true)) by {
                    if (owned@[i as int - 1].1 && owned@[i as int - 1].0 == b) {
                        assert(sub[i as int - 1] == (b, true));
                    } else {
                        let k = choose|k: int| 0 <= k < prev.len() && prev[k] == (b, true);
                        assert(sub[k] == prev[k]);
                    }
                }
            }
        }
    }
    proof {
        assert(owned@.subrange(0, i as int) =~= owned@);
    }
    best
}

/// Mutation: the lowest manifested sequence.
#[cfg(feature = "mut_retry_answers_lowest")]
pub fn retry_answer(owned: &[Owned]) -> (r: Option<u64>)
    ensures
        is_answer(owned@, r),
{
    let mut best: Option<u64> = None;
    let mut i = 0;
    while i < owned.len()
        invariant
            0 <= i <= owned.len(),
        decreases owned.len() - i,
    {
        let (s, has) = owned[i];
        if has {
            let better = match best {
                Some(b) => s < b,
                None => true,
            };
            if better {
                best = Some(s);
            }
        }
        i += 1;
    }
    best
}

/// Mutation: a bare marker (no manifest) answers too.
#[cfg(feature = "mut_retry_answers_bare_marker")]
pub fn retry_answer(owned: &[Owned]) -> (r: Option<u64>)
    ensures
        is_answer(owned@, r),
{
    let mut best: Option<u64> = None;
    let mut i = 0;
    while i < owned.len()
        invariant
            0 <= i <= owned.len(),
        decreases owned.len() - i,
    {
        let (s, _has) = owned[i];
        let better = match best {
            Some(b) => s > b,
            None => true,
        };
        if better {
            best = Some(s);
        }
        i += 1;
    }
    best
}

// ---- The model the kernels are proven against ----

/// The store's view of one export: the owner markers (`(tag, sequence)`, written before the manifest) and the manifests.
pub struct Store {
    pub markers: ISet<(Seq<u8>, u64)>,
    pub manifests: ISet<u64>,
}

/// The sequences listed under `o`.
pub open spec fn listed(st: Store, o: Seq<u8>) -> ISet<u64> {
    ISet::new(|s: u64| st.markers.contains((o, s)))
}

/// `owned` is the store's view of `o` as `retry_answer` receives it: one entry per marker of `o`, with whether its manifest exists.
pub open spec fn owned_view(st: Store, o: Seq<u8>, owned: Seq<Owned>) -> bool {
    (forall|i: int| 0 <= i < owned.len() ==> st.markers.contains((o, owned[i].0))
        && owned[i].1 == st.manifests.contains(owned[i].0))
        && (forall|s: u64| st.markers.contains((o, s)) ==> exists|i: int| 0 <= i < owned.len() && owned[i].0 == s)
}

/// The write counter never goes below a marker's sequence.
pub open spec fn mono(st: Store, fresh: u64) -> bool {
    forall|m: (Seq<u8>, u64)| st.markers.contains(m) ==> m.1 <= fresh
}

/// A manifested sequence of an owner is at least every sequence the owner has a marker for.
pub open spec fn inv(st: Store) -> bool {
    forall|o: Seq<u8>, s: u64, m: u64|
        st.markers.contains((o, s)) && st.manifests.contains(s) && st.markers.contains((o, m)) ==> m <= s
}

/// A cut under `o` with decision `r`: an answer changes nothing; otherwise it writes marker `(o, fresh)` then manifest `fresh`.
pub open spec fn cut(st: Store, o: Seq<u8>, fresh: u64, r: Option<u64>) -> (Store, u64) {
    match r {
        Some(s) => (st, s),
        None => (
            Store { markers: st.markers.insert((o, fresh)), manifests: st.manifests.insert(fresh) },
            fresh,
        ),
    }
}

/// A cut that crashed after its marker, before its manifest (only reached after a `None` decision).
pub open spec fn crash(st: Store, o: Seq<u8>, fresh: u64) -> Store {
    Store { markers: st.markers.insert((o, fresh)), manifests: st.manifests }
}

/// A manifested marker of `o` that is the unique maximum answers `is_answer`.
proof fn answer_unique(owned: Seq<Owned>, r1: Option<u64>, r2: Option<u64>)
    requires
        is_answer(owned, r1),
        is_answer(owned, r2),
    ensures
        r1 == r2,
{
    match (r1, r2) {
        (Some(a), Some(b)) => {
            let i = choose|i: int| 0 <= i < owned.len() && owned[i] == (a, true);
            let j = choose|j: int| 0 <= j < owned.len() && owned[j] == (b, true);
            assert(owned[i].1 && owned[j].1);
        },
        (Some(a), None) => {
            let i = choose|i: int| 0 <= i < owned.len() && owned[i] == (a, true);
        },
        (None, Some(b)) => {
            let j = choose|j: int| 0 <= j < owned.len() && owned[j] == (b, true);
        },
        (None, None) => {},
    }
}

/// `list_is_exactly_the_tagged_cuts`: after a cut under `o` the listing under `o` is its old listing plus the cut's own sequence, no other
/// owner's listing changes, and `owned_sequences` (the exec listing) returns exactly that set.
pub proof fn list_is_exactly_the_tagged_cuts(
    st: Store,
    o: Seq<u8>,
    o2: Seq<u8>,
    owned: Seq<Owned>,
    fresh: u64,
    r: Option<u64>,
)
    requires
        owned_view(st, o, owned),
        is_answer(owned, r),
    ensures
        ({
            let (st2, ans) = cut(st, o, fresh, r);
            listed(st2, o) =~= listed(st, o).insert(ans) && (o2 != o ==> listed(st2, o2) =~= listed(st, o2))
        }),
{
    match r {
        Some(s) => {
            let i = choose|i: int| 0 <= i < owned.len() && owned[i] == (s, true);
            assert(st.markers.contains((o, s)));
            assert(listed(st, o).insert(s) =~= listed(st, o));
        },
        None => {
            assert(listed(cut(st, o, fresh, r).0, o) =~= listed(st, o).insert(fresh));
            if o2 != o {
                assert(listed(cut(st, o, fresh, r).0, o2) =~= listed(st, o2));
            }
        },
    }
}

/// A cut answers a sequence `o` has a marker for (so the listing contains it), and an answer is already listed.
pub proof fn a_cut_lists_its_answer(st: Store, o: Seq<u8>, owned: Seq<Owned>, fresh: u64, r: Option<u64>)
    requires
        owned_view(st, o, owned),
        is_answer(owned, r),
    ensures
        ({
            let (st2, ans) = cut(st, o, fresh, r);
            listed(st2, o).contains(ans) && st2.manifests.contains(ans)
        }),
{
    if let Some(s) = r {
        let i = choose|i: int| 0 <= i < owned.len() && owned[i] == (s, true);
    }
}

/// `inv` holds after every cut under any owner (with the counter at least every marker) and every crashed cut.
pub proof fn inv_is_preserved(st: Store, o: Seq<u8>, owned: Seq<Owned>, fresh: u64, r: Option<u64>)
    requires
        inv(st),
        mono(st, fresh),
        owned_view(st, o, owned),
        is_answer(owned, r),
    ensures
        inv(cut(st, o, fresh, r).0),
        r is None ==> inv(crash(st, o, fresh)),
{
    match r {
        Some(_) => {},
        None => {
            // No marker of `o` has a manifest.
            assert forall|s: u64| st.markers.contains((o, s)) implies !st.manifests.contains(s) by {
                let i = choose|i: int| 0 <= i < owned.len() && owned[i].0 == s;
            }
            let st2 = cut(st, o, fresh, r).0;
            let stc = crash(st, o, fresh);
            assert forall|o3: Seq<u8>, s: u64, m: u64|
                st2.markers.contains((o3, s)) && st2.manifests.contains(s) && st2.markers.contains((o3, m)) implies m <= s by {
                if s == fresh {
                    if st.markers.contains((o3, m)) {
                        assert(m <= fresh);
                    }
                } else {
                    assert(st.markers.contains((o3, s)) && st.manifests.contains(s));
                    if st.markers.contains((o3, m)) {
                        assert(m <= s);
                    } else {
                        assert(o3 == o);
                    }
                }
            }
            assert forall|o3: Seq<u8>, s: u64, m: u64|
                stc.markers.contains((o3, s)) && stc.manifests.contains(s) && stc.markers.contains((o3, m)) implies m <= s by {
                if st.markers.contains((o3, s)) {
                    if st.markers.contains((o3, m)) {
                        assert(m <= s);
                    } else {
                        assert(o3 == o);
                    }
                } else {
                    assert(o3 == o && s == fresh);
                    if st.markers.contains((o3, m)) {
                        assert(m <= fresh);
                    }
                }
            }
        },
    }
}

/// `a_retried_cut_returns_the_same_sequence`: in any state with `inv` where `o` has a manifested marker `a` (which a cut under `o` that answered or
/// created `a` leaves), every cut under `o`, with whatever fresh sequence, answers `a` and changes nothing.
pub proof fn a_retried_cut_returns_the_same_sequence(
    st: Store,
    o: Seq<u8>,
    a: u64,
    owned: Seq<Owned>,
    fresh: u64,
    r: Option<u64>,
)
    requires
        inv(st),
        st.markers.contains((o, a)),
        st.manifests.contains(a),
        owned_view(st, o, owned),
        is_answer(owned, r),
    ensures
        cut(st, o, fresh, r) == (st, a),
{
    let i = choose|i: int| 0 <= i < owned.len() && owned[i].0 == a;
    assert(owned[i] == (a, true));
    match r {
        Some(s) => {
            let k = choose|k: int| 0 <= k < owned.len() && owned[k] == (s, true);
            assert(st.markers.contains((o, s)) && st.manifests.contains(s));
            assert(a <= s);
            assert(s <= a);
        },
        None => {
            assert(owned[i].1);
        },
    }
}

/// The first cut leaves `inv` and a manifested marker for its answer: it answers `a` and every later cut under `o` answers `a` too
/// (`a_retried_cut_returns_the_same_sequence` applies in the state the cut left, and `inv_is_preserved` carries it through any later cut).
pub proof fn a_cut_leaves_what_a_retry_needs(st: Store, o: Seq<u8>, owned: Seq<Owned>, fresh: u64, r: Option<u64>)
    requires
        inv(st),
        mono(st, fresh),
        owned_view(st, o, owned),
        is_answer(owned, r),
    ensures
        ({
            let (st2, a) = cut(st, o, fresh, r);
            inv(st2) && st2.markers.contains((o, a)) && st2.manifests.contains(a)
        }),
{
    inv_is_preserved(st, o, owned, fresh, r);
    if let Some(s) = r {
        let k = choose|k: int| 0 <= k < owned.len() && owned[k] == (s, true);
    }
}

} // verus!

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_is_the_owners_markers_only() {
        let a: &[u8] = b"cap-1";
        let b: &[u8] = b"cap-10";
        let markers = [
            Marker {
                owner: a,
                sequence: 4,
            },
            Marker {
                owner: b,
                sequence: 5,
            },
            Marker {
                owner: a,
                sequence: 9,
            },
        ];
        assert_eq!(owned_sequences(&markers, a), vec![4, 9]);
        assert_eq!(owned_sequences(&markers, b), vec![5]);
        assert!(owned_sequences(&markers, b"cap-2").is_empty());
    }

    #[test]
    fn retry_answers_the_highest() {
        assert_eq!(retry_answer(&[(3, true), (9, true), (5, true)]), Some(9));
        assert_eq!(
            retry_answer(&[(3, true), (9, false)]),
            Some(3),
            "a bare marker never answers"
        );
        assert_eq!(retry_answer(&[(9, false)]), None);
        assert_eq!(retry_answer(&[]), None);
    }
}
