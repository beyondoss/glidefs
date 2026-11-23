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
//! * From the empty export `inv_holds_from_empty` (cuts, crashed cuts and deletes) and `a_delete_keeps_the_listing_exact`; the write counter is `seq_counter`.
//! * [`markers_to_delete`]: the markers a delete of a sequence removes (every owner's marker of it, no other); `ContentStore::delete_snapshot` calls it. [`owned_cut_plan`]: the cut's plan, `retry_answer` then `owned_cut_decision`, called by `WriteCache::snapshot_owned`; its ensures carry the from-empty `inv` row.
//! * Both in one lemma, `list_by_tag_and_retried_cut`: the property `P.snapshot.glidefs-lists-by-tag-exactly-the-tagged-sequences` states.
//! * Safety `a_retried_cut_returns_the_same_sequence`: from any state satisfying `inv`, once a cut under `o` answered `a`, every later
//!   cut under `o` answers `a` and changes nothing; `inv` is preserved by every cut (any owner) and every crashed cut.
//!
//! Premises (axioms of the proof, not assumed in it): cuts of one export are serialized (`WriteCache::snapshot_owned` takes the
//! check under `flush_lock`: glidefs test `concurrent_owned_cuts_answer_one_sequence`); the write counter never decreases, so a cut's
//! sequence is at least every marker's; a sequence an owner cut is not deleted (the owner deletes it, and the listing drops it with it).
//!
//! Mutations (`verify-kernels.sh --features mut_x` must go red, and the named glidefs test fail): `mut_list_ignores_owner`,
//! `mut_retry_answers_bare_marker`, `mut_retry_answers_lowest`, `mut_closed_tag_still_cuts`, `mut_closed_marker_removed_early`, `mut_expired_cut_writes`, `mut_store_config_unchecked`.
//!
//! The tag fence: `close` (under the flush lock) marks a tag closed; a cut under a closed tag that cut nothing is refused (`owned_cut_decision`), so
//! `a_closed_tags_listing_is_final`: after the close returns, the tag's listing only shrinks. A closed marker is removed when older than the
//! request bound (`closed_marker_removable`, `a_removed_closed_marker_has_no_late_cut_to_stop`) or with its export.

use vstd::prelude::*;

/// The longest an owned cut may run before it writes: `WriteCache::snapshot_owned` refuses to write its marker or its manifest once the request
/// is older than this, checked under the flush lock (`cut_may_write`), and the router cancels the request at it. instd's client times out at the
/// same bound.
pub const OWNED_CUT_REQUEST_BOUND_SECS: u64 = 900;

/// S3 rejects a SigV4-signed request whose `X-Amz-Date` is more than 15 minutes from S3's clock (`RequestTimeTooSkewed`): axiom AX-s3-sigv4-skew.
pub const SIGV4_WINDOW_SECS: u64 = 900;

/// The most GlideFS's clock and S3's clock differ by (chrony/NTP): FA-clock-sync.
pub const CLOCK_SKEW_SECS: u64 = 60;

/// The object-store client's request timeout, and the longest a store write may take to be signed (credential fetch) or to send its body:
/// the largest `request_timeout` a store config may carry (`store_config_admitted`; GlideFS refuses a larger one at startup).
pub const REQUEST_TIMEOUT_SECS: u64 = 300;

/// The longest, after a cut's write passed `cut_may_write`, that the write can still be applied by the store. The client signs a request once
/// (object_store `aws/client.rs` `with_aws_sigv4` runs before `.retryable()`, and the retry loop re-sends the same signed request), so the date
/// in its signature is fixed at signing, which is at most one request timeout after the check (credential fetch). S3 applies a request only if
/// that date is within 15 minutes (+ clock skew) of its arrival (AX-s3-sigv4-skew), and only on receiving the whole body (AX-s3-whole-body); the
/// body takes at most one request timeout after arrival, because the client abandons the request then and stops sending
/// (`parse_object_store::an_abandoned_put_stops_sending_its_body`).
pub const STORE_WRITE_SLACK_SECS: u64 = REQUEST_TIMEOUT_SECS + SIGV4_WINDOW_SECS + CLOCK_SKEW_SECS + REQUEST_TIMEOUT_SECS;

const _: () = assert!(STORE_WRITE_SLACK_SECS == 2 * 300 + 900 + 60, "store_config_admitted's 300 is REQUEST_TIMEOUT_SECS");

/// The age a closed marker must pass before it is removed: the bound plus the slack.
pub const CLOSED_MARKER_KEEP_SECS: u64 = OWNED_CUT_REQUEST_BOUND_SECS + STORE_WRITE_SLACK_SECS;

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
    /// Owner tags that were closed (`close`): the fence a late cut meets.
    pub closed: ISet<Seq<u8>>,
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
            Store { markers: st.markers.insert((o, fresh)), manifests: st.manifests.insert(fresh), closed: st.closed },
            fresh,
        ),
    }
}

/// A cut that crashed after its marker, before its manifest (only reached after a `None` decision).
pub open spec fn crash(st: Store, o: Seq<u8>, fresh: u64) -> Store {
    Store { markers: st.markers.insert((o, fresh)), manifests: st.manifests, closed: st.closed }
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

/// The row's whole text, in one lemma: from any state with `inv` (and the counter at least every marker), a cut under `o` leaves the listing
/// under `o` exactly its old listing plus the cut's own sequence `a` and no other owner's listing changed (list-by-tag returns exactly the
/// sequences cut with that tag), and any cut under `o` retried in the state it left, with whatever fresh sequence, answers the same `a` and
/// changes nothing (a cut retried with the same tag returns the same sequence).
pub proof fn list_by_tag_and_retried_cut(
    st: Store,
    o: Seq<u8>,
    o2: Seq<u8>,
    owned: Seq<Owned>,
    fresh: u64,
    r: Option<u64>,
    owned2: Seq<Owned>,
    fresh2: u64,
    r2: Option<u64>,
)
    requires
        inv(st),
        mono(st, fresh),
        owned_view(st, o, owned),
        is_answer(owned, r),
        owned_view(cut(st, o, fresh, r).0, o, owned2),
        is_answer(owned2, r2),
    ensures
        ({
            let (st2, a) = cut(st, o, fresh, r);
            listed(st2, o) =~= listed(st, o).insert(a) && (o2 != o ==> listed(st2, o2) =~= listed(st, o2))
                && cut(st2, o, fresh2, r2) == (st2, a)
        }),
{
    list_is_exactly_the_tagged_cuts(st, o, o2, owned, fresh, r);
    a_cut_leaves_what_a_retry_needs(st, o, owned, fresh, r);
    let (st2, a) = cut(st, o, fresh, r);
    a_retried_cut_returns_the_same_sequence(st2, o, a, owned2, fresh2, r2);
}

// ---- Reachability from the empty export, and delete ----

/// What the export's owner state can do: a cut under a tag, a cut that crashed after its marker, a delete of one sequence.
pub enum Event {
    Cut { owner: Seq<u8>, fresh: u64 },
    Crash { owner: Seq<u8>, fresh: u64 },
    Delete { sequence: u64 },
    /// `POST .../snapshot-owners/{owner}/close`: under the flush lock, so after every cut of the tag ahead of it.
    Close { owner: Seq<u8> },
}

pub open spec fn empty_store() -> Store {
    Store { markers: ISet::empty(), manifests: ISet::empty(), closed: ISet::empty() }
}

/// The decision `retry_answer` makes, over the store: the manifested marker of `owner` that is at least every marker of `owner` (unique under
/// `inv`), else `None`.
pub open spec fn decide(st: Store, o: Seq<u8>) -> Option<u64> {
    if exists|s: u64| #[trigger] st.markers.contains((o, s)) && st.manifests.contains(s)
        && (forall|m: u64| st.markers.contains((o, m)) ==> m <= s) {
        Some(choose|s: u64| #[trigger] st.markers.contains((o, s)) && st.manifests.contains(s)
            && (forall|m: u64| st.markers.contains((o, m)) ==> m <= s))
    } else {
        None
    }
}

/// A delete removes the sequence's manifest and every owner's marker of it.
pub open spec fn delete(st: Store, seq: u64) -> Store {
    Store {
        markers: ISet::new(|m: (Seq<u8>, u64)| st.markers.contains(m) && m.1 != seq),
        manifests: ISet::new(|s: u64| st.manifests.contains(s) && s != seq),
        closed: st.closed,
    }
}

pub open spec fn enabled(st: Store, ev: Event) -> bool {
    match ev {
        Event::Cut { owner, fresh } => mono(st, fresh),
        // A cut that crashed after its marker held the lock until it died, so it ran before any close of its tag.
        Event::Crash { owner, fresh } => mono(st, fresh) && decide(st, owner) is None && !st.closed.contains(owner),
        Event::Delete { sequence } => true,
        Event::Close { owner } => true,
    }
}

pub open spec fn step(st: Store, ev: Event) -> Store {
    match ev {
        // `owned_cut_decision`: an answer is answered; otherwise a closed tag is refused (nothing written) and an open one is cut.
        Event::Cut { owner, fresh } => match cut_decision_spec(st.closed.contains(owner), decide(st, owner)) {
            CutDecision::Cut => cut(st, owner, fresh, None).0,
            _ => st,
        },
        Event::Crash { owner, fresh } => crash(st, owner, fresh),
        Event::Delete { sequence } => delete(st, sequence),
        Event::Close { owner } => Store { closed: st.closed.insert(owner), ..st },
    }
}

/// The store after `trace`, every step enabled in the store before it.
pub open spec fn run(trace: Seq<Event>) -> Store
    decreases trace.len(),
{
    if trace.len() == 0 {
        empty_store()
    } else {
        step(run(trace.drop_last()), trace.last())
    }
}

pub open spec fn trace_ok(trace: Seq<Event>) -> bool
    decreases trace.len(),
{
    if trace.len() == 0 {
        true
    } else {
        trace_ok(trace.drop_last()) && enabled(run(trace.drop_last()), trace.last())
    }
}

/// Under `inv`, `decide` is `None` exactly when no marker of `o` has a manifest.
proof fn decide_none_iff_unmanifested(st: Store, o: Seq<u8>)
    requires
        inv(st),
    ensures
        decide(st, o) is None <==> (forall|s: u64| st.markers.contains((o, s)) ==> !st.manifests.contains(s)),
{
    if exists|s0: u64| st.markers.contains((o, s0)) && st.manifests.contains(s0) {
        let s0 = choose|s0: u64| st.markers.contains((o, s0)) && st.manifests.contains(s0);
        assert(forall|m: u64| st.markers.contains((o, m)) ==> m <= s0);
        assert(st.markers.contains((o, s0)) && st.manifests.contains(s0) && (forall|m: u64| st.markers.contains((o, m)) ==> m <= s0));
    }
}

/// `inv` for a store whose only new marker is `(o, fresh)` and whose manifests are `st`'s, or those and `fresh`, when `o` had no manifested marker.
proof fn none_preserves_inv(st: Store, o: Seq<u8>, fresh: u64)
    requires
        inv(st),
        mono(st, fresh),
        forall|s: u64| st.markers.contains((o, s)) ==> !st.manifests.contains(s),
    ensures
        inv(cut(st, o, fresh, None).0),
        inv(crash(st, o, fresh)),
{
    let st2 = cut(st, o, fresh, None).0;
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
}

/// `inv` survives every enabled event.
pub proof fn inv_survives_every_event(st: Store, ev: Event)
    requires
        inv(st),
        enabled(st, ev),
    ensures
        inv(step(st, ev)),
{
    match ev {
        Event::Cut { owner, fresh } => {
            decide_none_iff_unmanifested(st, owner);
            if decide(st, owner) is None && !st.closed.contains(owner) {
                none_preserves_inv(st, owner, fresh);
            }
        },
        Event::Close { owner } => {
            let st2 = step(st, ev);
            assert(st2.markers == st.markers && st2.manifests == st.manifests);
        },
        Event::Crash { owner, fresh } => {
            decide_none_iff_unmanifested(st, owner);
            none_preserves_inv(st, owner, fresh);
        },
        Event::Delete { sequence } => {
            let st2 = delete(st, sequence);
            assert forall|o: Seq<u8>, s: u64, m: u64|
                st2.markers.contains((o, s)) && st2.manifests.contains(s) && st2.markers.contains((o, m)) implies m <= s by {
                assert(st.markers.contains((o, s)) && st.manifests.contains(s) && st.markers.contains((o, m)));
            }
        },
    }
}

/// `inv_holds_from_empty`: the export that starts with no markers satisfies `inv` after every enabled trace of cuts, crashed cuts and deletes.
pub proof fn inv_holds_from_empty(trace: Seq<Event>)
    requires
        trace_ok(trace),
    ensures
        inv(run(trace)),
    decreases trace.len(),
{
    if trace.len() > 0 {
        inv_holds_from_empty(trace.drop_last());
        inv_survives_every_event(run(trace.drop_last()), trace.last());
    }
}

/// A delete removes the sequence from every owner's listing and leaves every other sequence listed.
pub proof fn a_delete_drops_exactly_its_sequence(st: Store, seq: u64, o: Seq<u8>)
    ensures
        listed(delete(st, seq), o) =~= listed(st, o).remove(seq),
        delete(st, seq).manifests =~= st.manifests.remove(seq),
{
}

/// A retry after a delete of some other sequence still answers the owner's cut: `a` is still a manifested marker and `inv` still holds.
pub proof fn a_delete_of_another_sequence_keeps_the_answer(st: Store, o: Seq<u8>, a: u64, seq: u64)
    requires
        inv(st),
        st.markers.contains((o, a)),
        st.manifests.contains(a),
        seq != a,
    ensures
        ({
            let st2 = delete(st, seq);
            inv(st2) && st2.markers.contains((o, a)) && st2.manifests.contains(a)
        }),
{
    inv_survives_every_event(st, Event::Delete { sequence: seq });
}

/// What `retry_answer` returns over the store's view of `o` is `decide`: the trace model runs the decision the exec fn makes.
pub proof fn decide_is_the_exec_answer(st: Store, o: Seq<u8>, owned: Seq<Owned>, r: Option<u64>)
    requires
        inv(st),
        owned_view(st, o, owned),
        is_answer(owned, r),
    ensures
        r == decide(st, o),
{
    match r {
        Some(a) => {
            let i = choose|i: int| 0 <= i < owned.len() && owned[i] == (a, true);
            assert(st.markers.contains((o, a)) && st.manifests.contains(a));
            assert(forall|m: u64| st.markers.contains((o, m)) ==> m <= a);
            let s = choose|s: u64| st.markers.contains((o, s)) && st.manifests.contains(s)
                && (forall|m: u64| st.markers.contains((o, m)) ==> m <= s);
            let j = choose|j: int| 0 <= j < owned.len() && owned[j].0 == s;
            assert(owned[j].1);
            assert(s <= a && a <= s);
        },
        None => {
            assert forall|s: u64| st.markers.contains((o, s)) implies !st.manifests.contains(s) by {
                let i = choose|i: int| 0 <= i < owned.len() && owned[i].0 == s;
            }
            decide_none_iff_unmanifested(st, o);
        },
    }
}

/// The delete row's whole text: a delete of `seq` removes it from every owner's listing and leaves every other sequence listed, and a retried cut
/// under `o` still answers its cut `a` when `a` is not the deleted sequence (`inv` and `a`'s marker and manifest survive).
pub proof fn a_delete_keeps_the_listing_exact(st: Store, seq: u64, o: Seq<u8>, a: u64)
    requires
        inv(st),
        st.markers.contains((o, a)),
        st.manifests.contains(a),
        seq != a,
    ensures
        listed(delete(st, seq), o) =~= listed(st, o).remove(seq),
        ({
            let st2 = delete(st, seq);
            inv(st2) && st2.markers.contains((o, a)) && st2.manifests.contains(a)
        }),
{
    a_delete_drops_exactly_its_sequence(st, seq, o);
    a_delete_of_another_sequence_keeps_the_answer(st, o, a, seq);
}


// ---- Deleting a sequence: which markers go ----

/// The set of `(owner, sequence)` the listed markers name.
pub open spec fn marker_set(ms: Seq<Marker<'_>>) -> ISet<(Seq<u8>, u64)> {
    ISet::new(|p: (Seq<u8>, u64)| exists|i: int| 0 <= i < ms.len() && p == (ms[i].owner@, ms[i].sequence))
}

/// The set of `(owner, sequence)` the listed markers name once those at `gone` are deleted.
pub open spec fn kept_set(ms: Seq<Marker<'_>>, gone: Seq<usize>) -> ISet<(Seq<u8>, u64)> {
    ISet::new(|p: (Seq<u8>, u64)| exists|i: int| 0 <= i < ms.len() && !gone.contains(i as usize) && p == (ms[i].owner@, ms[i].sequence))
}

/// What a delete of `seq` leaves, for any store whose markers are the listed ones and the markers at `gone` removed.
pub open spec fn delete_row_holds(ms: Seq<Marker<'_>>, gone: Seq<usize>, seq: u64, st: Store, o: Seq<u8>, a: u64) -> bool {
    st.markers == marker_set(ms) ==> ({
        let st2 = Store { markers: kept_set(ms, gone), manifests: st.manifests.remove(seq), closed: st.closed };
        &&& listed(st2, o) =~= listed(st, o).remove(seq)
        &&& (inv(st) && st.markers.contains((o, a)) && st.manifests.contains(a) && seq != a ==> inv(st2) && st2.markers.contains((o, a))
            && st2.manifests.contains(a))
    })
}

/// The markers a delete of `seq` removes: every owner's marker of that sequence, and no other (`delete_snapshot` removes exactly these, after the
/// manifest). The delete row's whole text, over any store whose markers are the listed ones: the listing of every owner loses `seq` and keeps every
/// other sequence, and a retried cut under `o` still answers its cut `a` when `a` is not the deleted sequence (`inv`, `a`'s marker and manifest survive).
#[cfg(not(any(feature = "mut_delete_keeps_markers", feature = "mut_delete_one_owner")))]
pub fn markers_to_delete(markers: &[Marker<'_>], seq: u64) -> (gone: Vec<usize>)
    ensures
        forall|k: int| 0 <= k < gone.len() ==> gone@[k] < markers.len() && markers@[gone@[k] as int].sequence == seq,
        forall|i: int| 0 <= i < markers.len() && markers@[i].sequence == seq ==> gone@.contains(i as usize),
        forall|st: Store, o: Seq<u8>, a: u64| #[trigger] delete_row_holds(markers@, gone@, seq, st, o, a),
{
    let mut gone: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < markers.len()
        invariant
            0 <= i <= markers.len(),
            forall|k: int| 0 <= k < gone.len() ==> gone@[k] < i && markers@[gone@[k] as int].sequence == seq,
            forall|j: int| 0 <= j < i && markers@[j].sequence == seq ==> gone@.contains(j as usize),
        decreases markers.len() - i,
    {
        if markers[i].sequence == seq {
            let ghost old = gone@;
            gone.push(i);
            proof {
                broadcast use vstd::seq_lib::group_seq_properties;
                assert(gone@ == old.push(i as usize));
                assert forall|j: int| 0 <= j < i + 1 && markers@[j].sequence == seq implies gone@.contains(j as usize) by {
                    if j == i as int {
                        assert(gone@[old.len() as int] == i);
                    } else {
                        assert(old.contains(j as usize));
                    }
                }
            }
        }
        i += 1;
    }
    proof {
        assert forall|st: Store, o: Seq<u8>, a: u64| #[trigger] delete_row_holds(markers@, gone@, seq, st, o, a) by {
            if st.markers == marker_set(markers@) {
            let st2 = Store { markers: kept_set(markers@, gone@), manifests: st.manifests.remove(seq), closed: st.closed };
            let d = delete(st, seq);
            assert(st2.markers =~= d.markers) by {
                assert forall|p: (Seq<u8>, u64)| st2.markers.contains(p) == d.markers.contains(p) by {
                    if st2.markers.contains(p) {
                        let i = choose|i: int| 0 <= i < markers.len() && !gone@.contains(i as usize) && p == (markers@[i].owner@, markers@[i].sequence);
                        assert(markers@[i].sequence != seq);
                        assert(st.markers.contains(p));
                    }
                    if d.markers.contains(p) {
                        let i = choose|i: int| 0 <= i < markers.len() && p == (markers@[i].owner@, markers@[i].sequence);
                        assert(markers@[i].sequence != seq);
                        if gone@.contains(i as usize) {
                            let k = choose|k: int| 0 <= k < gone.len() && gone@[k] == i as usize;
                            assert(markers@[gone@[k] as int].sequence == seq);
                        }
                    }
                }
            }
            assert(st2.manifests =~= d.manifests);
            assert(st2 == d);
            if inv(st) && st.markers.contains((o, a)) && st.manifests.contains(a) && seq != a {
                a_delete_keeps_the_listing_exact(st, seq, o, a);
            }
            a_delete_drops_exactly_its_sequence(st, seq, o);
            }
        }
    }
    gone
}

/// Mutation: the markers stay.
#[cfg(feature = "mut_delete_keeps_markers")]
pub fn markers_to_delete(markers: &[Marker<'_>], seq: u64) -> (gone: Vec<usize>)
    ensures
        forall|k: int| 0 <= k < gone.len() ==> gone@[k] < markers.len() && markers@[gone@[k] as int].sequence == seq,
        forall|i: int| 0 <= i < markers.len() && markers@[i].sequence == seq ==> gone@.contains(i as usize),
        forall|st: Store, o: Seq<u8>, a: u64| #[trigger] delete_row_holds(markers@, gone@, seq, st, o, a),
{
    Vec::new()
}

/// Mutation: only the first marker of the sequence goes (another owner's marker of it stays listed).
#[cfg(feature = "mut_delete_one_owner")]
pub fn markers_to_delete(markers: &[Marker<'_>], seq: u64) -> (gone: Vec<usize>)
    ensures
        forall|k: int| 0 <= k < gone.len() ==> gone@[k] < markers.len() && markers@[gone@[k] as int].sequence == seq,
        forall|i: int| 0 <= i < markers.len() && markers@[i].sequence == seq ==> gone@.contains(i as usize),
        forall|st: Store, o: Seq<u8>, a: u64| #[trigger] delete_row_holds(markers@, gone@, seq, st, o, a),
{
    let mut gone: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < markers.len()
        invariant
            0 <= i <= markers.len(),
            forall|k: int| 0 <= k < gone.len() ==> gone@[k] < i && markers@[gone@[k] as int].sequence == seq,
        decreases markers.len() - i,
    {
        if markers[i].sequence == seq && gone.len() == 0 {
            gone.push(i);
        }
        i += 1;
    }
    gone
}

// ---- The tag fence ----

/// What a cut under an owner tag does, decided under the flush lock from whether the tag is closed and what the owner already cut.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CutDecision {
    /// The owner already cut this sequence: answer it.
    Answer(u64),
    /// The tag is closed and nothing was cut: write nothing, answer `owner_closed`.
    Refuse,
    /// Cut afresh.
    Cut,
}

/// The decision over the model: an answer is answered whether or not the tag is closed; a closed tag with no cut is refused; an open one is cut.
pub open spec fn cut_decision_spec(closed: bool, answer: Option<u64>) -> CutDecision {
    match answer {
        Some(s) => CutDecision::Answer(s),
        None => if closed { CutDecision::Refuse } else { CutDecision::Cut },
    }
}

/// An answer is answered whether or not the tag is closed; a closed tag with no cut is refused; an open one is cut.
#[cfg(not(feature = "mut_closed_tag_still_cuts"))]
pub fn owned_cut_decision(closed: bool, answer: Option<u64>) -> (d: CutDecision)
    ensures
        d == cut_decision_spec(closed, answer),
        answer is Some ==> d == CutDecision::Answer(answer->Some_0),
        answer is None && closed ==> d == CutDecision::Refuse,
        answer is None && !closed ==> d == CutDecision::Cut,
{
    match answer {
        Some(s) => CutDecision::Answer(s),
        None => if closed { CutDecision::Refuse } else { CutDecision::Cut },
    }
}

/// Mutation: a closed tag is cut anyway.
#[cfg(feature = "mut_closed_tag_still_cuts")]
pub fn owned_cut_decision(closed: bool, answer: Option<u64>) -> (d: CutDecision)
    ensures
        d == cut_decision_spec(closed, answer),
        answer is Some ==> d == CutDecision::Answer(answer->Some_0),
        answer is None && closed ==> d == CutDecision::Refuse,
        answer is None && !closed ==> d == CutDecision::Cut,
{
    match answer {
        Some(s) => CutDecision::Answer(s),
        None => CutDecision::Cut,
    }
}

/// What the cut row says of one cut over the store `st` it ran on: the plan is the model's decision, and the cut it plans leaves `inv`.
pub open spec fn plan_row_holds(owned: Seq<Owned>, closed: bool, d: CutDecision, st: Store, o: Seq<u8>, fresh: u64) -> bool {
    (inv(st) && owned_view(st, o, owned) && closed == st.closed.contains(o) && mono(st, fresh)) ==> {
        &&& d == cut_decision_spec(st.closed.contains(o), decide(st, o))
        &&& inv(step(st, Event::Cut { owner: o, fresh }))
        &&& (d is Cut ==> step(st, Event::Cut { owner: o, fresh }) == cut(st, o, fresh, None).0)
        &&& (!(d is Cut) ==> step(st, Event::Cut { owner: o, fresh }) == st)
    }
}

/// The cut's plan under the flush lock, from the owner's markers (`owned`, with whether each has a manifest) and whether the tag is closed: the
/// highest manifested sequence answers a retry (`retry_answer`), else a closed tag is refused and an open one is cut (`owned_cut_decision`).
/// The row's whole text: from an export with no owner markers, every enabled sequence of cuts (each decided by this plan) and crashed cuts leaves
/// `inv`: an owner's manifested sequence is at least every sequence the owner has a marker for; and one cut decided by this plan, over any
/// store the owner's view came from, leaves `inv` and writes exactly what the plan says.
pub fn owned_cut_plan(owned: &[Owned], closed: bool) -> (d: CutDecision)
    ensures
        forall|st: Store, o: Seq<u8>, fresh: u64| #[trigger] plan_row_holds(owned@, closed, d, st, o, fresh),
        forall|trace: Seq<Event>| trace_ok(trace) ==> #[trigger] inv(run(trace)),
{
    let answer = retry_answer(owned);
    let d = owned_cut_decision(closed, answer);
    proof {
        assert forall|st: Store, o: Seq<u8>, fresh: u64| #[trigger] plan_row_holds(owned@, closed, d, st, o, fresh) by {
            if inv(st) && owned_view(st, o, owned@) && closed == st.closed.contains(o) && mono(st, fresh) {
                decide_is_the_exec_answer(st, o, owned@, answer);
                inv_survives_every_event(st, Event::Cut { owner: o, fresh });
            }
        }
        assert forall|trace: Seq<Event>| trace_ok(trace) implies #[trigger] inv(run(trace)) by {
            inv_holds_from_empty(trace);
        }
    }
    d
}

/// Like `run`, from any store.
pub open spec fn run_from(st: Store, trace: Seq<Event>) -> Store
    decreases trace.len(),
{
    if trace.len() == 0 {
        st
    } else {
        step(run_from(st, trace.drop_last()), trace.last())
    }
}

pub open spec fn ok_from(st: Store, trace: Seq<Event>) -> bool
    decreases trace.len(),
{
    if trace.len() == 0 {
        true
    } else {
        ok_from(st, trace.drop_last()) && enabled(run_from(st, trace.drop_last()), trace.last())
    }
}

/// One enabled event on a store whose tag `o` is closed leaves `o` closed and its listing no larger.
proof fn a_closed_tag_gains_nothing_in_one_step(st: Store, ev: Event, o: Seq<u8>)
    requires
        inv(st),
        st.closed.contains(o),
        enabled(st, ev),
    ensures
        step(st, ev).closed.contains(o),
        forall|s: u64| step(st, ev).markers.contains((o, s)) ==> st.markers.contains((o, s)),
{
    match ev {
        Event::Cut { owner, fresh } => {
            decide_none_iff_unmanifested(st, owner);
        },
        Event::Crash { owner, fresh } => {},
        Event::Delete { sequence } => {},
        Event::Close { owner } => {},
    }
}

/// `a_closed_tags_listing_is_final` (the ordering proof): once `o` is closed, no enabled run of cuts (any owner), crashed cuts, deletes and
/// closes ever puts a sequence in `o`'s listing that was not already there: a late cut under a closed tag is refused.
pub proof fn a_closed_tags_listing_is_final(st: Store, trace: Seq<Event>, o: Seq<u8>)
    requires
        inv(st),
        st.closed.contains(o),
        ok_from(st, trace),
    ensures
        run_from(st, trace).closed.contains(o),
        forall|s: u64| run_from(st, trace).markers.contains((o, s)) ==> st.markers.contains((o, s)),
    decreases trace.len(),
{
    if trace.len() > 0 {
        let prev = run_from(st, trace.drop_last());
        a_closed_tags_listing_is_final(st, trace.drop_last(), o);
        inv_from_inv_run(st, trace.drop_last());
        a_closed_tag_gains_nothing_in_one_step(prev, trace.last(), o);
    }
}

/// `inv` holds along a run from any store that has it.
proof fn inv_from_inv_run(st: Store, trace: Seq<Event>)
    requires
        inv(st),
        ok_from(st, trace),
    ensures
        inv(run_from(st, trace)),
    decreases trace.len(),
{
    if trace.len() > 0 {
        inv_from_inv_run(st, trace.drop_last());
        inv_survives_every_event(run_from(st, trace.drop_last()), trace.last());
    }
}

/// A closed tag's marker may be removed once no request that began before the close can still be running: every owned cut is bounded by
/// `bound` seconds (the server cancels it), and a cut that begins after the close is refused. Removable when the marker is older than the bound.
#[cfg(not(feature = "mut_closed_marker_removed_early"))]
pub fn closed_marker_removable(age_secs: u64, bound_secs: u64) -> (r: bool)
    ensures
        r == (age_secs > bound_secs),
{
    age_secs > bound_secs
}

/// Mutation: removable at any age.
#[cfg(feature = "mut_closed_marker_removed_early")]
pub fn closed_marker_removable(age_secs: u64, bound_secs: u64) -> (r: bool)
    ensures
        r == (age_secs > bound_secs),
{
    true
}

/// A cut may write its marker or its manifest only while its request is younger than the bound. Checked under the flush lock before each write.
#[cfg(not(feature = "mut_expired_cut_writes"))]
pub fn cut_may_write(elapsed_secs: u64, bound_secs: u64) -> (r: bool)
    ensures
        r == (elapsed_secs < bound_secs),
{
    elapsed_secs < bound_secs
}

/// Mutation: an expired cut writes anyway.
#[cfg(feature = "mut_expired_cut_writes")]
pub fn cut_may_write(elapsed_secs: u64, bound_secs: u64) -> (r: bool)
    ensures
        r == (elapsed_secs < bound_secs),
{
    true
}

/// A store config is admitted only if its request timeout fits the slack: `2 * request_timeout + 15 min + clock skew <= STORE_WRITE_SLACK_SECS`.
/// GlideFS refuses a config that does not at startup, so no config can break the premise of `a_removed_closed_marker_has_no_late_cut_to_stop`.
#[cfg(not(feature = "mut_store_config_unchecked"))]
pub fn store_config_admitted(request_timeout_secs: u64) -> (r: bool)
    ensures
        r == (request_timeout_secs <= 300),
{
    request_timeout_secs <= 300
}

/// Mutation: every config is admitted.
#[cfg(feature = "mut_store_config_unchecked")]
pub fn store_config_admitted(request_timeout_secs: u64) -> (r: bool)
    ensures
        r == (request_timeout_secs <= 300),
{
    true
}

/// `a_removed_closed_marker_has_no_late_cut_to_stop`: a cut that started no later than the close wrote only while younger than `bound`
/// (`cut_may_write`). Its write is signed within `cred` of that check, arrives within `skew_window` (15 minutes + clock skew) of its signing
/// (AX-s3-sigv4-skew), and lands within `body` of arriving (AX-s3-whole-body, and the client stops sending at its timeout). So everything it will
/// ever write has landed before the marker is removable: older than `bound + cred + skew_window + body`. A cut that starts after the close meets
/// the closed tag and writes nothing (`a_closed_tags_listing_is_final`).
pub proof fn a_removed_closed_marker_has_no_late_cut_to_stop(
    start: int,
    close_at: int,
    write_at: int,
    signed_at: int,
    arrives_at: int,
    lands_at: int,
    now: int,
    bound: int,
    cred: int,
    skew_window: int,
    body: int,
)
    requires
        start <= close_at,
        write_at - start < bound,
        write_at <= signed_at,
        signed_at <= write_at + cred,
        arrives_at <= signed_at + skew_window,
        lands_at <= arrives_at + body,
        now - close_at > bound + cred + skew_window + body,
    ensures
        lands_at < now,
{
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
