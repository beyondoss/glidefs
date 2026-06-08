//! Static boot-set derivation for EROFS build-time layout reorder.
//!
//! Computes the file paths a container's boot will touch — **without executing
//! it** — so `glidefs bless --oci` (EROFS) can lay them down contiguously at the
//! front of the image via [`WriterOption::PriorityOrder`]. A contiguous boot set
//! collapses a cold boot's scattered S3 round-trips into a single range GET (see
//! `research-bootset/FINDINGS.md`: 4.5–30× faster cold boot on real app images).
//!
//! The derivation is the dynamic loader's own logic, run statically over the
//! merged OCI rootfs:
//!   1. the **entrypoint** binary (`config.Entrypoint`/`Cmd`, resolved via `PATH`),
//!   2. its ELF **interpreter** (`PT_INTERP`, i.e. `ld.so`),
//!   3. the transitive **`DT_NEEDED` shared-library closure** (honoring
//!      `DT_RUNPATH`/`DT_RPATH` + `$ORIGIN` and the standard amd64 search dirs),
//!   4. the dynamic linker cache (`/etc/ld.so.cache`) every dynamic exec reads,
//!   5. the **init** binary (`/sbin/init` or `systemd`) + a few early units.
//!
//! Best-effort and non-fatal: any parse/lookup failure just yields a shorter (or
//! empty) list, in which case bless falls back to the default DFS layout (no
//! prefetch hint). Memory is bounded — only ELF files under standard
//! library/binary directories are buffered, never the whole image.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Seek, SeekFrom};

use tracing::{debug, warn};

/// Per-file ELF cache budget (skip any single file larger than this).
const MAX_ELF_BYTES: u64 = 128 * 1024 * 1024;
/// Total in-memory ELF cache budget across the whole image.
const ELF_CACHE_BUDGET: u64 = 512 * 1024 * 1024;

/// Standard directories that hold executables and shared libraries. Only files
/// under these are buffered for ELF inspection — this bounds memory to the
/// system toolchain (tens of MiB) rather than the whole image's data.
const STD_DIRS: &[&str] = &[
    "bin/", "sbin/", "usr/bin/", "usr/sbin/", "usr/local/bin/", "usr/local/sbin/", "lib/",
    "lib32/", "lib64/", "usr/lib/", "usr/lib32/", "usr/lib64/", "usr/local/lib/",
];

/// Default shared-library search path for linux/amd64 (the platform `resolve`
/// selects). Mirrors glibc's built-in trusted dirs + the Debian/Ubuntu triplet.
const DEFAULT_LIB_DIRS: &[&str] = &[
    "lib/x86_64-linux-gnu",
    "usr/lib/x86_64-linux-gnu",
    "lib64",
    "usr/lib64",
    "lib",
    "usr/lib",
    "usr/local/lib",
    "usr/local/lib/x86_64-linux-gnu",
];

/// Default `PATH` when the image config sets none.
const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// init binaries to seed, in preference order.
const INIT_CANDIDATES: &[&str] =
    &["sbin/init", "usr/sbin/init", "lib/systemd/systemd", "usr/lib/systemd/systemd"];

/// Small, always-early files worth co-locating if present (the linker cache and
/// the systemd targets pulled in on every boot). Added as plain paths; missing
/// ones are silently dropped.
const EARLY_FILES: &[&str] = &[
    "etc/ld.so.cache",
    "usr/lib/systemd/system/default.target",
    "usr/lib/systemd/system/basic.target",
    "usr/lib/systemd/system/sysinit.target",
];

/// Statically derive the boot working set's file paths from an OCI image, in
/// priority order, as **absolute** paths (leading `/`). `config` is the raw OCI
/// image-config JSON; `layers` are the decompressed layer tar streams in
/// bottom-to-top order. Best-effort: never errors, returns `Vec::new()` when it
/// cannot derive anything (caller then skips the reorder).
///
/// Leaves the layer readers rewound to the start so the caller's converter can
/// re-scan them.
pub fn derive_boot_paths<R: Read + Seek>(config: &[u8], layers: &mut [R]) -> Vec<String> {
    let index = match Index::build(layers) {
        Ok(idx) => idx,
        Err(e) => {
            warn!(error = %e, "boot-set: failed to scan layers; skipping reorder");
            return Vec::new();
        }
    };
    // Rewind for the caller's converter (which also reseeks, but be defensive).
    for layer in layers.iter_mut() {
        let _ = layer.seek(SeekFrom::Start(0));
    }

    let paths = index.derive(config);
    if paths.is_empty() {
        debug!("boot-set: derived no paths; using default layout");
    } else {
        debug!(count = paths.len(), "boot-set: derived boot paths");
    }
    paths
}

/// The boot command from an OCI image config: `(argv, env, working_dir)` where
/// `argv` is `Entrypoint ++ Cmd`. Used by the runtime profiler to actually launch
/// the image and observe what it reads. Returns an empty `argv` when the config
/// names no command.
pub fn run_command(config: &[u8]) -> (Vec<String>, Vec<String>, String) {
    let cfg = parse_config(config);
    let mut argv = cfg.entrypoint;
    argv.extend(cfg.cmd);
    (argv, cfg.env, cfg.working_dir)
}

/// A normalized, merged view of the rootfs sufficient for static ELF closure:
/// the set of regular-file paths, the symlink/hardlink graph, and buffered bytes
/// for ELF files under standard dirs. Keys are normalized (no leading `/`).
struct Index {
    regular: HashSet<String>,
    symlinks: HashMap<String, String>,
    elf: HashMap<String, Vec<u8>>,
}

impl Index {
    fn build<R: Read + Seek>(layers: &mut [R]) -> std::io::Result<Self> {
        let mut idx = Index {
            regular: HashSet::new(),
            symlinks: HashMap::new(),
            elf: HashMap::new(),
        };
        let mut elf_budget = ELF_CACHE_BUDGET;

        for layer in layers.iter_mut() {
            layer.seek(SeekFrom::Start(0))?;
            let mut archive = tar::Archive::new(&mut *layer);
            for entry in archive.entries()? {
                let mut entry = entry?;
                let header = entry.header();
                let etype = header.entry_type();
                let size = header.size().unwrap_or(0);
                let name = match entry.path() {
                    Ok(p) => norm(&p.to_string_lossy()),
                    Err(_) => continue,
                };
                if name.is_empty() {
                    continue;
                }

                // Whiteouts: `.wh..wh..opq` clears a dir; `.wh.<base>` deletes one.
                if let Some((dir, file)) = rsplit(&name) {
                    if file == ".wh..wh..opq" {
                        idx.remove_under(dir);
                        continue;
                    }
                    if let Some(base) = file.strip_prefix(".wh.") {
                        let target =
                            if dir.is_empty() { base.to_string() } else { format!("{dir}/{base}") };
                        idx.remove_path(&target);
                        continue;
                    }
                }

                if etype.is_dir() {
                    continue;
                }
                if etype.is_symlink() || etype.is_hard_link() {
                    let target = entry
                        .link_name()
                        .ok()
                        .flatten()
                        .map(|p| p.to_string_lossy().into_owned());
                    idx.remove_path(&name);
                    if let Some(t) = target {
                        // Symlink targets are relative to the LINK's directory;
                        // hardlink targets are relative to the ARCHIVE ROOT (e.g.
                        // busybox stores every applet as a hardlink to `bin/[`).
                        // Store hardlinks as absolute so `resolve_real` (which
                        // joins relative targets onto the link's dir) treats them
                        // root-relative.
                        let t = if etype.is_hard_link() {
                            format!("/{}", norm(&t))
                        } else {
                            t
                        };
                        idx.symlinks.insert(name, t);
                    }
                    continue;
                }
                if !etype.is_file() {
                    continue; // fifo/char/block — irrelevant to the boot closure
                }

                // Regular file: upper layer wins, drop any shadowed entry first.
                idx.symlinks.remove(&name);
                idx.elf.remove(&name);
                idx.regular.insert(name.clone());

                // Buffer ELF bytes only for standard dirs, within budget.
                if (4..=MAX_ELF_BYTES.min(elf_budget)).contains(&size)
                    && under_std_dir(&name)
                    && let Some(bytes) = read_if_elf(&mut entry, size)
                {
                    elf_budget = elf_budget.saturating_sub(bytes.len() as u64);
                    idx.elf.insert(name, bytes);
                }
            }
        }
        Ok(idx)
    }

    fn remove_path(&mut self, p: &str) {
        self.regular.remove(p);
        self.symlinks.remove(p);
        self.elf.remove(p);
    }

    fn remove_under(&mut self, dir: &str) {
        let prefix = format!("{dir}/");
        self.regular.retain(|p| !p.starts_with(&prefix));
        self.symlinks.retain(|p, _| !p.starts_with(&prefix));
        self.elf.retain(|p, _| !p.starts_with(&prefix));
    }

    /// Does a real file (regular or buffered ELF) exist at this exact path?
    fn is_file(&self, p: &str) -> bool {
        self.regular.contains(p) || self.elf.contains_key(p)
    }

    /// Follow the symlink/hardlink chain to the real file path, or `None`.
    fn resolve_real(&self, path: &str) -> Option<String> {
        let mut cur = norm(path);
        for _ in 0..40 {
            if self.is_file(&cur) {
                return Some(cur);
            }
            let target = self.symlinks.get(&cur)?;
            cur = join_norm(parent_dir(&cur), target);
        }
        None
    }

    /// The full static derivation over the merged index.
    fn derive(&self, config: &[u8]) -> Vec<String> {
        let cfg = parse_config(config);

        let mut result: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();

        // Seed: the executables a boot launches. Both `Entrypoint[0]` and
        // `Cmd[0]` matter — the common `ENTRYPOINT ["/docker-entrypoint.sh"]
        // CMD ["nginx", ...]` pattern runs the real workload from Cmd, so seeding
        // only the entrypoint would miss nginx and its whole lib closure. Each
        // non-ELF script seed contributes its `#!` interpreter too.
        let seeds = self.derive_seeds(&cfg);
        if seeds.is_empty() {
            warn!("boot-set: could not resolve entrypoint/cmd; deriving init-only closure");
        }
        for seed in seeds {
            push(&mut result, &mut seen, &mut queue, seed);
        }

        // Seed: the first init binary that exists.
        for cand in INIT_CANDIDATES {
            if let Some(real) = self.resolve_real(cand) {
                push(&mut result, &mut seen, &mut queue, real);
                break;
            }
        }

        // BFS the ELF closure: each binary contributes its interpreter and its
        // DT_NEEDED libraries; each library is itself parsed for further needs.
        while let Some(real) = queue.pop_front() {
            let Some(bytes) = self.elf.get(&real) else {
                continue; // not buffered (outside std dirs) or not ELF
            };
            let Some(info) = parse_elf64(bytes) else {
                continue;
            };

            if let Some(interp) = &info.interp
                && let Some(rp) = self.resolve_real(interp)
            {
                push(&mut result, &mut seen, &mut queue, rp);
            }

            let origin = parent_dir(&real).to_string();
            let mut search: Vec<String> = Vec::new();
            for rp in info.runpaths.iter().chain(info.rpaths.iter()) {
                search.push(rp.replace("$ORIGIN", &origin).replace("${ORIGIN}", &origin));
            }
            search.extend(DEFAULT_LIB_DIRS.iter().map(|s| (*s).to_string()));

            for soname in &info.needed {
                for dir in &search {
                    let cand = join_norm(norm(dir).as_str(), soname);
                    if let Some(rp) = self.resolve_real(&cand) {
                        push(&mut result, &mut seen, &mut queue, rp);
                        break;
                    }
                }
            }
        }

        // Always-early small files (linker cache, systemd targets) if present.
        for f in EARLY_FILES {
            if let Some(real) = self.resolve_real(f) {
                push_leaf(&mut result, &mut seen, real);
            }
        }

        result.into_iter().map(|p| format!("/{p}")).collect()
    }

    /// Resolve the boot's seed executables: `Entrypoint[0]` and `Cmd[0]`, plus
    /// the `#!` interpreter of any seed that turns out to be a script. Returns
    /// real (symlink-resolved) paths, deduplicated, preserving order.
    fn derive_seeds(&self, cfg: &ImageConfig) -> Vec<String> {
        let mut seeds: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for argv0 in [cfg.entrypoint.first(), cfg.cmd.first()].into_iter().flatten() {
            let Some(real) = self.resolve_exe(argv0, cfg) else { continue };
            if seen.insert(real.clone()) {
                // A non-ELF seed is a script — pull in its `#!` interpreter.
                if !self.elf.contains_key(&real)
                    && let Some(interp) = self.shebang_interp(&real)
                    && seen.insert(interp.clone())
                {
                    seeds.push(interp);
                }
                seeds.push(real);
            }
        }
        seeds
    }

    /// Resolve an `argv[0]` (absolute, relative, or bare PATH-searched) to a real
    /// file path.
    fn resolve_exe(&self, argv0: &str, cfg: &ImageConfig) -> Option<String> {
        if argv0.is_empty() {
            return None;
        }
        if argv0.contains('/') {
            let workdir = if cfg.working_dir.is_empty() { "/" } else { &cfg.working_dir };
            let abs = join_norm(norm(workdir).as_str(), argv0);
            return self.resolve_real(&abs);
        }
        let path = cfg
            .env
            .iter()
            .find_map(|e| e.strip_prefix("PATH="))
            .unwrap_or(DEFAULT_PATH);
        for dir in path.split(':').filter(|d| !d.is_empty()) {
            let cand = join_norm(norm(dir).as_str(), argv0);
            if let Some(real) = self.resolve_real(&cand) {
                return Some(real);
            }
        }
        None
    }

    /// If `path` is a buffered script, read its `#!` line and resolve the
    /// interpreter (the token after `#!`, e.g. `/bin/sh` from `#!/bin/sh -e`).
    /// Scripts are only buffered when under a standard dir; bare-name scripts in
    /// `/` (like `/docker-entrypoint.sh`) are not, so this is a best-effort bonus
    /// on top of always seeding `Cmd[0]`.
    fn shebang_interp(&self, path: &str) -> Option<String> {
        let bytes = self.elf.get(path)?; // only buffered files are inspectable
        if !bytes.starts_with(b"#!") {
            return None;
        }
        let line_end = bytes.iter().position(|&c| c == b'\n').unwrap_or(bytes.len());
        let line = std::str::from_utf8(&bytes[2..line_end]).ok()?.trim();
        let interp = line.split_whitespace().next()?;
        self.resolve_real(interp)
    }
}

/// Push a real path into the result and the BFS queue (for ELF parsing).
fn push(result: &mut Vec<String>, seen: &mut HashSet<String>, queue: &mut VecDeque<String>, real: String) {
    if seen.insert(real.clone()) {
        result.push(real.clone());
        queue.push_back(real);
    }
}

/// Push a real path into the result only (a leaf — not parsed for further deps).
fn push_leaf(result: &mut Vec<String>, seen: &mut HashSet<String>, real: String) {
    if seen.insert(real.clone()) {
        result.push(real);
    }
}

// ===========================================================================
// OCI image config (only the fields we need).
// ===========================================================================

#[derive(Default)]
struct ImageConfig {
    entrypoint: Vec<String>,
    cmd: Vec<String>,
    env: Vec<String>,
    working_dir: String,
}

fn parse_config(bytes: &[u8]) -> ImageConfig {
    // OCI image config: { "config": { "Entrypoint": [...], "Cmd": [...],
    // "Env": [...], "WorkingDir": "..." }, ... }
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        warn!("boot-set: image config is not valid JSON");
        return ImageConfig::default();
    };
    let cfg = v.get("config").unwrap_or(&serde_json::Value::Null);
    let str_array = |key: &str| -> Vec<String> {
        cfg.get(key)
            .and_then(|x| x.as_array())
            .map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_owned)).collect())
            .unwrap_or_default()
    };
    ImageConfig {
        entrypoint: str_array("Entrypoint"),
        cmd: str_array("Cmd"),
        env: str_array("Env"),
        working_dir: cfg
            .get("WorkingDir")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
    }
}

// ===========================================================================
// Minimal ELF64 little-endian parser (linux/amd64).
// ===========================================================================

struct ElfDyn {
    interp: Option<String>,
    needed: Vec<String>,
    runpaths: Vec<String>,
    rpaths: Vec<String>,
}

/// Read first 4 bytes; if they are the ELF magic, buffer the whole entry (up to
/// `size`). Returns `None` for non-ELF (leaving the rest of the entry unread —
/// the tar iterator skips it on the next step).
fn read_if_elf<R: Read>(entry: &mut R, size: u64) -> Option<Vec<u8>> {
    let mut magic = [0u8; 4];
    let mut got = 0;
    while got < 4 {
        match entry.read(&mut magic[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(_) => return None,
        }
    }
    if got < 4 || magic != [0x7f, b'E', b'L', b'F'] {
        return None;
    }
    let mut buf = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
    buf.extend_from_slice(&magic);
    if entry.take(size - 4).read_to_end(&mut buf).is_err() {
        return None;
    }
    Some(buf)
}

fn rd_u16(b: &[u8], off: usize) -> Option<u16> {
    b.get(off..off + 2).map(|s| u16::from_le_bytes(s.try_into().unwrap()))
}
fn rd_u32(b: &[u8], off: usize) -> Option<u32> {
    b.get(off..off + 4).map(|s| u32::from_le_bytes(s.try_into().unwrap()))
}
fn rd_u64(b: &[u8], off: usize) -> Option<u64> {
    b.get(off..off + 8).map(|s| u64::from_le_bytes(s.try_into().unwrap()))
}
/// Read a u64 field and narrow it to a host `usize` (fails on a corrupt/huge
/// offset rather than truncating — these are file offsets we will index with).
fn rd_usize(b: &[u8], off: usize) -> Option<usize> {
    usize::try_from(rd_u64(b, off)?).ok()
}

fn read_cstr(b: &[u8], off: usize) -> Option<String> {
    let s = b.get(off..)?;
    let end = s.iter().position(|&c| c == 0).unwrap_or(s.len());
    Some(String::from_utf8_lossy(&s[..end]).into_owned())
}

/// Parse the program headers of an ELF64 LE binary for its interpreter and
/// dynamic section, returning the `PT_INTERP` path and the `DT_NEEDED` /
/// `DT_RUNPATH` / `DT_RPATH` strings. Uses program headers (always present in a
/// loadable object) rather than section headers, so it survives `strip`.
fn parse_elf64(b: &[u8]) -> Option<ElfDyn> {
    if b.len() < 64 || b[0..4] != [0x7f, b'E', b'L', b'F'] {
        return None;
    }
    if b[4] != 2 || b[5] != 1 {
        return None; // not ELF64 / not little-endian
    }
    let phoff = rd_usize(b, 0x20)?;
    let phentsize = rd_u16(b, 0x36)? as usize;
    let phnum = rd_u16(b, 0x38)? as usize;
    if phentsize < 56 {
        return None;
    }

    // PT_LOAD segments for vaddr→file-offset mapping; the interp + dynamic offs.
    let mut loads: Vec<(u64, u64, u64)> = Vec::new(); // (vaddr, offset, filesz)
    let mut interp: Option<String> = None;
    let mut dyn_off: Option<usize> = None;
    let mut dyn_size: usize = 0;

    for i in 0..phnum {
        let base = phoff.checked_add(i.checked_mul(phentsize)?)?;
        let p_type = rd_u32(b, base)?;
        let p_offset = rd_u64(b, base + 8)?;
        let p_vaddr = rd_u64(b, base + 16)?;
        let p_filesz = rd_u64(b, base + 32)?;
        match p_type {
            1 => loads.push((p_vaddr, p_offset, p_filesz)), // PT_LOAD
            2 => {
                // PT_DYNAMIC
                dyn_off = Some(usize::try_from(p_offset).ok()?);
                dyn_size = usize::try_from(p_filesz).ok()?;
            }
            3 => {
                // PT_INTERP — a nul-terminated path at the file offset.
                interp = read_cstr(b, usize::try_from(p_offset).ok()?);
            }
            _ => {}
        }
    }

    let dyn_off = dyn_off?;
    let map_vaddr = |va: u64| -> Option<usize> {
        for &(vaddr, offset, filesz) in &loads {
            if va >= vaddr && va < vaddr + filesz {
                return usize::try_from(offset + (va - vaddr)).ok();
            }
        }
        None
    };

    // Walk the dynamic array (16-byte entries: d_tag u64, d_val u64).
    let mut strtab_va: Option<u64> = None;
    let mut needed_off: Vec<u64> = Vec::new();
    let mut runpath_off: Vec<u64> = Vec::new();
    let mut rpath_off: Vec<u64> = Vec::new();
    let count = dyn_size / 16;
    for i in 0..count {
        let e = dyn_off + i * 16;
        let tag = rd_u64(b, e)?;
        let val = rd_u64(b, e + 8)?;
        match tag {
            0 => break,            // DT_NULL
            1 => needed_off.push(val), // DT_NEEDED
            5 => strtab_va = Some(val), // DT_STRTAB
            15 => rpath_off.push(val),  // DT_RPATH
            29 => runpath_off.push(val), // DT_RUNPATH
            _ => {}
        }
    }

    let strtab_off = strtab_va.and_then(map_vaddr);
    let resolve = |off: u64| -> Option<String> {
        read_cstr(b, strtab_off?.checked_add(usize::try_from(off).ok()?)?)
    };

    let needed: Vec<String> = needed_off.iter().filter_map(|&o| resolve(o)).collect();
    let split_paths = |offs: &[u64]| -> Vec<String> {
        offs.iter()
            .filter_map(|&o| resolve(o))
            .flat_map(|s| s.split(':').map(str::to_owned).collect::<Vec<_>>())
            .filter(|s| !s.is_empty())
            .collect()
    };

    Some(ElfDyn {
        interp,
        needed,
        runpaths: split_paths(&runpath_off),
        rpaths: split_paths(&rpath_off),
    })
}

// ===========================================================================
// Path helpers (all operate on slash paths, normalized without a leading `/`).
// ===========================================================================

/// Normalize a path: strip leading `./` and `/`, drop a trailing `/`. Does not
/// resolve `.`/`..` (that's [`join_norm`]'s job).
fn norm(p: &str) -> String {
    let mut s = p;
    while let Some(rest) = s.strip_prefix("./") {
        s = rest;
    }
    s.trim_start_matches('/').trim_end_matches('/').to_string()
}

/// The directory portion of a normalized path (everything before the last `/`).
fn parent_dir(p: &str) -> &str {
    p.rsplit_once('/').map_or("", |(dir, _)| dir)
}

/// Split a normalized path into `(dir, file)` on the last `/`. `dir` is `""` for
/// a top-level name.
fn rsplit(p: &str) -> Option<(&str, &str)> {
    Some(p.rsplit_once('/').unwrap_or(("", p)))
}

/// Join `target` onto `base_dir`, resolving `.`/`..`/empty components, and
/// return a normalized path with no leading `/`. An absolute `target` ignores
/// `base_dir`.
fn join_norm(base_dir: &str, target: &str) -> String {
    let mut comps: Vec<&str> = Vec::new();
    let parts: Box<dyn Iterator<Item = &str>> = if target.starts_with('/') {
        Box::new(target.split('/'))
    } else {
        Box::new(base_dir.split('/').chain(target.split('/')))
    };
    for part in parts {
        match part {
            "" | "." => {}
            ".." => {
                comps.pop();
            }
            p => comps.push(p),
        }
    }
    comps.join("/")
}

fn under_std_dir(name: &str) -> bool {
    STD_DIRS.iter().any(|d| name.starts_with(d))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal but valid ELF64 LE object exposing a `PT_INTERP`, a
    /// `PT_LOAD`, and a `PT_DYNAMIC` with the given `DT_NEEDED` sonames. Layout:
    /// [ehdr | phdrs | strtab | dynamic], with the single PT_LOAD covering the
    /// strtab so DT_STRTAB's vaddr maps back to a file offset.
    fn make_elf(interp: &str, needed: &[&str]) -> Vec<u8> {
        let ehdr_size = 64usize;
        let phentsize = 56usize;
        let phnum = 3usize;
        let ph_off = ehdr_size;
        let str_off = ph_off + phnum * phentsize;

        // String table: index 0 is NUL; then interp, then each soname.
        let mut strtab = vec![0u8];
        let interp_idx = strtab.len() as u64;
        strtab.extend_from_slice(interp.as_bytes());
        strtab.push(0);
        let mut needed_idx = Vec::new();
        for n in needed {
            needed_idx.push(strtab.len() as u64);
            strtab.extend_from_slice(n.as_bytes());
            strtab.push(0);
        }
        let dyn_off = str_off + strtab.len();

        // Dynamic entries: DT_STRTAB, DT_STRSZ, DT_NEEDED*, DT_NULL.
        let mut dynamic: Vec<(u64, u64)> = Vec::new();
        // vaddr == file offset (identity mapping via the PT_LOAD below).
        dynamic.push((5, str_off as u64)); // DT_STRTAB
        dynamic.push((10, strtab.len() as u64)); // DT_STRSZ
        for &idx in &needed_idx {
            dynamic.push((1, idx)); // DT_NEEDED
        }
        dynamic.push((0, 0)); // DT_NULL
        let dyn_size = dynamic.len() * 16;
        let total = dyn_off + dyn_size;

        let mut b = vec![0u8; total];
        // e_ident
        b[0..4].copy_from_slice(&[0x7f, b'E', b'L', b'F']);
        b[4] = 2; // ELFCLASS64
        b[5] = 1; // ELFDATA2LSB
        b[6] = 1; // version
        // e_type=ET_DYN(3), e_machine=EM_X86_64(62)
        b[16..18].copy_from_slice(&3u16.to_le_bytes());
        b[18..20].copy_from_slice(&62u16.to_le_bytes());
        b[0x20..0x28].copy_from_slice(&(ph_off as u64).to_le_bytes()); // e_phoff
        b[0x36..0x38].copy_from_slice(&(phentsize as u16).to_le_bytes());
        b[0x38..0x3a].copy_from_slice(&(phnum as u16).to_le_bytes());

        let mut put_ph = |i: usize, ptype: u32, off: u64, vaddr: u64, filesz: u64| {
            let base = ph_off + i * phentsize;
            b[base..base + 4].copy_from_slice(&ptype.to_le_bytes());
            b[base + 8..base + 16].copy_from_slice(&off.to_le_bytes());
            b[base + 16..base + 24].copy_from_slice(&vaddr.to_le_bytes());
            b[base + 32..base + 40].copy_from_slice(&filesz.to_le_bytes());
        };
        // PT_LOAD covering the whole file (identity vaddr=offset).
        put_ph(0, 1, 0, 0, total as u64);
        // PT_INTERP
        put_ph(2, 3, (str_off as u64) + interp_idx, 0, interp.len() as u64 + 1);
        // PT_DYNAMIC
        put_ph(1, 2, dyn_off as u64, dyn_off as u64, dyn_size as u64);

        // write interp string + strtab + dynamic
        b[str_off..str_off + strtab.len()].copy_from_slice(&strtab);
        for (i, &(tag, val)) in dynamic.iter().enumerate() {
            let e = dyn_off + i * 16;
            b[e..e + 8].copy_from_slice(&tag.to_le_bytes());
            b[e + 8..e + 16].copy_from_slice(&val.to_le_bytes());
        }
        b
    }

    /// Build a single-layer tar from `(path, kind, payload_or_target)` entries.
    fn make_layer(entries: &[(&str, char, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for &(path, kind, data) in entries {
            let mut h = tar::Header::new_gnu();
            match kind {
                'f' => {
                    h.set_entry_type(tar::EntryType::Regular);
                    h.set_size(data.len() as u64);
                    h.set_mode(0o755);
                    h.set_cksum();
                    builder.append_data(&mut h, path, data).unwrap();
                }
                'l' => {
                    h.set_entry_type(tar::EntryType::Symlink);
                    h.set_size(0);
                    h.set_mode(0o777);
                    let target = std::str::from_utf8(data).unwrap();
                    builder.append_link(&mut h, path, target).unwrap();
                }
                'h' => {
                    // Hardlink: target is archive-root-relative (busybox style).
                    h.set_entry_type(tar::EntryType::Link);
                    h.set_size(0);
                    h.set_mode(0o755);
                    let target = std::str::from_utf8(data).unwrap();
                    builder.append_link(&mut h, path, target).unwrap();
                }
                _ => unreachable!(),
            }
        }
        builder.into_inner().unwrap()
    }

    fn config_json(entrypoint: &[&str], env: &[&str]) -> Vec<u8> {
        serde_json::json!({
            "config": { "Entrypoint": entrypoint, "Env": env }
        })
        .to_string()
        .into_bytes()
    }

    #[test]
    fn elf_parser_extracts_interp_and_needed() {
        let elf = make_elf("/lib64/ld-linux-x86-64.so.2", &["libc.so.6", "libm.so.6"]);
        let info = parse_elf64(&elf).expect("valid elf");
        assert_eq!(info.interp.as_deref(), Some("/lib64/ld-linux-x86-64.so.2"));
        assert_eq!(info.needed, vec!["libc.so.6".to_string(), "libm.so.6".to_string()]);
    }

    #[test]
    fn join_norm_resolves_dotdot_and_absolute() {
        assert_eq!(join_norm("usr/lib/x86_64-linux-gnu", "libc.so.6"), "usr/lib/x86_64-linux-gnu/libc.so.6");
        assert_eq!(join_norm("lib64", "../lib/x86_64-linux-gnu/libc.so.6"), "lib/x86_64-linux-gnu/libc.so.6");
        assert_eq!(join_norm("anything", "/usr/lib/libc.so.6"), "usr/lib/libc.so.6");
    }

    #[test]
    fn derives_entrypoint_interp_and_libc_closure() {
        // /usr/bin/app needs libc; libc is reached via the ld.so default dirs.
        // ld.so itself is a symlink (real path in the triplet dir). libc is a
        // symlink libc.so.6 -> libc-2.so to prove symlink resolution.
        let app = make_elf("/lib64/ld-linux-x86-64.so.2", &["libc.so.6"]);
        let libc = make_elf("/lib64/ld-linux-x86-64.so.2", &[]);
        let ld = make_elf("", &[]);
        let layer = make_layer(&[
            ("usr/bin/app", 'f', &app),
            ("usr/lib/x86_64-linux-gnu/libc-2.so", 'f', &libc),
            ("usr/lib/x86_64-linux-gnu/libc.so.6", 'l', b"libc-2.so"),
            ("usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2", 'f', &ld),
            ("lib64/ld-linux-x86-64.so.2", 'l', b"../usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2"),
            ("etc/ld.so.cache", 'f', b"cachedata"),
        ]);
        let mut layers = [Cursor::new(layer)];
        let cfg = config_json(&["/usr/bin/app"], &[]);
        let paths = derive_boot_paths(&cfg, &mut layers);

        assert!(paths.contains(&"/usr/bin/app".to_string()), "entrypoint: {paths:?}");
        assert!(
            paths.contains(&"/usr/lib/x86_64-linux-gnu/libc-2.so".to_string()),
            "libc real path via symlink: {paths:?}"
        );
        assert!(
            paths.contains(&"/usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2".to_string()),
            "interp real path via symlink: {paths:?}"
        );
        assert!(paths.contains(&"/etc/ld.so.cache".to_string()), "linker cache: {paths:?}");
        // Layers left rewound for the converter.
        assert_eq!(layers[0].position(), 0);
    }

    #[test]
    fn entrypoint_via_path_search() {
        let app = make_elf("/lib64/ld.so.2", &[]);
        let layer = make_layer(&[("usr/local/bin/python3", 'f', &app)]);
        let mut layers = [Cursor::new(layer)];
        // Bare name resolved through PATH.
        let cfg = config_json(&["python3"], &["PATH=/usr/local/bin:/usr/bin"]);
        let paths = derive_boot_paths(&cfg, &mut layers);
        assert!(paths.contains(&"/usr/local/bin/python3".to_string()), "{paths:?}");
    }

    #[test]
    fn hardlinked_entrypoint_resolves_to_canonical_file() {
        // busybox layout: the ELF lives at bin/[ ; every applet (sh, busybox)
        // is a HARDLINK to it (target relative to archive root, not link dir).
        let bb = make_elf("/lib/ld-musl-x86_64.so.1", &[]);
        let layer = make_layer(&[
            ("bin/[", 'f', &bb),
            ("bin/sh", 'h', b"bin/["),
            ("bin/busybox", 'h', b"bin/["),
        ]);
        let mut layers = [Cursor::new(layer)];
        // Cmd ["sh"] resolved via PATH, sh is a hardlink → must reach bin/[.
        let cfg = config_json(&["sh"], &["PATH=/bin"]);
        let paths = derive_boot_paths(&cfg, &mut layers);
        assert!(paths.contains(&"/bin/[".to_string()), "hardlink → canonical ELF: {paths:?}");
    }

    #[test]
    fn whiteout_deletes_lower_layer_file() {
        let app = make_elf("/lib64/ld.so.2", &[]);
        let lower = make_layer(&[("usr/bin/app", 'f', &app)]);
        let upper = make_layer(&[("usr/bin/.wh.app", 'f', b"")]);
        let mut layers = [Cursor::new(lower), Cursor::new(upper)];
        let cfg = config_json(&["/usr/bin/app"], &[]);
        let paths = derive_boot_paths(&cfg, &mut layers);
        assert!(!paths.contains(&"/usr/bin/app".to_string()), "whiteout should delete: {paths:?}");
    }

    #[test]
    fn empty_config_yields_no_paths() {
        let layer = make_layer(&[("usr/bin/app", 'f', b"not-an-elf")]);
        let mut layers = [Cursor::new(layer)];
        let paths = derive_boot_paths(b"{}", &mut layers);
        assert!(paths.is_empty(), "{paths:?}");
    }
}
