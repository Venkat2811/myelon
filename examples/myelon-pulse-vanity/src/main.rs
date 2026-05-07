//! `myelon-pulse` — brand vanity demo.
//!
//! A House-MD-titles-style angiogram. The trunk emerges from a seed
//! at the left edge of the canvas and fractal-branches outward to the
//! right in fine, frayed, recursive tendrils. Once fully grown,
//! pulses travel along the branches in two demonstrable shapes:
//!
//! - **Broadcast**: every leaf branch lights up in lockstep on each
//!   pulse — `disruptor-mp`'s strict-broadcast multiprocess ring.
//! - **Ping-pong**: every branch fires an outbound pulse
//!   simultaneously; each pulse hits its own leaf and a return pulse
//!   fires back to the seed. All branches do the round-trip in
//!   parallel; the rhythm is `out — back — pause — repeat`.
//!
//! UI: `START` grows a fresh tree from empty; `STOP` clears it back
//! to empty. `--mode` picks the pulse motion once the tree is alive;
//! the default `alternating` toggles between broadcast and ping-pong.

use std::f32::consts::TAU;
use std::fs::{create_dir_all, File};
use std::io::{self, BufWriter};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use macroquad::prelude::*;

// ---------------------------------------------------------------------------
// Palette — sampled from the reference frames.
// ---------------------------------------------------------------------------

const BG_DEEP: Color = Color::new(0.013, 0.010, 0.013, 1.0);
const BG_WARM: Color = Color::new(0.08, 0.04, 0.024, 1.0);
const VIGNETTE: Color = Color::new(0.0, 0.0, 0.0, 0.55);
const EMBER_DIM: Color = Color::new(0.72, 0.45, 0.20, 0.18);
const EMBER_BRIGHT: Color = Color::new(0.95, 0.65, 0.30, 0.55);
const BRANCH_HALO: Color = Color::new(0.60, 0.22, 0.08, 1.0);
const BRANCH_GLOW: Color = Color::new(1.00, 0.50, 0.18, 1.0);
const BRANCH_CORE: Color = Color::new(1.00, 0.78, 0.45, 1.0);
const BRANCH_TIP: Color = Color::new(1.00, 0.95, 0.80, 1.0);
const PULSE_HOT: Color = Color::new(1.00, 0.85, 0.55, 1.0);
const PULSE_CORE: Color = Color::new(1.00, 0.97, 0.88, 1.0);
const BUTTON_DIM: Color = Color::new(0.70, 0.40, 0.20, 1.0);
const BUTTON_HOT: Color = Color::new(1.00, 0.65, 0.30, 1.0);
const BUTTON_DISABLED: Color = Color::new(0.35, 0.27, 0.22, 1.0);

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug, Clone)]
#[command(
    name = "myelon-pulse",
    about = "Brand demo for the myelon transport stack"
)]
struct Args {
    /// Pulse motion shape once the tree is alive.
    #[arg(long, value_enum, default_value_t = Mode::Alternating)]
    mode: Mode,

    /// Override the leaf branch count target. Default: 2/3 of
    /// physical cores rounded down to even, clamped [2, 16].
    #[arg(long)]
    branches: Option<usize>,

    /// Skip the title screen and start growth immediately.
    #[arg(long)]
    autostart: bool,

    /// Enable capture mode. Adds a `REC` (GIF recording,
    /// click-to-toggle) button to the UI. Files are written under
    /// `./myelon-pulse-captures/` relative to the process's current
    /// working directory (workspace root when launched via
    /// `cargo run`); the directory is gitignored.
    #[arg(long)]
    record: bool,

    /// Enable the X-axis pitch oscillation that gives the tree a
    /// volumetric "breathing" feel. Off by default — when off, the
    /// scene renders flat (theta = 0) and `project_x_rot` reduces to
    /// the identity transform, so segments and pulses draw in pure 2D.
    #[arg(long)]
    rotate: bool,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Broadcast,
    Pingpong,
    Alternating,
}

fn default_leaf_target() -> usize {
    let cores = num_cpus::get_physical().max(2);
    let two_thirds = (cores * 2) / 3;
    let even_floor = two_thirds & !1;
    even_floor.clamp(2, 16)
}

// ---------------------------------------------------------------------------
// PRNG — deterministic per-tree, no external dep.
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        let s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        Self(if s == 0 { 0xDEAD_BEEF } else { s })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn unit(&mut self) -> f32 {
        (self.next_u64() >> 11) as f32 / (1u64 << 53) as f32
    }

    fn range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + self.unit() * (hi - lo)
    }

    fn sign(&mut self) -> f32 {
        if self.unit() < 0.5 {
            -1.0
        } else {
            1.0
        }
    }
}

fn seed_from_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0xC0FFEE)
}

// ---------------------------------------------------------------------------
// Tree — recursive fractal branching, left → right.
// ---------------------------------------------------------------------------

const SAMPLES_PER_SEGMENT: usize = 80;

struct Segment {
    samples: Vec<Vec2>,
    arc_length: f32,
    parent: Option<usize>,
    depth: u8,
    /// Wall-clock seconds (relative to growth start) when this
    /// segment begins to draw.
    born_at: f32,
    /// Seconds for this segment to fully reveal.
    grow_duration: f32,
    /// Final Z value at this segment's tip (sample N-1).
    /// Trunk = 0. Primary branches fan out to `±~12 % canvas_h`;
    /// sub-branches drift slightly from their parent's `z_offset`
    /// so the tree forms one coherent volume instead of noise.
    z_offset: f32,
    /// Z value at every sample point, length = `SAMPLES_PER_SEGMENT`.
    /// Linearly ramps from the parent's Z **at this segment's
    /// `attach_t`** (sample 0) to this segment's own `z_offset`
    /// (sample N-1). Using the parent's Z *at the attachment
    /// point* — rather than at the parent's tip — is what makes
    /// junctions seamless, because a child branches off
    /// mid-parent, not from the parent's tip.
    z_samples: Vec<f32>,
    /// Fractional position along the parent at which this segment
    /// is attached. `0.0` for the trunk (no parent). Used by the
    /// per-sample-Z pass so a child's start Z matches the parent's
    /// Z at the actual junction point.
    attach_t: f32,
}

impl Segment {
    fn at(&self, t: f32) -> Vec2 {
        let t = t.clamp(0.0, 1.0);
        let f = t * (SAMPLES_PER_SEGMENT - 1) as f32;
        let i = f.floor() as usize;
        if i >= SAMPLES_PER_SEGMENT - 1 {
            return *self.samples.last().unwrap();
        }
        let frac = f - i as f32;
        self.samples[i].lerp(self.samples[i + 1], frac)
    }

    /// Z-depth at fractional position `t` along the segment. Used
    /// by pulse drawing so a pulse moving through a junction sees
    /// a continuous Z (parent's tip Z = child's start Z).
    fn at_z(&self, t: f32) -> f32 {
        if self.z_samples.is_empty() {
            return 0.0;
        }
        let t = t.clamp(0.0, 1.0);
        let f = t * (SAMPLES_PER_SEGMENT - 1) as f32;
        let i = f.floor() as usize;
        if i >= SAMPLES_PER_SEGMENT - 1 {
            return *self.z_samples.last().unwrap();
        }
        let frac = f - i as f32;
        self.z_samples[i] * (1.0 - frac) + self.z_samples[i + 1] * frac
    }

    fn tangent_at(&self, t: f32) -> Vec2 {
        let p1 = self.at((t - 0.01).max(0.0));
        let p2 = self.at((t + 0.01).min(1.0));
        let d = p2 - p1;
        if d.length_squared() < 1e-6 {
            Vec2::X
        } else {
            d.normalize()
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_segment(
    p_start: Vec2,
    p_end: Vec2,
    wave_amp: f32,
    wave_phase: f32,
    parent: Option<usize>,
    depth: u8,
    born_at: f32,
    grow_duration: f32,
    attach_t: f32,
) -> Segment {
    let dir = p_end - p_start;
    let dir = if dir.length_squared() < 1e-6 {
        Vec2::X
    } else {
        dir.normalize()
    };
    let perp = Vec2::new(-dir.y, dir.x);

    let mut samples = Vec::with_capacity(SAMPLES_PER_SEGMENT);
    let mut last = Vec2::ZERO;
    let mut arc_length = 0.0;

    for i in 0..SAMPLES_PER_SEGMENT {
        let t = i as f32 / (SAMPLES_PER_SEGMENT - 1) as f32;
        let base = p_start.lerp(p_end, t);
        let env = (4.0 * t * (1.0 - t)).max(0.0);
        let wobble = (t * 6.2 + wave_phase).sin() * 0.55
            + (t * 13.7 + wave_phase * 0.7).sin() * 0.30
            + (t * 27.1 + wave_phase * 1.3).sin() * 0.15;
        let pos = base + perp * (wobble * wave_amp * env);
        if i > 0 {
            arc_length += pos.distance(last);
        }
        last = pos;
        samples.push(pos);
    }

    Segment {
        samples,
        arc_length,
        parent,
        depth,
        born_at,
        grow_duration,
        // `z_offset` and `z_samples` are filled in by `build_tree`
        // after all segments exist, so each child can read its
        // parent's Z **at the attachment point** (not the tip)
        // for a continuous junction.
        z_offset: 0.0,
        z_samples: Vec::new(),
        attach_t,
    }
}

struct Tree {
    segments: Vec<Segment>,
    /// `is_leaf[i]` is true if segment `i` has no children. Drawing
    /// uses this to render the permanent end-bead on actual leaves
    /// only, not on every junction.
    is_leaf: Vec<bool>,
    /// One entry per leaf segment: chain of segment indices from
    /// trunk (index 0) to that leaf.
    leaf_paths: Vec<Vec<usize>>,
    /// Pre-summed total arc length of each leaf path.
    leaf_path_lengths: Vec<f32>,
    full_grow_duration: f32,
}

fn build_tree(canvas_w: f32, canvas_h: f32, leaf_target: usize, seed: u64) -> Tree {
    let mut rng = Rng::new(seed);
    let mut segments: Vec<Segment> = Vec::with_capacity(64);

    // Trunk: from the seed at left → roughly horizontal toward center.
    let seed_pt = Vec2::new(canvas_w * 0.10, canvas_h * 0.50);
    let trunk_end = Vec2::new(
        canvas_w * rng.range(0.42, 0.50),
        canvas_h * 0.50 + rng.range(-0.04, 0.04) * canvas_h,
    );
    let trunk = build_segment(
        seed_pt,
        trunk_end,
        canvas_h * 0.04,
        rng.range(0.0, TAU),
        None,
        0,
        0.0,
        // Trunk grow time. The narrative caption walks four init
        // steps during growth (INITIALIZE → DISCOVER → ATTACH →
        // READY); each step needs ~1.5s of dwell to feel
        // deliberate and not like a flash card. 2.8s on the trunk
        // lands the full reveal around 5s and pairs with the
        // caption crossfade below.
        2.8,
        // attach_t — trunk has no parent, so this is unused; the
        // z_samples pass guards on `parent.is_none()`.
        0.0,
    );
    segments.push(trunk);

    let max_depth: u8 = match leaf_target {
        0..=4 => 3,
        _ => 4,
    };

    spawn_children(&mut segments, &mut rng, 0, 1, max_depth, canvas_w, canvas_h);

    // Identify leaves (no children) and build paths.
    let n = segments.len();
    let mut has_children = vec![false; n];
    for s in &segments {
        if let Some(p) = s.parent {
            has_children[p] = true;
        }
    }

    let is_leaf: Vec<bool> = (0..n).map(|i| !has_children[i]).collect();

    // Assign a Z-depth to every segment so the tree occupies a 3D
    // volume rather than a flat plane. We walk segments in index
    // order (parents come before children — they were pushed that
    // way), so each child can read its parent's `z_offset` and
    // place itself nearby. Trunk stays at z = 0; primary branches
    // get the largest spread; sub-branches drift only slightly from
    // their parent so the volume reads as one coherent shape rather
    // than scattered noise. Values are deterministic from segment
    // index, so the same seed + canvas size always gives the same
    // 3D layout.
    let primary_z_amp = canvas_h * 0.12;
    let sub_drift_amp = canvas_h * 0.04;
    for i in 1..n {
        let parent_idx = segments[i].parent.expect("non-trunk segment has parent");
        let parent_z = segments[parent_idx].z_offset;
        let h =
            ((i as f32) * 7.7351 + 1.3).sin() * 0.65 + ((i as f32) * 13.4189 + 4.1).sin() * 0.35;
        let h = h.clamp(-1.0, 1.0);
        let z = match segments[i].depth {
            1 => h * primary_z_amp,
            _ => parent_z + h * sub_drift_amp,
        };
        segments[i].z_offset = z;
    }

    // Per-sample Z. Each segment's Z linearly ramps from the
    // parent's Z **at this segment's `attach_t`** (sample 0) to
    // its own `z_offset` (sample N-1). Children branch off
    // mid-parent, not from the parent's tip, so the parent-side
    // Z to anchor on is the interpolated value at the actual
    // attachment fraction — that's what makes a junction
    // truly seamless under perspective. The loop visits parents
    // before children (segments are pushed in BFS-ish order in
    // `spawn_children`), so by the time we read
    // `parent.at_z(t)` the parent's `z_samples` is already
    // populated.
    for i in 0..n {
        let parent_attach_z = match segments[i].parent {
            Some(p) => segments[p].at_z(segments[i].attach_t),
            None => 0.0,
        };
        let own_z = segments[i].z_offset;
        let mut z_samples = Vec::with_capacity(SAMPLES_PER_SEGMENT);
        for k in 0..SAMPLES_PER_SEGMENT {
            let t = k as f32 / (SAMPLES_PER_SEGMENT - 1) as f32;
            z_samples.push(parent_attach_z * (1.0 - t) + own_z * t);
        }
        segments[i].z_samples = z_samples;
    }

    let mut leaf_paths: Vec<Vec<usize>> = Vec::new();
    let mut leaf_path_lengths: Vec<f32> = Vec::new();
    for (i, &leaf) in is_leaf.iter().enumerate() {
        if !leaf {
            continue;
        }
        let mut path = vec![i];
        let mut cur = i;
        while let Some(p) = segments[cur].parent {
            path.push(p);
            cur = p;
        }
        path.reverse();
        let len: f32 = path.iter().map(|&j| segments[j].arc_length).sum();
        leaf_paths.push(path);
        leaf_path_lengths.push(len);
    }

    let full_grow_duration = segments
        .iter()
        .map(|s| s.born_at + s.grow_duration)
        .fold(0.0_f32, f32::max);

    Tree {
        segments,
        is_leaf,
        leaf_paths,
        leaf_path_lengths,
        full_grow_duration,
    }
}

fn spawn_children(
    segs: &mut Vec<Segment>,
    rng: &mut Rng,
    parent_idx: usize,
    depth: u8,
    max_depth: u8,
    canvas_w: f32,
    canvas_h: f32,
) {
    if depth > max_depth {
        return;
    }

    let n_children = match depth {
        1 => 3 + (rng.next_u64() % 3) as usize, // 3..5 primary
        2 => 1 + (rng.next_u64() % 3) as usize, // 1..3
        3 => {
            if rng.unit() < 0.6 {
                1 + (rng.next_u64() % 2) as usize
            } else {
                0
            }
        }
        _ => {
            if rng.unit() < 0.3 {
                1
            } else {
                0
            }
        }
    };

    for k in 0..n_children {
        // Snapshot parent fields we need (we'll borrow segs mutably below).
        let (parent_born, parent_grow, parent_len, attach_pt, parent_dir, attach_t) = {
            let parent = &segs[parent_idx];
            let parent_len = parent.samples.last().unwrap().distance(parent.samples[0]);
            let attach_t = match depth {
                1 => 0.30 + (k as f32 + rng.unit() * 0.7) / n_children.max(1) as f32 * 0.65,
                2 => 0.35 + rng.unit() * 0.55,
                _ => 0.30 + rng.unit() * 0.65,
            };
            (
                parent.born_at,
                parent.grow_duration,
                parent_len,
                parent.at(attach_t),
                parent.tangent_at(attach_t),
                attach_t,
            )
        };

        // Branch off direction: rotate parent tangent by ±angle.
        // Primary level alternates sides; deeper levels random.
        let side = if depth == 1 {
            if k & 1 == 0 {
                -1.0
            } else {
                1.0
            }
        } else {
            rng.sign()
        };
        let angle = side * rng.range(0.45, 1.05); // ~25–60 deg
        let cos_a = angle.cos();
        let sin_a = angle.sin();
        let child_dir = Vec2::new(
            parent_dir.x * cos_a - parent_dir.y * sin_a,
            parent_dir.x * sin_a + parent_dir.y * cos_a,
        );

        let length_factor = match depth {
            1 => rng.range(0.55, 0.85),
            2 => rng.range(0.40, 0.70),
            3 => rng.range(0.28, 0.55),
            _ => rng.range(0.18, 0.40),
        };
        let child_len = (parent_len * length_factor).max(canvas_w * 0.02);
        let child_end = attach_pt + child_dir * child_len;

        // Wave amplitude & growth duration shrink with depth.
        let wave_amp = canvas_h * (0.045 / (1.0 + depth as f32 * 0.6));
        let attach_t_for_birth = match depth {
            1 => 0.4,
            _ => 0.5,
        };
        let child_born = parent_born + parent_grow * attach_t_for_birth;
        let child_grow = (parent_grow * rng.range(0.70, 0.92)).max(0.55);

        let seg = build_segment(
            attach_pt,
            child_end,
            wave_amp,
            rng.range(0.0, TAU),
            Some(parent_idx),
            depth,
            child_born,
            child_grow,
            attach_t,
        );
        let child_idx = segs.len();
        segs.push(seg);

        spawn_children(
            segs,
            rng,
            child_idx,
            depth + 1,
            max_depth,
            canvas_w,
            canvas_h,
        );
    }
}

// ---------------------------------------------------------------------------
// Pulse system — pulses travel along leaf paths in absolute pixel
// distance so shared trunk segments naturally see overlapping pulses.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PulseDir {
    Outbound,
    Inbound,
}

struct Pulse {
    /// Index into `Tree.leaf_paths`.
    path_idx: usize,
    /// Distance traveled along the path from its start (trunk root)
    /// in pixels.
    distance: f32,
    dir: PulseDir,
    speed: f32,
    life: f32,
    hot: Color,
}

/// Per-path ping-pong state. Each leaf path owns its own
/// publish/receive cycle so out and return pulses across different
/// branches overlap continuously instead of running as synchronized
/// rounds.
#[derive(Clone, Copy, Debug)]
enum PathPhase {
    /// Resting between cycles; the `f32` is seconds remaining
    /// before the next outbound spawn.
    Idle(f32),
    /// An outbound pulse is currently flying on this path.
    Outbound,
    /// An inbound (return) pulse is currently flying on this path.
    Inbound,
}

struct PulseSystem {
    pulses: Vec<Pulse>,
    /// Used by broadcast mode only.
    broadcast_clock: f32,
    effective_mode: Mode,
    alternation_clock: f32,
    /// One entry per leaf path. Reinitialized when path count changes.
    path_states: Vec<PathPhase>,
}

const BROADCAST_INTERVAL_S: f32 = 0.55;
const PINGPONG_REST_MIN_S: f32 = 0.18;
const PINGPONG_REST_MAX_S: f32 = 0.45;
const ALTERNATION_PERIOD_S: f32 = 7.0;

fn pulse_speed_for(canvas_w: f32) -> f32 {
    canvas_w * 0.55
}

/// Build initial per-path states with staggered initial cooldowns
/// so paths begin their cycles at slightly different phases. The
/// stagger gives the visual a continuous out+return flow rather
/// than an obvious all-at-once round.
fn init_path_states(n: usize) -> Vec<PathPhase> {
    (0..n)
        .map(|i| {
            // Spread initial cooldowns across [0.0, 0.6) deterministically.
            let stagger = (i as f32 * 0.137).rem_euclid(0.6);
            PathPhase::Idle(stagger)
        })
        .collect()
}

/// Rest cooldown after a return completes, with deterministic jitter
/// per path so paths drift apart over time.
fn rest_cooldown(path_idx: usize) -> f32 {
    let s = (path_idx as f32 * 0.7321 + 1.3).sin() * 0.5 + 0.5;
    PINGPONG_REST_MIN_S + s * (PINGPONG_REST_MAX_S - PINGPONG_REST_MIN_S)
}

impl PulseSystem {
    fn new(initial_mode: Mode) -> Self {
        Self {
            pulses: Vec::with_capacity(64),
            broadcast_clock: 0.0,
            effective_mode: match initial_mode {
                Mode::Alternating => Mode::Broadcast,
                m => m,
            },
            alternation_clock: 0.0,
            path_states: Vec::new(),
        }
    }

    fn tick(&mut self, dt: f32, requested_mode: Mode, tree: &Tree, canvas_w: f32) {
        // Mode resolution + alternation.
        let prev_effective = self.effective_mode;
        if requested_mode == Mode::Alternating {
            self.alternation_clock += dt;
            if self.alternation_clock >= ALTERNATION_PERIOD_S {
                self.alternation_clock = 0.0;
                self.effective_mode = match self.effective_mode {
                    Mode::Broadcast => Mode::Pingpong,
                    _ => Mode::Broadcast,
                };
            }
        } else {
            self.effective_mode = requested_mode;
        }

        // Re-initialize per-path states whenever path count changes
        // OR we just transitioned into ping-pong mode (gives the new
        // mode a fresh staggered start).
        let entered_pingpong = matches!(self.effective_mode, Mode::Pingpong)
            && !matches!(prev_effective, Mode::Pingpong);
        if self.path_states.len() != tree.leaf_paths.len() || entered_pingpong {
            self.path_states = init_path_states(tree.leaf_paths.len());
        }

        // Advance live pulses by absolute distance.
        for p in &mut self.pulses {
            let dv = match p.dir {
                PulseDir::Outbound => p.speed * dt,
                PulseDir::Inbound => -p.speed * dt,
            };
            p.distance += dv;
            let total = tree.leaf_path_lengths[p.path_idx];
            if p.dir == PulseDir::Outbound && p.distance >= total {
                p.life -= dt * 2.5;
            }
            if p.dir == PulseDir::Inbound && p.distance <= 0.0 {
                p.life -= dt * 2.5;
            }
        }
        self.pulses.retain(|p| p.life > 0.0);

        // Spawn.
        match self.effective_mode {
            Mode::Broadcast => {
                self.broadcast_clock += dt;
                self.tick_broadcast(tree, canvas_w);
            }
            Mode::Pingpong => {
                self.broadcast_clock = 0.0;
                self.tick_pingpong(dt, tree, canvas_w);
            }
            Mode::Alternating => unreachable!(),
        }
    }

    fn tick_broadcast(&mut self, tree: &Tree, canvas_w: f32) {
        if self.broadcast_clock < BROADCAST_INTERVAL_S {
            return;
        }
        self.broadcast_clock = 0.0;
        let speed = pulse_speed_for(canvas_w);
        for path_idx in 0..tree.leaf_paths.len() {
            self.pulses.push(Pulse {
                path_idx,
                distance: 0.0,
                dir: PulseDir::Outbound,
                speed,
                life: 1.0,
                hot: PULSE_HOT,
            });
        }
    }

    fn tick_pingpong(&mut self, dt: f32, tree: &Tree, canvas_w: f32) {
        let speed = pulse_speed_for(canvas_w);
        let n = tree.leaf_paths.len();
        if n == 0 {
            return;
        }

        // Snapshot pulse-arrival flags so per-path state updates can
        // borrow `self.path_states` mutably without conflict.
        let mut outbound_arrived = vec![false; n];
        let mut inbound_returned = vec![false; n];
        for p in &self.pulses {
            if p.life <= 0.0 {
                continue;
            }
            match p.dir {
                PulseDir::Outbound => {
                    if p.distance >= tree.leaf_path_lengths[p.path_idx] - 4.0 {
                        outbound_arrived[p.path_idx] = true;
                    }
                }
                PulseDir::Inbound => {
                    if p.distance <= 4.0 {
                        inbound_returned[p.path_idx] = true;
                    }
                }
            }
        }

        // Per-path independent state machine. Each path runs:
        //   Idle(cd)  → countdown, then spawn outbound → Outbound
        //   Outbound  → wait for arrival, spawn return → Inbound
        //   Inbound   → wait for return, set rest cooldown → Idle
        //
        // Result: at any moment some branches have outbound pulses
        // mid-flight while others have inbound returns mid-flight,
        // and the publish/receive flow looks continuous.
        let mut spawn_outbound: Vec<usize> = Vec::new();
        let mut spawn_inbound: Vec<usize> = Vec::new();

        for path_idx in 0..n {
            let st = &mut self.path_states[path_idx];
            match *st {
                PathPhase::Idle(cd) => {
                    let new_cd = cd - dt;
                    if new_cd <= 0.0 {
                        spawn_outbound.push(path_idx);
                        *st = PathPhase::Outbound;
                    } else {
                        *st = PathPhase::Idle(new_cd);
                    }
                }
                PathPhase::Outbound => {
                    if outbound_arrived[path_idx] {
                        spawn_inbound.push(path_idx);
                        *st = PathPhase::Inbound;
                    }
                }
                PathPhase::Inbound => {
                    if inbound_returned[path_idx] {
                        *st = PathPhase::Idle(rest_cooldown(path_idx));
                    }
                }
            }
        }

        for path_idx in spawn_outbound {
            self.pulses.push(Pulse {
                path_idx,
                distance: 0.0,
                dir: PulseDir::Outbound,
                speed,
                life: 1.0,
                hot: PULSE_HOT,
            });
        }
        for path_idx in spawn_inbound {
            let total = tree.leaf_path_lengths[path_idx];
            self.pulses.push(Pulse {
                path_idx,
                distance: total,
                dir: PulseDir::Inbound,
                speed,
                life: 1.0,
                hot: PULSE_CORE,
            });
        }
    }
}

/// Resolve a `(segment_index, t-in-segment)` for a given distance
/// along a leaf path.
fn position_along_path(tree: &Tree, path_idx: usize, distance: f32) -> Option<(usize, f32)> {
    let path = tree.leaf_paths.get(path_idx)?;
    let mut acc = 0.0;
    for &seg_idx in path {
        let len = tree.segments[seg_idx].arc_length;
        if len <= 0.0 {
            continue;
        }
        if acc + len >= distance {
            let t = ((distance - acc) / len).clamp(0.0, 1.0);
            return Some((seg_idx, t));
        }
        acc += len;
    }
    path.last().map(|&i| (i, 1.0))
}

// ---------------------------------------------------------------------------
// Embers
// ---------------------------------------------------------------------------

struct Ember {
    pos: Vec2,
    vel: Vec2,
    radius: f32,
    flicker_phase: f32,
    base_alpha: f32,
}

const EMBER_COUNT: usize = 140;

fn build_embers(w: f32, h: f32) -> Vec<Ember> {
    let mut embers = Vec::with_capacity(EMBER_COUNT);
    for i in 0..EMBER_COUNT {
        let s = (i as f32 * 0.618_034).fract();
        let s2 = (i as f32 * 0.314_159).fract();
        embers.push(Ember {
            pos: Vec2::new(s * w, s2 * h),
            vel: Vec2::new(
                4.0 + (s * TAU + 1.7).sin() * 6.0, // mostly leftward / rightward drift
                (s2 * TAU + 0.3).cos() * 4.0,
            ),
            radius: 0.6 + (s2 * 4.0).fract() * 1.5,
            flicker_phase: s * TAU,
            base_alpha: 0.16 + (s + s2).fract() * 0.45,
        });
    }
    embers
}

fn tick_embers(embers: &mut [Ember], dt: f32, w: f32, h: f32) {
    for e in embers {
        e.pos += e.vel * dt;
        e.flicker_phase += dt * 1.4;
        if e.pos.x > w + 6.0 {
            e.pos.x = -6.0;
            e.pos.y = (e.pos.y + 23.0) % h;
        } else if e.pos.x < -6.0 {
            e.pos.x = w + 6.0;
        }
        if e.pos.y < -6.0 {
            e.pos.y = h + 6.0;
        } else if e.pos.y > h + 6.0 {
            e.pos.y = -6.0;
        }
    }
}

// ---------------------------------------------------------------------------
// Capture — GIF recording, with optional ffmpeg → MP4 conversion.
//
// Files write to `<cwd>/myelon-pulse-captures/` (created on first
// save), gitignored at the workspace root. Filenames embed a
// unix-seconds + millis timestamp so rapid clicks don't collide.
//
// GIFs are streamed frame-by-frame through the `gif` crate at a
// configurable FPS (we capture every Nth render frame). The encoder
// finalizes its trailer when the recorder is dropped, so toggling
// REC off just drops the `Option<GifRecorder>` and the file closes
// cleanly. After the GIF closes we attempt to spawn `ffmpeg` to
// convert it to a Twitter-friendly H.264 MP4 next to the GIF; if
// ffmpeg isn't on PATH we just keep the GIF and warn.
//
// GIF capture happens after the brand visual is drawn but before
// any UI chrome (buttons, toast, debug HUD), so those don't leak
// into the recording.
// ---------------------------------------------------------------------------

/// Returns `<cwd>/myelon-pulse-captures/`, creating it on first call.
/// `cargo run -p myelon-pulse-vanity` runs the binary with the workspace
/// root as cwd, so captures land alongside the source tree (and are
/// gitignored at that path) instead of polluting `$HOME`.
fn capture_dir() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let dir = cwd.join("myelon-pulse-captures");
    let _ = create_dir_all(&dir);
    dir
}

/// Flip an RGBA framebuffer in-place top-to-bottom. macroquad's
/// `get_screen_data()` returns the back buffer in OpenGL convention
/// (origin at bottom-left); PNG export already handles the flip
/// internally, but the raw bytes we hand to the gif encoder do not,
/// so we mirror rows here before encoding.
fn flip_rgba_rows(bytes: &mut [u8], width: u16, height: u16) {
    let row_bytes = width as usize * 4;
    let h = height as usize;
    if row_bytes == 0 || h == 0 {
        return;
    }
    for y in 0..h / 2 {
        let top = y * row_bytes;
        let bot = (h - 1 - y) * row_bytes;
        for i in 0..row_bytes {
            bytes.swap(top + i, bot + i);
        }
    }
}

fn timestamp_string() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}-{:03}", now.as_secs(), now.subsec_millis())
}

struct GifRecorder {
    encoder: gif::Encoder<BufWriter<File>>,
    width: u16,
    height: u16,
    /// Seconds since the last captured frame; when this exceeds
    /// `interval` we capture and reset.
    frame_clock: f32,
    interval: f32,
    /// 1/100ths of a second per frame (gif spec). Pre-computed.
    delay_centiseconds: u16,
    path: PathBuf,
    frames_written: u32,
}

impl GifRecorder {
    /// Begin a new recording. Captures dimensions from the current
    /// framebuffer, so the recording is locked to that resolution
    /// — if the window resizes mid-recording, those frames are
    /// dropped (better than corrupting the GIF stream).
    fn start(fps: f32) -> io::Result<Self> {
        let img = get_screen_data();
        let width = img.width;
        let height = img.height;
        let dir = capture_dir();
        let path = dir.join(format!("recording-{}.gif", timestamp_string()));
        let file = File::create(&path)?;
        let buf = BufWriter::new(file);
        let mut encoder = gif::Encoder::new(buf, width, height, &[])
            .map_err(|e| io::Error::other(e.to_string()))?;
        encoder
            .set_repeat(gif::Repeat::Infinite)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let interval = 1.0 / fps.max(1.0);
        let delay_centiseconds = (100.0 / fps.max(1.0)).round() as u16;
        Ok(Self {
            encoder,
            width,
            height,
            frame_clock: 0.0,
            interval,
            delay_centiseconds,
            path,
            frames_written: 0,
        })
    }

    /// Advance the per-frame clock and capture if due. The framebuffer
    /// dimensions must match the recorder's locked dimensions — frames
    /// at a different size are silently skipped.
    fn try_capture(&mut self, dt: f32) -> io::Result<()> {
        self.frame_clock += dt;
        if self.frame_clock < self.interval {
            return Ok(());
        }
        self.frame_clock -= self.interval;

        let img = get_screen_data();
        if img.width != self.width || img.height != self.height {
            return Ok(());
        }
        let mut bytes = img.bytes;
        // Match the orientation PNG export produces (top-left origin)
        // before encoding — gif crate doesn't flip for us.
        flip_rgba_rows(&mut bytes, self.width, self.height);
        // speed: 1=best quality, 30=fastest. 12 is a balanced default.
        let mut frame = gif::Frame::from_rgba_speed(self.width, self.height, &mut bytes, 12);
        frame.delay = self.delay_centiseconds;
        self.encoder
            .write_frame(&frame)
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.frames_written += 1;
        Ok(())
    }
}

/// Twitter caps GIF uploads at 15 MB on web (5 MB mobile) and
/// 1280×1080 dimensions; our retina-resolution recordings blow past
/// both. After every successful GIF finalize we attempt to spawn
/// `ffmpeg` to convert the GIF into a Twitter-friendly H.264 MP4
/// (libx264, yuv420p, +faststart, scaled to ≤1280 px wide). Returns
/// `Ok(mp4_path)` on success, or an error if ffmpeg is missing or
/// the conversion failed — in which case the GIF is still on disk.
fn convert_gif_to_mp4(gif_path: &PathBuf) -> io::Result<PathBuf> {
    let mp4_path = gif_path.with_extension("mp4");
    let output = std::process::Command::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error", "-y", "-i"])
        .arg(gif_path)
        .args([
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            // Scale to ≤1280 px wide (Twitter cap), preserve aspect,
            // and force even dimensions — libx264 + yuv420p require
            // even width and height.
            "-vf",
            "scale='min(1280,iw)':-2:flags=lanczos",
        ])
        .arg(&mp4_path)
        .output()
        .map_err(|e| io::Error::other(format!("could not spawn ffmpeg: {e}")))?;
    if output.status.success() {
        Ok(mp4_path)
    } else {
        Err(io::Error::other(format!(
            "ffmpeg exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

// ---------------------------------------------------------------------------
// Buttons
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Button {
    rect: Rect,
    label: &'static str,
}

impl Button {
    fn hovering(&self, mouse: Vec2) -> bool {
        self.rect.contains(mouse)
    }
}

struct UiButtons {
    start: Button,
    stop: Button,
    /// GIF record toggle. Present only when capture mode is on.
    rec: Option<Button>,
}

fn make_buttons(canvas_w: f32, canvas_h: f32, record_mode: bool) -> UiButtons {
    let btn_w = 100.0_f32;
    let btn_h = 36.0_f32;
    let gap = 14.0_f32;
    let group_gap = 32.0_f32;
    let cy = canvas_h - 64.0;

    let phase_w = btn_w * 2.0 + gap;
    let cap_w = btn_w; // just REC now; SHOT was dropped per user request
    let total_w = if record_mode {
        phase_w + group_gap + cap_w
    } else {
        phase_w
    };
    let cx0 = canvas_w * 0.5 - total_w * 0.5;

    let start = Button {
        rect: Rect::new(cx0, cy, btn_w, btn_h),
        label: "START",
    };
    let stop = Button {
        rect: Rect::new(cx0 + btn_w + gap, cy, btn_w, btn_h),
        label: "STOP",
    };
    let rec = if record_mode {
        let cap_x = cx0 + phase_w + group_gap;
        Some(Button {
            rect: Rect::new(cap_x, cy, btn_w, btn_h),
            label: "REC",
        })
    } else {
        None
    };

    UiButtons { start, stop, rec }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ButtonState {
    /// Not interactive (e.g. STOP while idle).
    Disabled,
    /// Default look.
    Idle,
    /// Mouse is hovering.
    Hover,
    /// Toggleable button is currently active (e.g. REC while recording).
    Active,
}

fn draw_button(btn: &Button, state: ButtonState) {
    let (stroke, halo_tint) = match state {
        ButtonState::Disabled => (BUTTON_DISABLED, None),
        ButtonState::Idle => (BUTTON_DIM, None),
        ButtonState::Hover => (BUTTON_HOT, Some(BUTTON_HOT)),
        ButtonState::Active => (
            // Recording-active is a hot red instead of amber so it
            // reads "on air" at a glance.
            Color::new(1.00, 0.35, 0.20, 1.0),
            Some(Color::new(1.00, 0.20, 0.10, 1.0)),
        ),
    };

    if let Some(glow_col) = halo_tint {
        for (inset, a) in [(-6.0, 0.10), (-3.0, 0.20)] {
            let glow = Color::new(glow_col.r, glow_col.g * 0.6, glow_col.b * 0.3, a);
            draw_rectangle_lines(
                btn.rect.x + inset,
                btn.rect.y + inset,
                btn.rect.w - inset * 2.0,
                btn.rect.h - inset * 2.0,
                2.0,
                glow,
            );
        }
    }
    draw_rectangle_lines(btn.rect.x, btn.rect.y, btn.rect.w, btn.rect.h, 1.5, stroke);

    let font_size = 18.0;
    // When the REC button is active, prepend a "●" so the recording
    // state is also legible without color (accessibility + screenshots).
    let label_owned: String;
    let label_str: &str = if matches!(state, ButtonState::Active) && btn.label == "REC" {
        label_owned = format!("\u{25CF} {}", btn.label);
        label_owned.as_str()
    } else {
        btn.label
    };
    let dim = measure_text(label_str, None, font_size as u16, 1.0);
    let tx = btn.rect.x + (btn.rect.w - dim.width) * 0.5;
    let ty = btn.rect.y + (btn.rect.h + dim.offset_y) * 0.5;
    draw_text(label_str, tx, ty, font_size, stroke);
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

fn draw_background(w: f32, h: f32) {
    clear_background(BG_DEEP);

    // Soft warm radial wash, off-center toward upper-left where the seed lives.
    let cx = w * 0.30;
    let cy = h * 0.35;
    let layers = [
        (0.85, 0.06),
        (0.65, 0.10),
        (0.45, 0.14),
        (0.30, 0.20),
        (0.18, 0.24),
        (0.10, 0.30),
    ];
    for (radius_frac, alpha) in layers {
        let r = (w.max(h)) * radius_frac;
        let c = Color::new(BG_WARM.r, BG_WARM.g, BG_WARM.b, alpha);
        draw_circle(cx, cy, r, c);
    }

    // Vignette: dark frame on the far edges.
    let v = VIGNETTE;
    draw_rectangle(0.0, 0.0, w, h * 0.06, Color::new(v.r, v.g, v.b, v.a));
    draw_rectangle(0.0, h * 0.94, w, h * 0.06, Color::new(v.r, v.g, v.b, v.a));
    draw_rectangle(0.0, 0.0, w * 0.04, h, Color::new(v.r, v.g, v.b, v.a));
    draw_rectangle(w * 0.96, 0.0, w * 0.04, h, Color::new(v.r, v.g, v.b, v.a));
}

fn draw_embers(embers: &[Ember]) {
    for e in embers {
        let flick = (e.flicker_phase.sin() * 0.5 + 0.5).powf(1.5);
        let alpha = e.base_alpha * (0.4 + 0.6 * flick);
        let core = Color::new(EMBER_BRIGHT.r, EMBER_BRIGHT.g, EMBER_BRIGHT.b, alpha);
        let halo = Color::new(EMBER_DIM.r, EMBER_DIM.g, EMBER_DIM.b, alpha * 0.30);
        draw_circle(e.pos.x, e.pos.y, e.radius * 2.4, halo);
        draw_circle(e.pos.x, e.pos.y, e.radius, core);
    }
}

fn draw_seed(seed_pt: Vec2, intensity: f32) {
    let i = intensity.clamp(0.0, 1.0);
    for (r, a) in [(46.0, 0.05), (28.0, 0.10), (14.0, 0.22), (6.0, 0.7)] {
        let c = Color::new(
            BRANCH_GLOW.r,
            BRANCH_GLOW.g * 0.7,
            BRANCH_GLOW.b * 0.3,
            a * i,
        );
        draw_circle(seed_pt.x, seed_pt.y, r, c);
    }
    draw_circle(
        seed_pt.x,
        seed_pt.y,
        2.5 * i,
        Color::new(BRANCH_TIP.r, BRANCH_TIP.g, BRANCH_TIP.b, 0.95 * i),
    );
}

/// Apply a fake X-axis rotation in 2D using a pinhole-camera
/// perspective projection. Treats the input point as living on a
/// flat plane at z=0, rotates that plane around a horizontal axis
/// at `pivot_y` by `theta` radians, and projects back through a
/// camera at z=+focal looking down -z.
///
/// At `theta = 0` it's the identity. As `theta` grows positively,
/// points above the pivot tip *toward* the camera (z > 0, scale > 1
/// → they appear larger), and points below the pivot tip *away*
/// from it (z < 0, scale < 1 → they appear smaller). That
/// asymmetric scaling is what gives the rotation a clear "tilting
/// toward you / away from you" direction instead of the symmetric
/// vertical squash a pure cos-multiplier would produce.
///
/// Still strictly 2D rendering — we just do the projection math
/// before handing coordinates to macroquad's `draw_line` /
/// `draw_circle`.
#[inline]
fn project_x_rot(
    p: Vec2,
    z_world: f32,
    pivot_x: f32,
    pivot_y: f32,
    theta: f32,
    focal: f32,
) -> Vec2 {
    let local_x = p.x - pivot_x;
    // Up-positive Y: moving up the screen = larger model-y, so the
    // sign convention matches how a human thinks about "above the
    // pivot".
    let local_y = pivot_y - p.y;
    let (sin_t, cos_t) = theta.sin_cos();
    // X-axis rotation of the (y, z) plane:
    //   y' = y · cos θ - z · sin θ
    //   z' = y · sin θ + z · cos θ
    // Sign convention: theta > 0 → top of tree (local_y > 0) ends up
    // at z' > 0, i.e. closer to the camera at z = +focal — reads as
    // "tipping toward the viewer". `z_world` is each segment's
    // baked-in depth so the tree has volume even at θ = 0.
    let y_rotated = local_y * cos_t - z_world * sin_t;
    let z_rotated = local_y * sin_t + z_world * cos_t;
    // Pinhole projection. Clamp the denominator so we never blow up
    // when the rotated point would otherwise pass through or behind
    // the camera.
    let denom = (focal - z_rotated).max(focal * 0.10);
    let scale = focal / denom;
    Vec2::new(pivot_x + local_x * scale, pivot_y - y_rotated * scale)
}

/// Draw a segment up to a fractional reveal `t_max` in [0, 1].
/// `(pivot_x, pivot_y)`, `theta`, and `focal` apply the X-axis
/// rotation defined above to every sampled position; pass
/// `theta = 0.0` for the unrotated view.
fn draw_segment(
    seg: &Segment,
    t_max: f32,
    is_leaf: bool,
    pivot_x: f32,
    pivot_y: f32,
    theta: f32,
    focal: f32,
) {
    let n_total = seg.samples.len();
    let n = ((n_total - 1) as f32 * t_max.clamp(0.0, 1.0)).floor() as usize;
    if n == 0 {
        return;
    }

    // Depth-based dimming: deeper branches are dimmer + thinner.
    let depth_dim = 0.92_f32.powi(seg.depth as i32);
    let depth_thick = 0.78_f32.powi(seg.depth as i32);

    // Three glow passes, back to front: halo, glow, core.
    // We taper thickness from base (full) to tip (half).
    for i in 0..n {
        // Per-sample Z so the start of this segment matches the
        // tip Z of its parent — junctions stay continuous.
        let a = project_x_rot(
            seg.samples[i],
            seg.z_samples[i],
            pivot_x,
            pivot_y,
            theta,
            focal,
        );
        let b = project_x_rot(
            seg.samples[i + 1],
            seg.z_samples[i + 1],
            pivot_x,
            pivot_y,
            theta,
            focal,
        );
        let f = i as f32 / (n_total - 1) as f32;
        let taper = 1.0 - f * 0.55;
        let t_base = depth_thick * taper;

        let halo_w = (10.0 * t_base).max(2.5);
        let glow_w = (4.5 * t_base).max(1.2);
        let core_w = (1.6 * t_base).max(0.45);

        // Floor branch alphas so deep tendrils stay clearly
        // visible. The pulse heads + trails are intrinsically
        // bright and don't dim with depth, so without a floor on
        // the line we'd see bright pulses on barely-visible lines
        // — reading as "dots floating outside the line" rather
        // than "pulses travelling along the cord".
        let halo_a = (0.10 * depth_dim).max(0.06);
        let glow_a = (0.28 * depth_dim).max(0.20);
        let core_a = (0.85 * depth_dim).max(0.65);

        draw_line(
            a.x,
            a.y,
            b.x,
            b.y,
            halo_w,
            Color::new(BRANCH_HALO.r, BRANCH_HALO.g, BRANCH_HALO.b, halo_a),
        );
        draw_line(
            a.x,
            a.y,
            b.x,
            b.y,
            glow_w,
            Color::new(BRANCH_GLOW.r, BRANCH_GLOW.g, BRANCH_GLOW.b, glow_a),
        );
        draw_line(
            a.x,
            a.y,
            b.x,
            b.y,
            core_w,
            Color::new(BRANCH_CORE.r, BRANCH_CORE.g, BRANCH_CORE.b, core_a),
        );
    }

    // Growth tip flare: a small bright bead at the leading edge
    // while the segment is still revealing. Heavily dim at depth
    // so deep tendrils don't flash bright dots all over the canvas.
    if t_max < 0.999 {
        let tip_idx = n.min(n_total - 1);
        let p = project_x_rot(
            seg.samples[tip_idx],
            seg.z_samples[tip_idx],
            pivot_x,
            pivot_y,
            theta,
            focal,
        );
        let depth_dim_tip = 0.65_f32.powi(seg.depth as i32);
        for (r, a) in [(7.0, 0.14), (3.4, 0.40), (1.4, 0.85)] {
            draw_circle(
                p.x,
                p.y,
                r * depth_dim_tip,
                Color::new(BRANCH_TIP.r, BRANCH_TIP.g, BRANCH_TIP.b, a * depth_dim_tip),
            );
        }
    } else if is_leaf && seg.depth >= 1 && seg.depth <= 3 {
        // Permanent leaf bead — only at actual leaves and only at
        // shallow-to-mid depths so the canvas stays uncluttered.
        let p = project_x_rot(
            seg.samples[n_total - 1],
            seg.z_samples[n_total - 1],
            pivot_x,
            pivot_y,
            theta,
            focal,
        );
        let dim = 0.55_f32.powi(seg.depth as i32 - 1);
        for (r, a) in [(3.2, 0.08), (1.2, 0.30)] {
            draw_circle(
                p.x,
                p.y,
                r,
                Color::new(BRANCH_TIP.r, BRANCH_TIP.g, BRANCH_TIP.b, a * dim),
            );
        }
    }
}

fn draw_pulse(tree: &Tree, pulse: &Pulse, pivot_x: f32, pivot_y: f32, theta: f32, focal: f32) {
    // Head.
    if let Some((seg_idx, t)) = position_along_path(tree, pulse.path_idx, pulse.distance) {
        // BUG FIX: must use `at_z(t)` (the *interpolated* Z at this
        // fractional position along the segment), not `z_offset`
        // (the segment's tip Z). Using tip Z meant the head pulse
        // projected with one Z while the line at that same point
        // was drawn with `at_z(t)` — same 2D point, different Z,
        // different projected pixel → head visibly drifted off the
        // line at high pitch angles. The trail loop below already
        // does this correctly.
        let pos = project_x_rot(
            tree.segments[seg_idx].at(t),
            tree.segments[seg_idx].at_z(t),
            pivot_x,
            pivot_y,
            theta,
            focal,
        );
        let life = pulse.life.clamp(0.0, 1.0);
        let r = 5.5 * life;
        // Tighter halo (was r + 8.0). The previous halo radius
        // bled well past the line and reinforced the floating-dot
        // illusion when the line under it was dim.
        draw_circle(
            pos.x,
            pos.y,
            r + 3.0,
            Color::new(
                pulse.hot.r,
                pulse.hot.g * 0.45,
                pulse.hot.b * 0.15,
                0.20 * life,
            ),
        );
        draw_circle(
            pos.x,
            pos.y,
            r,
            Color::new(pulse.hot.r, pulse.hot.g, pulse.hot.b * 0.6, 0.55 * life),
        );
        draw_circle(
            pos.x,
            pos.y,
            2.2 * life,
            Color::new(PULSE_CORE.r, PULSE_CORE.g, PULSE_CORE.b, 0.95 * life),
        );
    }

    // Trail — sample backwards (or forwards for inbound) along the
    // path at fixed pixel spacing. 8 dots is plenty in ping-pong
    // mode where every leaf gets its own pulse simultaneously;
    // more was visual noise.
    const TRAIL_LEN: usize = 8;
    for i in 1..=TRAIL_LEN {
        let trail_dist = match pulse.dir {
            PulseDir::Outbound => pulse.distance - (i as f32 * 6.0),
            PulseDir::Inbound => pulse.distance + (i as f32 * 6.0),
        };
        if trail_dist < 0.0 || trail_dist > tree.leaf_path_lengths[pulse.path_idx] {
            continue;
        }
        if let Some((seg_idx, t)) = position_along_path(tree, pulse.path_idx, trail_dist) {
            let pos = project_x_rot(
                tree.segments[seg_idx].at(t),
                tree.segments[seg_idx].at_z(t),
                pivot_x,
                pivot_y,
                theta,
                focal,
            );
            let f = (1.0 - i as f32 / TRAIL_LEN as f32) * pulse.life;
            // Tighter trail dots — half the previous radius and
            // alpha. Pulses now read as a comet-tail along the
            // line rather than a pile of bright dots overwhelming
            // a thin line.
            let r = 0.9 + f * 1.8;
            let a = f * 0.30;
            draw_circle(
                pos.x,
                pos.y,
                r,
                Color::new(
                    pulse.hot.r * 0.95,
                    pulse.hot.g * 0.55,
                    pulse.hot.b * 0.20,
                    a,
                ),
            );
        }
    }
}

/// Pick a left-edge `x` for a text block of width `total_w` so that
/// it visually centers around `center_frac * canvas_w` but never
/// overflows the right margin (or the left margin, defensively).
/// Used by both the title and the caption so any future caption
/// text that is longer than the budget slides leftward instead of
/// running off the canvas.
fn clamp_text_x(canvas_w: f32, total_w: f32, center_frac: f32) -> f32 {
    let margin = canvas_w * 0.04;
    let max_end = canvas_w - margin;
    let min_start = margin;
    let mut start_x = canvas_w * center_frac - total_w * 0.5;
    let end_x = start_x + total_w;
    if end_x > max_end {
        start_x -= end_x - max_end;
    }
    if start_x < min_start {
        start_x = min_start;
    }
    start_x
}

/// `time_s` is wall-clock seconds (e.g. `get_time()`), used to drive
/// the per-letter flicker and spark phases. `crackle` in `[0, 1]`
/// controls the intensity of the electrification: 0 turns the effect
/// off entirely, 1 is full warm-up "ignition". Typical baseline
/// during steady state is ~0.30.
fn draw_title(canvas_w: f32, canvas_h: f32, alpha: f32, time_s: f32, crackle: f32) {
    if alpha <= 0.0 {
        return;
    }
    let text = "MYELON";
    // Bigger headline, broader letter spacing.
    let font_size = (canvas_h * 0.058).round();
    let letter_pad = font_size * 0.70;

    let glyphs: Vec<(char, f32)> = text
        .chars()
        .map(|c| {
            let s = String::from(c);
            let dim = measure_text(&s, None, font_size as u16, 1.0);
            (c, dim.width)
        })
        .collect();
    let total_w: f32 = glyphs.iter().map(|(_, w)| w + letter_pad).sum::<f32>() - letter_pad;

    // Center the block at 74% of canvas width, but slide it left if
    // the rendered glyphs would overflow the right margin (or right
    // if they'd overflow the left margin — covers very short window
    // widths defensively).
    let start_x = clamp_text_x(canvas_w, total_w, 0.74);
    // Raised so the right-side text block sits visually centered with
    // the tree mass instead of below it.
    let y = canvas_h * 0.62;

    // Multi-pass rendering thickens the bitmap font (macroquad's
    // default is thin). Two layers of warm halo behind, then a
    // 5-point cardinal cluster of the main glyph for a near-bold
    // weight in pure-white.
    let main_offsets = [(0.0, 0.0), (1.0, 0.0), (-1.0, 0.0), (0.0, 1.0), (0.0, -1.0)];
    let crackle = crackle.clamp(0.0, 1.0);

    let mut x = start_x;
    for (i, (c, w)) in glyphs.iter().enumerate() {
        let s = String::from(*c);

        // Per-letter brightness flicker. Each letter gets its own
        // phase (offset by `i`) so they don't pulse in unison —
        // gives the "buzzing nerve fibre" feel rather than a
        // uniform whole-word strobe.
        //
        // Two octaves at ~1.1 Hz / 2.7 Hz fundamentals — slow
        // enough that the eye actually catches the brightness
        // drift instead of averaging it out (the previous
        // ~3.7 Hz / 9 Hz pair sat near the flicker-fusion limit
        // and read as a steady glow). Amplitude pushed to ±32 %
        // so it registers at a glance.
        let f_phase = time_s * 7.0 + i as f32 * 1.7;
        let flicker_raw = f_phase.sin() * 0.70 + (f_phase * 2.4 + 0.7).sin() * 0.30;
        let alpha_mul = (1.0 + flicker_raw * 0.32 * crackle).clamp(0.45, 1.55);

        // Outer warm halo, two octaves; alpha modulation works
        // here because each per-pass alpha is low (0.18 / 0.32),
        // so the 8-stamp accumulation doesn't fully saturate and
        // the flicker still rides through.
        for (off, ha) in [(4.0, 0.18), (2.0, 0.32)] {
            let halo = Color::new(
                BRANCH_GLOW.r,
                BRANCH_GLOW.g * 0.65,
                BRANCH_GLOW.b * 0.30,
                ha * alpha * alpha_mul,
            );
            for (dx, dy) in [(off, off), (-off, off), (off, -off), (-off, -off)] {
                draw_text(&s, x + dx, y + dy, font_size, halo);
            }
        }
        // Main: 5 cardinal-offset passes for thicker, brighter
        // strokes.
        //
        // Caveat: alpha-only modulation is *invisible* on the main
        // glyph. Each of the 5 passes blends with the previous one,
        // and the central stroke pixels saturate to ≥99 % brightness
        // even at alpha 0.5 (`1 - 0.5⁵ = 0.969`). So instead we
        // modulate the source RGB directly — the dim half of the
        // flicker cycle drops the colour toward ~(0.6, 0.6, 0.6)
        // and survives the multi-pass overwrite as genuinely dim
        // text. White can't go brighter than white, so the bright
        // half of the cycle is delivered by the halo brightening
        // (alpha-modulated above) and the spark particles below.
        let bright = alpha_mul.clamp(0.0, 1.0);
        let main = Color::new(1.00 * bright, 0.99 * bright, 0.97 * bright, alpha);
        for (dx, dy) in main_offsets {
            draw_text(&s, x + dx, y + dy, font_size, main);
        }

        // Sparks: short-lived bright pinpoints scattered across each
        // letter's bounding box. Visibility is a sin-curve raised to
        // a high power, so each spark briefly peaks then disappears,
        // giving the "crackling" rhythm rather than a constant glow.
        if crackle > 0.05 {
            let n_sparks = 2 + (3.0 * crackle) as usize;
            for k in 0..n_sparks {
                let p = time_s * (14.0 + k as f32 * 4.7) + i as f32 * 2.7 + k as f32 * 1.13;
                let vis = (p.sin() * 0.5 + 0.5).powi(7);
                if vis < 0.04 {
                    continue;
                }
                let px = (p * 0.83).sin() * 0.5 + 0.5; // 0..1
                let py = (p * 1.27 + 0.3).cos() * 0.5 + 0.5; // 0..1
                let sx = x - 2.0 + (w + 4.0) * px;
                // Letter cap-height extends ~0.7 * font_size above
                // the baseline `y`; sample within that span.
                let sy = y - font_size * (0.05 + py * 0.65);
                let sa = vis * alpha * 0.95 * crackle;
                // Three-layer spark for stronger orange saturation:
                //   outer halo  — wide, fully-orange BRANCH_GLOW
                //   amber mid   — narrower, slightly hotter
                //   ivory core  — tiny pinpoint so the dominant
                //                 colour reads as orange, not white
                draw_circle(
                    sx,
                    sy,
                    5.0,
                    Color::new(BRANCH_GLOW.r, BRANCH_GLOW.g, BRANCH_GLOW.b, sa * 0.55),
                );
                draw_circle(
                    sx,
                    sy,
                    2.4,
                    Color::new(1.00, BRANCH_GLOW.g + 0.25, BRANCH_GLOW.b + 0.15, sa * 0.85),
                );
                draw_circle(sx, sy, 0.7, Color::new(1.00, 0.95, 0.80, sa));
            }
        }

        x += w + letter_pad;
    }
}

/// Phase-aware status caption that walks a viewer through the
/// transport lifecycle: the growth animation maps to a real
/// `myelon` / `disruptor-mp` init handshake (allocate → discover →
/// attach → ready), then the live phase shows what the established
/// ring is currently doing (broadcast / ping-pong).
fn phase_caption(app: &App) -> &'static str {
    match app.phase {
        // Kept short so it never overflows the right edge in the
        // standard window. The growing/live captions are
        // INITIALIZE TRANSPORT / DISCOVER CONSUMERS / ATTACH RING /
        // READY / BROADCAST / PING-PONG, which all fit within the
        // same column width budget.
        Phase::Idle => "PRESS START",
        Phase::Growing => match app.tree.as_ref() {
            Some(tree) => {
                let frac = (app.growth_clock / tree.full_grow_duration.max(0.01)).clamp(0.0, 1.0);
                if frac < 0.25 {
                    "INITIALIZE TRANSPORT"
                } else if frac < 0.55 {
                    "DISCOVER CONSUMERS"
                } else if frac < 0.85 {
                    "ATTACH RING"
                } else {
                    "READY"
                }
            }
            None => "INITIALIZE TRANSPORT",
        },
        Phase::Live => match app.pulse_system.effective_mode {
            Mode::Broadcast => "BROADCAST",
            Mode::Pingpong => "PING-PONG",
            // Alternating resolves to one of the two by the time we
            // hit Live; this branch is just defensive.
            Mode::Alternating => "READY",
        },
    }
}

/// Letter-spaced caps subtitle just below MYELON. Names the current
/// lifecycle stage in the same warm-bold treatment as the title so
/// the pair reads as one block.
fn draw_caption(text: &str, canvas_w: f32, canvas_h: f32, alpha: f32) {
    if alpha <= 0.0 || text.is_empty() {
        return;
    }
    let font_size = (canvas_h * 0.028).round().max(16.0);
    let letter_pad = font_size * 0.82;

    let glyphs: Vec<(char, f32)> = text
        .chars()
        .map(|c| {
            let s = String::from(c);
            let dim = measure_text(&s, None, font_size as u16, 1.0);
            (c, dim.width)
        })
        .collect();
    let total_w: f32 = glyphs.iter().map(|(_, w)| w + letter_pad).sum::<f32>() - letter_pad;

    let start_x = clamp_text_x(canvas_w, total_w, 0.74);
    let y = canvas_h * 0.72;

    let main = Color::new(0.99, 0.96, 0.91, 0.92 * alpha);
    let main_offsets = [(0.0, 0.0), (1.0, 0.0), (-1.0, 0.0), (0.0, 1.0), (0.0, -1.0)];

    let mut x = start_x;
    for (c, w) in &glyphs {
        let s = String::from(*c);
        // Halo, slightly tighter than MYELON so the subtitle reads
        // as related but secondary.
        for (off, ha) in [(3.0, 0.15), (1.5, 0.28)] {
            let halo = Color::new(
                BRANCH_GLOW.r,
                BRANCH_GLOW.g * 0.55,
                BRANCH_GLOW.b * 0.25,
                ha * alpha,
            );
            for (dx, dy) in [(off, off), (-off, off), (off, -off), (-off, -off)] {
                draw_text(&s, x + dx, y + dy, font_size, halo);
            }
        }
        for (dx, dy) in main_offsets {
            draw_text(&s, x + dx, y + dy, font_size, main);
        }
        x += w + letter_pad;
    }
}

fn draw_hud(sys: &PulseSystem, leaf_paths: usize, fps: f32) {
    let line = format!(
        "branches = {leaf_paths}  •  pulses = {}  •  {:.0} fps",
        sys.pulses.len(),
        fps
    );
    draw_text(
        &line,
        18.0,
        screen_height() - 18.0,
        13.0,
        Color::new(0.95, 0.92, 0.88, 0.30),
    );
}

// ---------------------------------------------------------------------------
// App state machine
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    Growing,
    Live,
}

struct App {
    phase: Phase,
    tree: Option<Tree>,
    pulse_system: PulseSystem,
    growth_clock: f32,
    title_alpha: f32,
    seed_pulse: f32, // 0..1 visibility intensity for the seed glow
    /// Seconds since the `Live` phase began. Used to drive a brief
    /// warm-up boost on branch glow at the init→live handoff so the
    /// "system online" moment reads deliberately.
    live_clock: f32,
    /// PRNG seed of the current tree, kept so resize can rebuild
    /// at new dimensions while preserving the same shape.
    seed: Option<u64>,
    /// Target leaf count, captured at construction.
    leaf_target: usize,
    /// Whether `--record` was passed: enables capture buttons.
    record_mode: bool,
    /// `Some(rec)` while a GIF is being recorded; the encoder
    /// finalizes the file when this is set back to `None`.
    gif_recorder: Option<GifRecorder>,
    /// Brief on-screen confirmation after a recording starts or
    /// finalizes. `(text, ttl_seconds)`. None = nothing shown.
    shot_toast: Option<(String, f32)>,
    /// Most recently rendered phase caption — used to detect text
    /// changes and trigger a soft crossfade so captions don't
    /// hard-cut between INITIALIZE → DISCOVER → ATTACH → READY →
    /// mode label.
    last_caption: &'static str,
    /// Fade-in alpha for the current caption: ramps from 0 to 1
    /// over `CAPTION_FADE_S` whenever the caption text changes,
    /// then stays at 1.
    caption_fade_in: f32,
    /// Time accumulator that drives the pitch oscillation. Starts
    /// ticking partway through growth (around the `ATTACH RING`
    /// caption), so the tree is already breathing in 3D before
    /// pulses arrive — the rotation feels like part of the system
    /// coming alive, not a separate effect bolted onto the steady
    /// state.
    rotation_clock: f32,
}

const CAPTION_FADE_S: f32 = 0.45;
/// Growth fraction at which the tree starts pitching. `0.55` lines
/// up with the `ATTACH RING` caption (its band runs `0.55..0.85`),
/// so the rotation literally begins at "ring attached".
const ROTATION_START_GROWTH_FRAC: f32 = 0.55;
/// Angular frequency of the pitch oscillation in radians per
/// second. Period = `2π / ROTATION_SPEED_RAD_S`. At `0.40` the
/// full back-and-forth takes ~16 s, slow enough to feel meditative
/// without dwelling at any one orientation.
const ROTATION_SPEED_RAD_S: f32 = 0.40;
/// Pitch oscillation amplitude in radians. `π/4` (45°) keeps the
/// tree well clear of edge-on (`±π/2`), so it never collapses to a
/// horizontal line; the cycle is always in the zone where
/// perspective foreshortening reads as a 3D tilt.
const ROTATION_AMPLITUDE_RAD: f32 = std::f32::consts::FRAC_PI_4;

impl App {
    fn new(mode: Mode, leaf_target: usize, record_mode: bool) -> Self {
        Self {
            phase: Phase::Idle,
            tree: None,
            pulse_system: PulseSystem::new(mode),
            growth_clock: 0.0,
            title_alpha: 0.0,
            seed_pulse: 0.0,
            live_clock: 0.0,
            seed: None,
            leaf_target,
            record_mode,
            gif_recorder: None,
            shot_toast: None,
            last_caption: "",
            caption_fade_in: 0.0,
            rotation_clock: 0.0,
        }
    }

    fn start(&mut self, canvas_w: f32, canvas_h: f32) {
        let seed = seed_from_clock();
        self.seed = Some(seed);
        self.tree = Some(build_tree(canvas_w, canvas_h, self.leaf_target, seed));
        self.phase = Phase::Growing;
        self.growth_clock = 0.0;
        self.title_alpha = 0.0;
        self.live_clock = 0.0;
        self.rotation_clock = 0.0;
        self.pulse_system.pulses.clear();
        self.pulse_system.broadcast_clock = 0.0;
        self.pulse_system.alternation_clock = 0.0;
        self.pulse_system.path_states.clear();
        // Force the next caption to fade in fresh — useful when
        // restarting without a STOP detour, so PRESS START → INIT
        // doesn't hard-cut.
        self.last_caption = "";
        self.caption_fade_in = 0.0;
    }

    fn stop(&mut self) {
        self.phase = Phase::Idle;
        self.tree = None;
        self.seed = None;
        self.growth_clock = 0.0;
        self.title_alpha = 0.0;
        self.live_clock = 0.0;
        self.rotation_clock = 0.0;
        self.pulse_system.pulses.clear();
        self.pulse_system.path_states.clear();
        self.last_caption = "";
        self.caption_fade_in = 0.0;
    }

    /// Rebuild the current tree at new canvas dimensions, preserving
    /// shape (same seed) and current animation phase. Pulses are
    /// cleared because the path lengths change, but per-path state
    /// re-initializes on the next tick. No-op when idle.
    fn resize(&mut self, canvas_w: f32, canvas_h: f32) {
        let Some(seed) = self.seed else {
            return;
        };
        self.tree = Some(build_tree(canvas_w, canvas_h, self.leaf_target, seed));
        self.pulse_system.pulses.clear();
        self.pulse_system.path_states.clear();
    }

    fn show_toast(&mut self, message: String, ttl: f32) {
        self.shot_toast = Some((message, ttl));
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn window_conf() -> Conf {
    Conf {
        window_title: "myelon-pulse".to_owned(),
        window_width: 1280,
        window_height: 800,
        sample_count: 4,
        fullscreen: false,
        high_dpi: true,
        ..Default::default()
    }
}

#[macroquad::main(window_conf)]
async fn main() {
    let args = Args::parse();
    let leaf_target = args.branches.unwrap_or_else(default_leaf_target);

    let mut last_size = (screen_width(), screen_height());
    let mut embers = build_embers(last_size.0, last_size.1);
    let mut app = App::new(args.mode, leaf_target, args.record);
    let mut fps_smoothed = 60.0_f32;

    if args.autostart {
        app.start(last_size.0, last_size.1);
    }

    loop {
        if is_key_pressed(KeyCode::Escape) || is_key_pressed(KeyCode::Q) {
            break;
        }

        let cur_size = (screen_width(), screen_height());
        if (cur_size.0 - last_size.0).abs() > 0.5 || (cur_size.1 - last_size.1).abs() > 0.5 {
            last_size = cur_size;
            embers = build_embers(cur_size.0, cur_size.1);
            app.resize(cur_size.0, cur_size.1);
        }

        let dt = get_frame_time().min(0.05);
        fps_smoothed = fps_smoothed * 0.92 + (1.0 / dt.max(1e-3)) * 0.08;

        // Mouse / button input.
        let mouse_pos = {
            let (mx, my) = mouse_position();
            Vec2::new(mx, my)
        };
        let buttons = make_buttons(cur_size.0, cur_size.1, app.record_mode);
        let start_hover = buttons.start.hovering(mouse_pos);
        let stop_hover = buttons.stop.hovering(mouse_pos);
        // START only fires from Idle (so Growing / Live can't be
        // restarted accidentally by clicking it again — that would
        // reset the tree mid-flight); STOP only fires when there's
        // something running.
        let start_enabled = matches!(app.phase, Phase::Idle);
        let stop_enabled = !matches!(app.phase, Phase::Idle);
        let rec_hover = buttons
            .rec
            .as_ref()
            .map(|b| b.hovering(mouse_pos))
            .unwrap_or(false);

        if is_mouse_button_pressed(MouseButton::Left) {
            if start_hover && start_enabled {
                app.start(cur_size.0, cur_size.1);
            } else if stop_hover && stop_enabled {
                app.stop();
            } else if rec_hover && app.record_mode {
                if app.gif_recorder.is_some() {
                    // Stop recording: drop encoder, finalize the
                    // GIF, then try ffmpeg → MP4 (Twitter-friendly).
                    if let Some(rec) = app.gif_recorder.take() {
                        let frames = rec.frames_written;
                        let gif_path = rec.path.clone();
                        drop(rec);
                        println!(
                            "[capture] GIF saved · {} frames · {}",
                            frames,
                            gif_path.display()
                        );
                        match convert_gif_to_mp4(&gif_path) {
                            Ok(mp4_path) => {
                                println!("[capture] MP4 saved · {}", mp4_path.display());
                                app.show_toast(
                                    format!("MP4 + GIF saved → {}", mp4_path.display()),
                                    5.0,
                                );
                            }
                            Err(e) => {
                                eprintln!("[capture] MP4 conversion skipped: {e}");
                                app.show_toast(
                                    format!(
                                        "GIF saved · {} (install ffmpeg for MP4)",
                                        gif_path.display()
                                    ),
                                    5.0,
                                );
                            }
                        }
                    }
                } else {
                    match GifRecorder::start(15.0) {
                        Ok(rec) => {
                            println!("[capture] recording → {}", rec.path.display());
                            app.show_toast(format!("recording → {}", rec.path.display()), 3.0);
                            app.gif_recorder = Some(rec);
                        }
                        Err(e) => {
                            let msg = format!("GIF start failed: {e}");
                            eprintln!("[capture] {msg}");
                            app.show_toast(msg, 4.0);
                        }
                    }
                }
            }
        }
        // Spacebar toggles for keyboard fans.
        if is_key_pressed(KeyCode::Space) {
            if matches!(app.phase, Phase::Idle) {
                app.start(cur_size.0, cur_size.1);
            } else {
                app.stop();
            }
        }

        // Toast countdown.
        if let Some((_, ttl)) = &mut app.shot_toast {
            *ttl -= dt;
            if *ttl <= 0.0 {
                app.shot_toast = None;
            }
        }

        // Tick state machine.
        match app.phase {
            Phase::Idle => {
                app.title_alpha = (app.title_alpha + dt * 0.4).min(0.55);
                app.seed_pulse = (app.seed_pulse + dt * 0.4).min(0.65);
            }
            Phase::Growing => {
                app.growth_clock += dt;
                app.seed_pulse = (app.seed_pulse + dt * 0.6).min(1.0);
                if let Some(tree) = &app.tree {
                    // Linger ~0.7s on the final "READY" caption
                    // before flipping into Live so the handoff
                    // reads as deliberate (init complete, system
                    // online) without dragging.
                    if app.growth_clock >= tree.full_grow_duration + 0.7 {
                        app.phase = Phase::Live;
                    }
                    // Once growth crosses the ATTACH RING threshold,
                    // the tree starts pitching. Rotation clock ticks
                    // continuously from this point on (across the
                    // Growing → Live boundary), so the oscillation
                    // is already in progress when pulses arrive.
                    let frac =
                        (app.growth_clock / tree.full_grow_duration.max(0.01)).clamp(0.0, 1.0);
                    if args.rotate && frac >= ROTATION_START_GROWTH_FRAC {
                        app.rotation_clock += dt;
                    }
                }
                // Title fades in during the second half of growth.
                if let Some(tree) = &app.tree {
                    let frac = (app.growth_clock - tree.full_grow_duration * 0.5)
                        / (tree.full_grow_duration * 0.5).max(0.1);
                    app.title_alpha = frac.clamp(0.0, 1.0);
                }
            }
            Phase::Live => {
                app.title_alpha = (app.title_alpha + dt * 0.4).min(1.0);
                // Warm-up: seed pulses brighter for ~0.7s after the
                // init→data handoff so the "system online" beat
                // registers, then settles to baseline.
                let warmup = if app.live_clock < 0.7 {
                    1.0 + (1.0 - app.live_clock / 0.7) * 0.6
                } else {
                    1.0
                };
                app.seed_pulse = warmup;
                app.live_clock += dt;
                // Rotation clock keeps ticking across the
                // Growing → Live boundary (no grace period); the
                // oscillation that started during ATTACH RING just
                // continues while pulses fly. Gated on `--rotate`.
                if args.rotate {
                    app.rotation_clock += dt;
                }
                if let Some(tree) = &app.tree {
                    app.pulse_system.tick(dt, args.mode, tree, cur_size.0);
                }
            }
        }

        tick_embers(&mut embers, dt, cur_size.0, cur_size.1);

        // ---- draw ----
        draw_background(cur_size.0, cur_size.1);
        draw_embers(&embers);

        let seed_pt = Vec2::new(cur_size.0 * 0.10, cur_size.1 * 0.50);
        draw_seed(seed_pt, app.seed_pulse);

        // Pitch transform applied to tree segments + pulses (not to
        // background, embers, seed, title, or UI chrome).
        // `project_x_rot` does a real X-axis rotation around a
        // horizontal pivot through `(pivot_x, pivot_y)` followed by
        // pinhole-perspective projection back to 2D, with each
        // segment's baked-in `z_offset` giving the tree volumetric
        // depth so it reads as 3D even at θ = 0.
        //
        // Theta is an *oscillation*, not a continuous rotation:
        //   θ(t) = AMPLITUDE · sin(t · SPEED)
        // so the tree pitches forward to ~+45°, back through facing,
        // and to ~-45° on the other side, then back. Never reaches
        // edge-on (±90°), where the tree would degenerate to a flat
        // line. The full back-and-forth period is `2π/SPEED`
        // (~16 s at SPEED = 0.40), slow enough to feel like a 3D
        // structure breathing rather than spinning.
        //
        // The seed sits at `(seed.x, pivot_y)` — exactly on the
        // pivot line — so it stays anchored while the branches
        // breathe in 3D around it.
        let pivot_x = cur_size.0 * 0.50;
        let pivot_y = cur_size.1 * 0.50;
        let theta = ROTATION_AMPLITUDE_RAD * (app.rotation_clock * ROTATION_SPEED_RAD_S).sin();
        let focal = cur_size.1 * 1.5;

        if let Some(tree) = &app.tree {
            // Draw segments deepest-first so trunk core overlays the
            // sub-branch glow nicely.
            let mut order: Vec<usize> = (0..tree.segments.len()).collect();
            order.sort_by_key(|&i| std::cmp::Reverse(tree.segments[i].depth));
            for &i in &order {
                let seg = &tree.segments[i];
                let local = (app.growth_clock - seg.born_at).max(0.0);
                let progress = (local / seg.grow_duration).clamp(0.0, 1.0);
                let progress = if matches!(app.phase, Phase::Live) {
                    1.0
                } else {
                    progress
                };
                if progress > 0.001 {
                    draw_segment(
                        seg,
                        progress,
                        tree.is_leaf[i],
                        pivot_x,
                        pivot_y,
                        theta,
                        focal,
                    );
                }
            }

            if matches!(app.phase, Phase::Live) {
                for pulse in &app.pulse_system.pulses {
                    draw_pulse(tree, pulse, pivot_x, pivot_y, theta, focal);
                }
            }
        }

        // MYELON crackle: ambient baseline 0.30 so the brand mark
        // always feels alive; spikes to 1.0 right at the init→live
        // handoff (matching the seed warm-up) and decays back to
        // baseline over ~1.5s. STOP returns to baseline.
        let crackle = match app.phase {
            Phase::Idle => 0.30,
            Phase::Growing => 0.40,
            Phase::Live => {
                if app.live_clock < 1.5 {
                    let f = (1.0 - app.live_clock / 1.5).clamp(0.0, 1.0);
                    0.30 + f * 0.70
                } else {
                    0.30
                }
            }
        };
        let now_s = get_time() as f32;
        draw_title(cur_size.0, cur_size.1, app.title_alpha, now_s, crackle);
        // Caption alpha: full strength during Growing/Live so the
        // narrative is always readable; ramps in with the title
        // during Idle so the empty canvas doesn't pop text instantly.
        let caption_alpha = match app.phase {
            Phase::Idle => (app.title_alpha / 0.55).clamp(0.0, 1.0),
            _ => 1.0,
        };
        // Crossfade between caption strings — when phase_caption
        // returns a different value than last frame (e.g.
        // INITIALIZE → DISCOVER, READY → BROADCAST, etc.) reset the
        // fade-in clock so the new text eases in over
        // `CAPTION_FADE_S` instead of pop-cutting.
        let current_caption = phase_caption(&app);
        if current_caption != app.last_caption {
            app.last_caption = current_caption;
            app.caption_fade_in = 0.0;
        } else {
            app.caption_fade_in = (app.caption_fade_in + dt / CAPTION_FADE_S).min(1.0);
        }
        let final_caption_alpha = caption_alpha * app.caption_fade_in;
        draw_caption(current_caption, cur_size.0, cur_size.1, final_caption_alpha);

        // ---- GIF capture happens HERE, after the brand visual is
        // drawn but before any UI chrome. Otherwise the toast
        // (which shows the recording file path), the buttons, and
        // the debug HUD would all leak into the saved GIF.
        if let Some(rec) = app.gif_recorder.as_mut() {
            if let Err(e) = rec.try_capture(dt) {
                eprintln!("[capture] gif frame error: {e}");
            }
        }

        let start_state = if !start_enabled {
            ButtonState::Disabled
        } else if start_hover {
            ButtonState::Hover
        } else {
            ButtonState::Idle
        };
        let stop_state = if !stop_enabled {
            ButtonState::Disabled
        } else if stop_hover {
            ButtonState::Hover
        } else {
            ButtonState::Idle
        };
        draw_button(&buttons.start, start_state);
        draw_button(&buttons.stop, stop_state);
        if let Some(rec) = &buttons.rec {
            let s = if app.gif_recorder.is_some() {
                ButtonState::Active
            } else if rec_hover {
                ButtonState::Hover
            } else {
                ButtonState::Idle
            };
            draw_button(rec, s);
        }

        // Toast (recording start / saved status). Drawn AFTER the
        // GIF capture above so the file path it advertises doesn't
        // appear in the recording.
        if let Some((text, ttl)) = &app.shot_toast {
            // Fade out over the last 0.6 seconds of the TTL.
            let alpha = (ttl / 0.6).clamp(0.0, 1.0);
            draw_toast(text, cur_size.1, alpha);
        }

        draw_hud(
            &app.pulse_system,
            app.tree.as_ref().map(|t| t.leaf_paths.len()).unwrap_or(0),
            fps_smoothed,
        );

        next_frame().await;
    }

    // On clean exit, finalize any in-flight recording so we don't
    // lose the user's footage to a truncated trailer. Same MP4
    // conversion as the REC-stop click path.
    if let Some(rec) = app.gif_recorder.take() {
        let gif_path = rec.path.clone();
        drop(rec);
        println!("[capture] GIF finalized on exit → {}", gif_path.display());
        match convert_gif_to_mp4(&gif_path) {
            Ok(mp4_path) => println!("[capture] MP4 saved · {}", mp4_path.display()),
            Err(e) => eprintln!("[capture] MP4 conversion skipped: {e}"),
        }
    }
}

fn draw_toast(text: &str, canvas_h: f32, alpha: f32) {
    if alpha <= 0.0 || text.is_empty() {
        return;
    }
    let y = canvas_h - 38.0;
    let halo = Color::new(
        BRANCH_GLOW.r,
        BRANCH_GLOW.g * 0.55,
        BRANCH_GLOW.b * 0.25,
        0.20 * alpha,
    );
    draw_text(text, 19.0, y + 1.0, 13.0, halo);
    let main = Color::new(0.92, 0.86, 0.78, 0.65 * alpha);
    draw_text(text, 18.0, y, 13.0, main);
}
