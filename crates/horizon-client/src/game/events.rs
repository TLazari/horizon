//! What happened in the game, for agents (`events`, `wait-for`; docs/agent-control.md): the
//! state is compared once per event loop turn, after the game ran, so watching never changes
//! what the game does.
use super::Game;
use horizon_client::{Result, control::Push};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::mpsc::Sender,
    time::{Duration, Instant},
};

pub(super) const HELP: &[&str] = &[
    "events [--since SEQ]           what happened (screen, chat, death, respawn, item, dimension, world, hurt, low_health, hunger, hostile_near, task_done, task_failed), oldest first",
    "event-options [--hostile-radius R] [--low-health H] [--low-food F]  the thresholds of hostile_near, low_health and hunger (default 16 blocks, 6, 6)",
    "wait-for screen NAME|widget ID|loaded|chat TEXT|event KIND [--timeout S] [--since SEQ]  reply once it holds (default 30 s; chat and event count from SEQ, else from now)",
    "subscribe [KIND...] [--since SEQ]  push each new event (of these kinds) on this connection as a line with \"event\"; replies keep \"ok\"/\"error\"",
    "unsubscribe                    stop pushing events on this connection",
];

/// Events kept for `events`.
const KEPT: usize = 256;

/// What the last turn saw.
#[derive(PartialEq)]
struct Snapshot {
    screen: &'static str,
    world: Option<String>,
    dimension: String,
    chat: u64,
    counts: BTreeMap<String, i64>,
    health: Option<f32>,
    food: Option<i32>,
    /// Hostile mobs within the radius: id → their `hostile_near` event.
    hostiles: BTreeMap<u32, Value>,
}

/// The thresholds of the events that watch the player (`event-options`).
pub(super) struct EventOptions {
    hostile_radius: f64,
    low_health: f32,
    low_food: i32,
}
impl Default for EventOptions {
    fn default() -> Self {
        Self {
            hostile_radius: 16.0,
            low_health: 6.0,
            // Below 7 the player can't sprint.
            low_food: 6,
        }
    }
}
/// A mob leaves the radius this much past it, so one at the edge isn't
/// reported over and over.
const HOSTILE_SLACK: f64 = 2.0;
/// How long the server's damage report names the source of a health drop.
const DAMAGE_TICKS: u64 = 20;

enum Condition {
    Screen(String),
    Widget(String),
    Loaded,
    Chat(String),
    Event(String),
}

/// A connection that gets each new event as a line (`subscribe`).
struct Subscriber {
    /// Event kinds to send; empty for all.
    kinds: Vec<String>,
    push: Push,
}
impl Subscriber {
    fn wants(&self, event: &Value) -> bool {
        self.kinds.is_empty() || self.kinds.iter().any(|k| event["event"] == **k)
    }
}

struct Wait {
    condition: Condition,
    /// The event sequence number when the wait started.
    since: u64,
    started: Instant,
    timeout: Duration,
    reply: Sender<String>,
}

#[derive(Default)]
pub(super) struct Events {
    /// The sequence number of the newest event (the first is 1).
    seq: u64,
    log: VecDeque<Value>,
    last: Option<Snapshot>,
    waits: Vec<Wait>,
    subscribers: Vec<Subscriber>,
    options: EventOptions,
    /// The last damage the server reported to this player, and its tick:
    /// the source of the next `hurt`.
    damage: Option<(u64, Value)>,
}

impl Events {
    fn push(&mut self, tick: u64, mut event: Value) {
        self.seq += 1;
        event["seq"] = self.seq.into();
        event["tick"] = tick.into();
        if self.log.len() == KEPT {
            self.log.pop_front();
        }
        if !self.subscribers.is_empty() {
            let line = event.to_string();
            // A closed connection drops its subscription.
            self.subscribers
                .retain(|s| !s.wants(&event) || s.push.out.send(line.clone()).is_ok());
        }
        self.log.push_back(event);
    }
    /// `task_done` or `task_failed` for an agent task that ended with `reply`.
    pub(super) fn task_ended(&mut self, tick: u64, task: &str, reply: &Value) {
        let event = match reply.get("error") {
            Some(reason) => {
                json!({ "event": "task_failed", "task": task, "reason": reason, "reply": reply })
            }
            None => json!({ "event": "task_done", "task": task, "reply": reply }),
        };
        self.push(tick, event);
    }
    /// Subscribe a connection, or change the kinds it gets.
    fn subscribe(&mut self, kinds: Vec<String>, push: &Push) {
        self.unsubscribe(push);
        self.subscribers.push(Subscriber {
            kinds,
            push: push.clone(),
        });
    }
    fn unsubscribe(&mut self, push: &Push) -> bool {
        let before = self.subscribers.len();
        self.subscribers
            .retain(|s| s.push.connection != push.connection);
        self.subscribers.len() < before
    }
    fn since(&self, seq: u64) -> impl Iterator<Item = &Value> {
        self.log
            .iter()
            .filter(move |e| e["seq"].as_u64() > Some(seq))
    }
}

/// The events between two snapshots.
fn diff(before: &Snapshot, now: &Snapshot, chat: &[String], o: &EventOptions) -> Vec<Value> {
    let mut out = Vec::new();
    if before.world != now.world {
        if let Some(name) = &before.world {
            out.push(json!({ "event": "world", "state": "closed", "name": name }));
        }
        if let Some(name) = &now.world {
            out.push(json!({ "event": "world", "state": "opened", "name": name }));
        }
    }
    if before.dimension != now.dimension && now.world.is_some() && before.world == now.world {
        out.push(json!({ "event": "dimension", "from": before.dimension, "to": now.dimension }));
    }
    if before.screen != now.screen {
        out.push(json!({ "event": "screen", "from": before.screen, "to": now.screen }));
        if now.screen == "dead" {
            out.push(json!({ "event": "death" }));
        } else if before.screen == "dead" {
            out.push(json!({ "event": "respawn" }));
        }
    }
    // Oldest first; `chat` is newest first.
    for text in chat.iter().rev() {
        out.push(json!({ "event": "chat", "text": text }));
    }
    if before.world == now.world {
        let names = before.counts.keys().chain(now.counts.keys());
        let mut seen = std::collections::BTreeSet::new();
        for item in names.filter(|n| seen.insert(*n)) {
            let delta = now.counts.get(item).unwrap_or(&0) - before.counts.get(item).unwrap_or(&0);
            if delta != 0 {
                out.push(json!({ "event": "item", "item": item, "delta": delta }));
            }
        }
        if let (Some(was), Some(is)) = (before.health, now.health) {
            if is < was {
                out.push(json!({ "event": "hurt", "amount": was - is, "health": is }));
            }
            if is < o.low_health && was >= o.low_health && is > 0.0 {
                out.push(json!({ "event": "low_health", "health": is, "below": o.low_health }));
            }
        }
        if let (Some(was), Some(is)) = (before.food, now.food)
            && is <= o.low_food
            && was > o.low_food
        {
            out.push(json!({ "event": "hunger", "food": is, "below": o.low_food + 1 }));
        }
        for (id, event) in &now.hostiles {
            if !before.hostiles.contains_key(id) {
                out.push(event.clone());
            }
        }
    }
    out
}

/// `event-options`' flags over the current options.
fn parse_options(words: &[&str], o: &mut EventOptions) -> Result<()> {
    let usage = "event-options [--hostile-radius R] [--low-health H] [--low-food F]";
    let mut words = words.iter();
    while let Some(&flag) = words.next() {
        let value = words.next().and_then(|v| v.parse::<f64>().ok());
        let value = value.ok_or(usage)?;
        match flag {
            "--hostile-radius" if (1.0..=128.0).contains(&value) => o.hostile_radius = value,
            "--low-health" if (0.0..=1024.0).contains(&value) => o.low_health = value as f32,
            "--low-food" if (0.0..=20.0).contains(&value) => o.low_food = value as i32,
            _ => return Err(format!("{usage}: radius 1-128, health 0-1024, food 0-20").into()),
        }
    }
    Ok(())
}

/// The kinds and `--since` of `subscribe`.
fn parse_subscribe(words: &[&str]) -> Result<(Vec<String>, Option<u64>)> {
    let mut kinds = Vec::new();
    let mut since = None;
    let mut words = words.iter();
    while let Some(&w) = words.next() {
        if w == "--since" {
            let seq = words.next().and_then(|t| t.parse::<u64>().ok());
            since = Some(seq.ok_or("--since takes an event seq")?);
        } else if w.starts_with("--") {
            return Err("subscribe [KIND...] [--since SEQ]".into());
        } else {
            kinds.push(w.to_owned());
        }
    }
    Ok((kinds, since))
}

/// The condition, the timeout and `--since`.
fn parse_wait(words: &[&str]) -> Result<(Condition, Duration, Option<u64>)> {
    let mut rest = Vec::new();
    let mut timeout = 30.0;
    let mut since = None;
    let mut i = 0;
    while i < words.len() {
        if words[i] == "--timeout" {
            timeout = words
                .get(i + 1)
                .and_then(|t| t.parse::<f64>().ok())
                .ok_or("--timeout takes seconds")?;
            i += 2;
        } else if words[i] == "--since" {
            since = Some(
                words
                    .get(i + 1)
                    .and_then(|t| t.parse::<u64>().ok())
                    .ok_or("--since takes an event seq")?,
            );
            i += 2;
        } else {
            rest.push(words[i]);
            i += 1;
        }
    }
    let usage =
        "wait-for screen NAME|widget ID|loaded|chat TEXT|event KIND [--timeout S] [--since SEQ]";
    let text = rest.get(1..).map(|r| r.join(" ")).unwrap_or_default();
    let need = |text: String| {
        if text.is_empty() {
            Err(usage)
        } else {
            Ok(text)
        }
    };
    let condition = match rest.first().copied() {
        Some("screen") => Condition::Screen(need(text)?),
        Some("widget") => Condition::Widget(need(text)?),
        Some("loaded") => Condition::Loaded,
        Some("chat") => Condition::Chat(need(text)?),
        Some("event") => Condition::Event(need(text)?),
        _ => return Err(usage.into()),
    };
    Ok((
        condition,
        Duration::from_secs_f64(timeout.clamp(0.0, 3600.0)),
        since,
    ))
}

impl Game<'_> {
    fn snapshot(&self) -> Snapshot {
        let world = self.title.is_none().then(|| self.level.name.clone());
        let player = self.player.as_ref().filter(|_| world.is_some());
        let mut hostiles = BTreeMap::new();
        if let Some(p) = player {
            let radius = self.events.options.hostile_radius;
            let near = |id: &u32| {
                self.events
                    .last
                    .as_ref()
                    .is_some_and(|s| s.hostiles.contains_key(id))
            };
            for m in self.mobs.iter().filter(|m| m.alive() && m.hostile()) {
                let (dx, dy, dz) = (
                    m.position.x - p.position.x,
                    m.position.y - p.position.y,
                    m.position.z - p.position.z,
                );
                let d = (dx * dx + dy * dy + dz * dz).sqrt();
                if d <= radius || near(&m.id) && d <= radius + HOSTILE_SLACK {
                    let pos = [m.position.x, m.position.y, m.position.z];
                    let event = json!({ "event": "hostile_near", "id": m.id, "type": m.kind.id,
                                        "distance": d, "pos": pos });
                    hostiles.insert(m.id, event);
                }
            }
        }
        Snapshot {
            health: player.map(|p| p.health.health()),
            food: player.map(|p| p.health.food.food),
            hostiles,
            screen: self.screen_label(),
            dimension: self.dim.id().to_string(),
            // A remote client counts the server's lines itself.
            chat: self
                .remote
                .as_ref()
                .map_or(self.ui.chat.received, |r| r.chat_received),
            counts: if world.is_some() {
                self.inventory_counts()
            } else {
                BTreeMap::new()
            },
            world,
        }
    }

    /// ClientboundDamageEventPacket for this player: the source of the next
    /// `hurt` (the damage type, the causing and the direct entity).
    pub(super) fn note_damage(
        &mut self,
        source_type: i32,
        cause: Option<i32>,
        direct: Option<i32>,
    ) {
        let damage_type = self
            .remote
            .as_ref()
            .and_then(|r| r.registries.name("minecraft:damage_type", source_type))
            .map(str::to_owned);
        let kind = |id: Option<i32>| {
            let id = u32::try_from(id?).ok()?;
            self.mobs
                .iter()
                .find(|m| m.id == id)
                .map(|m| m.kind.id.to_string())
        };
        let source = json!({
            "damage_type": damage_type,
            "id": cause,
            "type": kind(cause),
            "direct": direct,
            "direct_type": kind(direct),
        });
        self.events.damage = Some((self.ticks, source));
    }

    fn loaded(&self) -> bool {
        self.title.is_none()
            && self.player.is_some()
            && self.streaming.pending() + self.pending_uploads.len() == 0
    }

    /// Once per event loop turn: record what changed, answer the waits that hold.
    pub(super) fn watch_events(&mut self) {
        let now = self.snapshot();
        if let Some(before) = &self.events.last
            && *before != now
        {
            // Joining or leaving a server starts another count.
            let new_chat = now.chat.saturating_sub(before.chat).min(100) as usize;
            // Newest first; the remote client keeps its lines oldest first.
            let lines = match &self.remote {
                Some(r) => r
                    .chat
                    .iter()
                    .rev()
                    .take(new_chat)
                    .map(|(t, _)| t.clone())
                    .collect(),
                None => self.ui.chat.recent_lines(new_chat),
            };
            for mut event in diff(before, &now, &lines, &self.events.options) {
                if event["event"] == "hurt" {
                    let recent = self
                        .events
                        .damage
                        .take()
                        .filter(|(tick, _)| self.ticks.saturating_sub(*tick) <= DAMAGE_TICKS);
                    event["source"] = recent.map_or(Value::Null, |(_, source)| source);
                }
                self.events.push(self.ticks, event);
            }
        }
        self.events.last = Some(now);
        if self.events.waits.is_empty() {
            return;
        }
        let waits = std::mem::take(&mut self.events.waits);
        for wait in waits {
            let done = match &wait.condition {
                Condition::Screen(name) => self.screen_label() == name,
                Condition::Widget(id) => self.ui_nodes().iter().any(|n| &n.id == id),
                Condition::Loaded => self.loaded(),
                Condition::Chat(text) => self.events.since(wait.since).any(|e| {
                    e["event"] == "chat" && e["text"].as_str().is_some_and(|t| t.contains(text))
                }),
                Condition::Event(kind) => {
                    self.events.since(wait.since).any(|e| e["event"] == **kind)
                }
            };
            let waited = wait.started.elapsed();
            let reply = if done {
                json!({ "ok": true, "waited_seconds": waited.as_secs_f64(), "screen": self.screen_label() })
            } else if waited >= wait.timeout {
                json!({
                    "error": format!(
                        "timed out after {} s; the screen is {}",
                        wait.timeout.as_secs_f64(),
                        self.screen_label()
                    ),
                    "kind": "timeout",
                })
            } else {
                self.events.waits.push(wait);
                continue;
            };
            let _ = wait.reply.send(reply.to_string());
        }
    }

    /// `events`, `wait-for`, `subscribe` and `unsubscribe`; None for other
    /// commands, Some(None) when deferred. `push` is the connection's own lines.
    pub(super) fn events_command(
        &mut self,
        words: &[&str],
        reply: &Sender<String>,
        push: Option<&Push>,
    ) -> Result<Option<Option<Value>>> {
        match words[0] {
            "subscribe" => {
                let out = push.ok_or("subscribe needs a connection to the control socket")?;
                let (kinds, since) = parse_subscribe(&words[1..])?;
                // What happened since SEQ comes in the reply, so nothing is missed.
                let missed: Vec<&Value> = since
                    .map(|seq| {
                        self.events
                            .since(seq)
                            .filter(|e| kinds.is_empty() || kinds.iter().any(|k| e["event"] == **k))
                            .collect()
                    })
                    .unwrap_or_default();
                let value = json!({ "ok": true, "kinds": kinds, "events": missed, "last": self.events.seq });
                self.events.subscribe(kinds, out);
                Ok(Some(Some(value)))
            }
            "event-options" => {
                parse_options(&words[1..], &mut self.events.options)?;
                let o = &self.events.options;
                Ok(Some(Some(json!({
                    "ok": true,
                    "hostile_radius": o.hostile_radius,
                    "low_health": o.low_health,
                    "low_food": o.low_food,
                }))))
            }
            "unsubscribe" => {
                let was = push.is_some_and(|out| self.events.unsubscribe(out));
                Ok(Some(Some(json!({ "ok": true, "was_subscribed": was }))))
            }
            "events" => {
                let since = match words.get(1..) {
                    Some(["--since", n]) => n.parse().map_err(|_| "--since takes a number")?,
                    Some([]) | None => 0,
                    _ => return Err("events [--since SEQ]".into()),
                };
                let events: Vec<&Value> = self.events.since(since).collect();
                Ok(Some(Some(
                    json!({ "events": events, "last": self.events.seq }),
                )))
            }
            "wait-for" => {
                let (condition, timeout, since) = parse_wait(&words[1..])?;
                self.events.waits.push(Wait {
                    condition,
                    since: since.unwrap_or(self.events.seq),
                    started: Instant::now(),
                    timeout,
                    reply: reply.clone(),
                });
                // Answered at the end of this turn if it already holds.
                Ok(Some(None))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(screen: &'static str, chat: u64, logs: i64) -> Snapshot {
        Snapshot {
            screen,
            world: Some("w".into()),
            dimension: "minecraft:overworld".into(),
            chat,
            counts: BTreeMap::from([("minecraft:oak_log".to_owned(), logs)]),
            health: Some(20.0),
            food: Some(20),
            hostiles: BTreeMap::new(),
        }
    }

    #[test]
    fn diffs_name_what_changed() {
        let o = EventOptions::default();
        let events = diff(
            &snap("playing", 3, 1),
            &snap("dead", 5, 0),
            &["newest".into(), "older".into()],
            &o,
        );
        let kinds: Vec<&str> = events
            .iter()
            .map(|e| e["event"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["screen", "death", "chat", "chat", "item"]);
        assert_eq!(events[2]["text"], "older", "chat oldest first");
        assert_eq!(events[4]["delta"], -1);
        let back = diff(&snap("dead", 5, 0), &snap("playing", 5, 0), &[], &o);
        assert_eq!(back[1]["event"], "respawn");
        let mut closed = snap("title", 5, 0);
        closed.world = None;
        closed.counts.clear();
        let left = diff(&snap("playing", 5, 4), &closed, &[], &o);
        assert_eq!(
            left[0],
            json!({ "event": "world", "state": "closed", "name": "w" })
        );
        assert!(
            left.iter().all(|e| e["event"] != "item"),
            "leaving drops no items"
        );
    }

    #[test]
    fn health_food_and_hostiles_cross_their_thresholds() {
        let o = EventOptions::default();
        let before = snap("playing", 0, 0);
        let mut now = snap("playing", 0, 0);
        now.health = Some(5.0);
        now.food = Some(6);
        let zombie = json!({ "event": "hostile_near", "id": 7, "type": "minecraft:zombie" });
        now.hostiles.insert(7, zombie.clone());
        let events = diff(&before, &now, &[], &o);
        let kinds: Vec<&str> = events
            .iter()
            .map(|e| e["event"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["hurt", "low_health", "hunger", "hostile_near"]);
        assert_eq!(events[0]["amount"], 15.0);
        assert_eq!(events[3], zombie);
        // Already below, already near: nothing new; healing is no hurt.
        let mut later = snap("playing", 0, 0);
        later.health = Some(5.5);
        later.food = Some(5);
        later.hostiles.insert(7, zombie);
        assert!(diff(&now, &later, &[], &o).is_empty());
        let mut opts = EventOptions::default();
        parse_options(&["--hostile-radius", "24", "--low-food", "3"], &mut opts).unwrap();
        assert_eq!((opts.hostile_radius, opts.low_food), (24.0, 3));
        assert!(parse_options(&["--hostile-radius", "500"], &mut opts).is_err());
        assert!(parse_options(&["--low-health"], &mut opts).is_err());
    }

    #[test]
    fn tasks_end_done_or_failed() {
        let mut e = Events::default();
        e.task_ended(1, "goto", &json!({ "ok": true, "arrived": true }));
        e.task_ended(2, "break", &json!({ "error": "cancelled by stop" }));
        let got: Vec<&Value> = e.since(0).collect();
        assert_eq!(
            (got[0]["event"].as_str(), got[0]["task"].as_str()),
            (Some("task_done"), Some("goto"))
        );
        assert_eq!(got[1]["event"], "task_failed");
        assert_eq!(got[1]["reason"], "cancelled by stop");
    }

    #[test]
    fn keeps_the_newest_events_numbered() {
        let mut e = Events::default();
        for i in 0..300 {
            e.push(i, json!({ "event": "chat" }));
        }
        assert_eq!(e.log.len(), KEPT);
        assert_eq!(e.since(295).count(), 5);
        assert_eq!(e.since(0).next().unwrap()["seq"], 45);
    }

    #[test]
    fn subscribers_get_the_kinds_they_asked_for() {
        let mut e = Events::default();
        let push = |connection| {
            let (out, lines) = std::sync::mpsc::channel();
            (Push { connection, out }, lines)
        };
        let (all, all_lines) = push(1);
        let (deaths, death_lines) = push(2);
        e.subscribe(Vec::new(), &all);
        e.subscribe(vec!["death".into()], &deaths);
        e.push(1, json!({ "event": "chat", "text": "hi" }));
        e.push(2, json!({ "event": "death" }));
        assert_eq!(all_lines.try_iter().count(), 2);
        let got: Vec<Value> = death_lines
            .try_iter()
            .map(|l| serde_json::from_str(&l).unwrap())
            .collect();
        assert_eq!(got.len(), 1);
        assert_eq!(
            (got[0]["event"].as_str(), got[0]["seq"].as_u64()),
            (Some("death"), Some(2))
        );
        // Subscribing again replaces the kinds; a closed connection is dropped.
        e.subscribe(vec!["chat".into()], &deaths);
        assert_eq!(e.subscribers.len(), 2);
        drop(all_lines);
        e.push(3, json!({ "event": "chat" }));
        assert_eq!(e.subscribers.len(), 1);
        assert!(e.unsubscribe(&deaths) && !e.unsubscribe(&deaths));
        assert_eq!(
            parse_subscribe(&["death", "--since", "4", "hurt"]).unwrap(),
            (vec!["death".to_owned(), "hurt".to_owned()], Some(4))
        );
        assert!(parse_subscribe(&["--since"]).is_err());
    }

    #[test]
    fn parses_conditions_and_timeouts() {
        let (c, t, since) =
            parse_wait(&["chat", "hello", "--since", "7", "there", "--timeout", "5"]).unwrap();
        assert!(matches!(c, Condition::Chat(ref s) if s == "hello there"));
        assert_eq!((t, since), (Duration::from_secs(5), Some(7)));
        assert!(matches!(
            parse_wait(&["loaded"]).unwrap().0,
            Condition::Loaded
        ));
        assert!(parse_wait(&["screen"]).is_err());
        assert!(parse_wait(&["soon"]).is_err());
    }
}
