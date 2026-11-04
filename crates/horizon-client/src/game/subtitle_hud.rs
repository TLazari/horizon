//! The subtitle overlay in the game (`Hud` -> `SubtitleOverlay`): the sound
//! engine reports the sounds it plays while the overlay listens, the HUD
//! renders it (which starts or stops the listening as the "Show Subtitles"
//! option says).
//!
//! The same reports are kept for a short while for agents (`sounds`): what a
//! player would hear, a zombie groaning behind a wall or a creeper hissing
//! out of sight, with where it came from.
use super::Game;
use horizon_client::{
    hud::HudBatch,
    hud_layout,
    hud_overlays::{SubtitleFrame, SubtitleOverlay, now_ms},
};
use horizon_client_render::audio::Heard;
use std::{
    cell::RefCell,
    collections::VecDeque,
    time::{Duration, Instant},
};

/// How long the heard sounds are kept for `sounds`.
pub(super) const HEARD_KEPT: Duration = Duration::from_secs(30);
/// At most this many heard sounds are kept.
const HEARD_MAX: usize = 256;

#[derive(Default)]
pub(super) struct SubtitleState {
    overlay: RefCell<SubtitleOverlay>,
    /// The sounds heard lately, oldest first, for `sounds`.
    pub(super) heard: VecDeque<(Instant, Heard)>,
}
impl Game<'_> {
    /// Feed the sounds the engine reported since the last tick
    /// (`SoundEventListener.onPlaySound`) to the overlay while it listens,
    /// and keep them a while for agents (`sounds`).
    pub(super) fn tick_subtitles(&mut self) {
        // Always recorded: the agents hear even with subtitles off.
        self.audio.record_heard = true;
        let heard = self.audio.take_heard();
        let now_i = Instant::now();
        let log = &mut self.subtitles.heard;
        while log
            .front()
            .is_some_and(|(t, _)| now_i.duration_since(*t) > HEARD_KEPT)
            || log.len() > HEARD_MAX
        {
            log.pop_front();
        }
        let mut overlay = self.subtitles.overlay.borrow_mut();
        let listening = overlay.listening();
        let now = now_ms();
        for h in heard {
            if listening {
                let at = h.at.unwrap_or(self.audio.listener);
                overlay.on_play(Some(&h.subtitle), at, h.range, h.looping, now);
            }
            self.subtitles.heard.push_back((now_i, h));
        }
    }
    pub(super) fn draw_subtitles(&self, batch: &mut HudBatch) {
        // Hud.extractRenderState: hidden with the rest of the HUD (F1).
        if self.ui.hide_gui {
            return;
        }
        let v = &self.view;
        let frame = SubtitleFrame {
            show: self.settings.show_subtitles,
            position: v.position,
            forward: v.forwards.map(f64::from),
            up: v.up.map(f64::from),
            multiplier: f64::from(self.settings.notification_display_time),
            background: self.settings.text_background(0.8),
            width: batch.width as i32,
            height: batch.height as i32,
        };
        let atlas = &self.gui.atlas;
        let draws = self
            .subtitles
            .overlay
            .borrow_mut()
            .extract(&frame, &atlas.font, now_ms());
        hud_layout::paint(batch, atlas, &draws);
    }
}
