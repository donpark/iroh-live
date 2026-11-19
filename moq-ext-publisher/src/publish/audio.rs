//! The audio publish task: a microphone, or PCM from any other source.
//!
//! The source is read once and fans out to one encoder per rendition. A
//! rendition that fails ends on its own, and the others go on.

use moq_ext_sources::{
    AudioFormat, AudioKind, AudioSource, ByPtr, Error, SlotState, stats::SharedCell,
};
use tokio::sync::broadcast;
#[cfg(feature = "capture")]
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

#[cfg(feature = "capture")]
use super::status::Reporter;
use super::{Job, SourceClock, encoding::AudioRendition, status::RenditionState};
use crate::stats::AudioEncodeStats;

/// Publishes `source` encoded as every rendition of `renditions` until it ends or the slot stops.
pub(super) async fn run(
    mut job: Job,
    source: AudioSource,
    renditions: Vec<AudioRendition>,
    stop: CancellationToken,
) {
    let Some(_tracks) = job.take_over(&stop).await else {
        return;
    };
    let stats = job.stats.audio();
    stats.update(|stats| {
        stats.codec = renditions
            .first()
            .map(|rendition| rendition.config.codec.to_string());
        stats.renditions = renditions
            .iter()
            .map(|rendition| (rendition.name.clone(), rendition.config.codec.to_string()))
            .collect();
    });
    let result = match source.kind() {
        #[cfg(feature = "capture")]
        // Opened after `take_over`, because an echo canceller attaches to one
        // microphone at a time, and the publication this one replaces holds
        // it until it stops.
        AudioKind::Microphone(capture) => {
            microphone(&job, &stats, capture.clone(), &renditions, &stop).await
        }
        AudioKind::Pcm { format, fanout } => match fanout.upgrade() {
            Some(fanout) => {
                let frames = fanout.subscribe();
                drop(fanout);
                let _wanted = source.want();
                pcm(&job, &stats, *format, frames, &renditions, &stop).await
            }
            // The producer is already gone.
            None => Ok(()),
        },
        // Another crate in the build turned on microphones in `moq-ext-sources`
        // while this crate's `capture` is off.
        #[cfg(not(feature = "capture"))]
        #[allow(
            unreachable_patterns,
            reason = "a variant exists only with the feature"
        )]
        _ => Err(Error::invalid(
            "publishing a microphone needs the `capture` feature of moq-ext-publisher",
        )),
    };
    if stop.is_cancelled() {
        return;
    }
    match result {
        Ok(()) => {
            debug!("audio source ended");
            job.reporter.slot(SlotState::Ended);
        }
        Err(err) => {
            warn!(error = format_args!("{err:#}"), "audio publish failed");
            job.reporter.slot(SlotState::Failed(ByPtr::new(err)));
        }
    }
}

/// Marks rendition `name` failed with `err`, and drops it from the stats.
fn fail_rendition(
    job: &Job,
    stats: &SharedCell<AudioEncodeStats>,
    name: &str,
    err: &Error,
) -> ByPtr<Error> {
    warn!(rendition = %name, error = format_args!("{err:#}"), "audio rendition failed");
    stats.update(|stats| drop(stats.renditions.remove(name)));
    let shared = ByPtr::new(Error::encoder_msg(format!("{err:#}")));
    job.reporter
        .rendition(name, RenditionState::Failed(shared.clone()));
    shared
}

/// Encodes PCM from a fan-out receiver into every rendition.
async fn pcm(
    job: &Job,
    stats: &SharedCell<AudioEncodeStats>,
    format: AudioFormat,
    mut frames: broadcast::Receiver<moq_audio::Frame>,
    renditions: &[AudioRendition],
    stop: &CancellationToken,
) -> Result<(), Error> {
    let input = moq_audio::encode::Input::new(format.sample_rate, format.layout);
    let mut encoders = Vec::with_capacity(renditions.len());
    let mut last_err = None;
    for rendition in renditions {
        let mut options = moq_audio::encode::Options::default();
        options.track = Some(rendition.name.clone());
        options.config = rendition.config.clone();
        let mut broadcast = job.producer.clone();
        match moq_audio::encode::Producer::new(
            &mut broadcast,
            job.catalog.clone(),
            input.clone(),
            &options,
        ) {
            Ok(encoder) => {
                info!(track = %rendition.name, rate = format.sample_rate, "publishing audio");
                job.reporter.rendition(
                    &rendition.name,
                    RenditionState::Encoding {
                        encoder: rendition.config.codec.to_string(),
                    },
                );
                encoders.push((rendition.name.clone(), encoder));
            }
            Err(err) => {
                let err = Error::encoder(err);
                fail_rendition(job, stats, &rendition.name, &err);
                last_err = Some(err);
            }
        }
    }
    if encoders.is_empty() {
        return Err(last_err.unwrap_or_else(|| Error::invalid("no audio renditions")));
    }
    job.reporter.slot(SlotState::Running);

    let mut source_clock = SourceClock::new(job.clock.clone());
    let mut bitrates = job.bitrates.clone();
    // Rates asked for before the encoders existed apply now.
    bitrates.mark_changed();
    // The sender goes with the slot; once it has, nothing more can be asked.
    let mut bitrates_open = true;
    loop {
        let frame = tokio::select! {
            frame = frames.recv() => frame,
            changed = bitrates.changed(), if bitrates_open => {
                if changed.is_err() {
                    bitrates_open = false;
                } else {
                    let rates = bitrates.borrow_and_update().clone();
                    for (name, encoder) in &mut encoders {
                        let Some(rate) = rates.get(name) else { continue };
                        if encoder.bitrate() == *rate {
                            continue;
                        }
                        match encoder.set_bitrate(*rate) {
                            Ok(()) => debug!(rendition = %name, bps = rate.as_bps(), "audio bitrate retuned"),
                            Err(err) => warn!(rendition = %name, bps = rate.as_bps(), error = %err, "audio bitrate refused"),
                        }
                    }
                }
                continue;
            }
            () = stop.cancelled() => break,
        };
        let mut frame = match frame {
            Ok(frame) => frame,
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                debug!(missed, "the audio publish fell behind its source");
                stats.update(|stats| stats.dropped += missed);
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        frame.timestamp = source_clock.map(frame.timestamp, None);
        encoders.retain_mut(|(name, encoder)| match encoder.write(&frame) {
            Ok(()) => true,
            Err(err) => {
                let err = Error::encoder(err);
                fail_rendition(job, stats, name, &err);
                last_err = Some(err);
                false
            }
        });
        if encoders.is_empty() {
            return Err(last_err.unwrap_or_else(|| Error::invalid("no audio renditions")));
        }
        stats.update(|stats| stats.frames += 1);
    }
    // Every packet is out. Dropping the finished tracks releases them.
    let mut result = Ok(());
    for (name, encoder) in encoders {
        if let Err(err) = encoder.finish() {
            debug!(rendition = %name, error = %err, "audio rendition did not finish cleanly");
            result = Err(Error::encoder(err));
        }
    }
    result
}

/// A rendition's publication, handed from the capture thread.
#[cfg(feature = "capture")]
type Publications = Vec<(AudioRendition, moq_audio::encode::Publication)>;

/// Captures a microphone once and encodes it into every rendition through moq-audio's publications.
///
/// The capture producer owns the device, and each publication encodes one of
/// its consumers while someone reads its track. Their futures are not `Send`
/// on macOS, so they run on their own thread.
#[cfg(feature = "capture")]
async fn microphone(
    job: &Job,
    stats: &SharedCell<AudioEncodeStats>,
    capture: moq_audio::capture::Config,
    renditions: &[AudioRendition],
    stop: &CancellationToken,
) -> Result<(), Error> {
    let (handle_tx, handle) = oneshot::channel::<Result<(Publications, Vec<Error>), Error>>();
    let broadcast = job.producer.clone();
    let catalog = job.catalog.clone();
    let wanted = renditions.to_vec();
    let driver_stop = stop.child_token();
    let driver = moq_ext_sources::local_task::spawn(
        "audio-capture",
        driver_stop.clone(),
        move |stop| async move {
            let capture = match moq_audio::capture::Producer::open(capture).await {
                Ok(capture) => capture,
                Err(err) => {
                    let _ = handle_tx.send(Err(Error::device(err)));
                    return;
                }
            };
            let mut publications = Vec::new();
            let mut drivers = Vec::new();
            let mut failed = Vec::new();
            for rendition in wanted {
                let mut options = moq_audio::encode::Capture::new(capture.consume());
                options.encode.track = Some(rendition.name.clone());
                options.encode.config = rendition.config.clone();
                match moq_audio::encode::publish_capture(
                    broadcast.clone(),
                    catalog.clone(),
                    options,
                ) {
                    Ok((publication, driver)) => {
                        publications.push((rendition, publication));
                        drivers.push(driver);
                    }
                    Err(err) => {
                        warn!(rendition = %rendition.name, error = %err, "audio rendition not published");
                        failed.push((rendition.name, Error::device(err)));
                    }
                }
            }
            if publications.is_empty() {
                let err = failed
                    .pop()
                    .map(|(_, err)| err)
                    .unwrap_or_else(|| Error::invalid("no audio renditions"));
                let _ = handle_tx.send(Err(err));
                return;
            }
            let failed: Vec<Error> = failed
                .into_iter()
                .map(|(name, err)| Error::device_msg(format!("rendition {name}: {err:#}")))
                .collect();
            if handle_tx.send(Ok((publications, failed))).is_err() {
                return;
            }
            // A capture failure also reaches the publications, which report it
            // as `Status::Failed`, so the capture future only logs it.
            let capture = async move {
                if let Err(err) = capture.run().await {
                    debug!(error = %err, "microphone capture stopped");
                }
                std::future::pending::<()>().await;
            };
            // Each driver runs until its publication ends. One that fails ends
            // alone, and its publication reports why.
            let run = n0_future::join_all(drivers.into_iter().map(|driver| async move {
                if let Err(err) = driver.run().await {
                    warn!(error = %err, "a microphone publication stopped");
                }
            }));
            tokio::pin!(capture, run);
            tokio::select! {
                _ = &mut run => {}
                () = &mut capture => {}
                () = stop.cancelled() => {}
            }
        },
    )?;
    let result = follow_all(handle, job, stats, stop).await;
    // The capture thread holds the device and the echo canceller. A
    // publication replacing this one asks for them as soon as this returns,
    // so the thread must let go first.
    driver_stop.cancel();
    driver.joined().await;
    result
}

/// Follows every rendition's publication until all have ended or the slot stops.
///
/// Fails only when every rendition failed.
#[cfg(feature = "capture")]
async fn follow_all(
    handle: oneshot::Receiver<Result<(Publications, Vec<Error>), Error>>,
    job: &Job,
    stats: &SharedCell<AudioEncodeStats>,
    stop: &CancellationToken,
) -> Result<(), Error> {
    let (publications, failed) = handle
        .await
        .map_err(|_| Error::device_msg("the microphone thread stopped before it started"))??;
    for err in &failed {
        // The rendition name is in the message; the state goes to the slot's log.
        debug!(error = %err, "audio rendition not published");
    }
    let names: Vec<String> = publications
        .iter()
        .map(|(rendition, _)| rendition.name.clone())
        .collect();
    info!(renditions = ?names, "publishing microphone");
    let results = n0_future::join_all(
        publications
            .into_iter()
            .map(|(rendition, publication)| follow(publication, &job.reporter, rendition, stop)),
    )
    .await;
    let mut last_err = None;
    let mut any_ok = false;
    for (name, result) in names.iter().zip(results) {
        match result {
            Ok(()) => any_ok = true,
            Err(err) => {
                fail_rendition(job, stats, name, &err);
                last_err = Some(err);
            }
        }
    }
    match last_err {
        Some(err) if !any_ok => Err(err),
        _ => Ok(()),
    }
}

/// Reports one rendition's state changes until it ends or the slot stops.
#[cfg(feature = "capture")]
async fn follow(
    mut publication: moq_audio::encode::Publication,
    reporter: &Reporter,
    rendition: AudioRendition,
    stop: &CancellationToken,
) -> Result<(), Error> {
    let track = rendition.name.as_str();
    loop {
        let state = tokio::select! {
            state = publication.changed() => state,
            () = stop.cancelled() => return Ok(()),
        };
        let Some(state) = state else {
            // The driver exited, so the track ended.
            return Ok(());
        };
        match state.status() {
            moq_audio::encode::Status::Starting => reporter.slot(SlotState::Starting),
            moq_audio::encode::Status::Waiting => {
                reporter.slot(SlotState::Running);
                reporter.rendition(track, RenditionState::Idle);
            }
            moq_audio::encode::Status::Live => {
                reporter.slot(SlotState::Running);
                reporter.rendition(
                    track,
                    RenditionState::Encoding {
                        encoder: rendition.config.codec.to_string(),
                    },
                );
            }
            moq_audio::encode::Status::Failed(err) => {
                // moq-audio does not reopen a failed capture, so the
                // publication ends here and the caller reports the failure.
                return Err(Error::device_msg(err.to_string()));
            }
            moq_audio::encode::Status::Ended | moq_audio::encode::Status::Cancelled => {
                return Ok(());
            }
            _ => {}
        }
    }
}
