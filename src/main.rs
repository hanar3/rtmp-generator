#![allow(unused)]
extern crate gstreamer_app as gst_app;
extern crate gstreamer_video;
use base64::prelude::*;
use glib::object::Cast;
use std::{
    io::Write,
    sync::mpsc::{self, Receiver},
};

use futures_util::StreamExt as _;
use glib::{
    ffi::{g_base64_decode, g_free, gpointer},
    object::ObjectExt,
};
use gstreamer::{
    ffi::gst_buffer_new_wrapped_full,
    prelude::{ElementExt, ElementExtManual, GstBinExtManual, PadExt},
    Caps, Element, ElementFactory, Pipeline,
};
use std::ffi::CStr;
use std::ptr;
mod main_loop;

const AUDIO_POOLS: usize = 1;
const AUDIO_MAX_BUFFERS: i32 = 32;
const AUDIO_FORMAT: gstreamer_audio::AudioFormat = gstreamer_audio::AudioFormat::S16le;
const AUDIO_INPUT_INTERLEAVED: gstreamer_audio::AudioLayout =
    gstreamer_audio::AudioLayout::Interleaved;
// const REDIS_AUDIO_FEEDS[AUDIO_POOLS];

struct GstAudio {
    appsrc: Element,
    queue: Element,
    audioconvert: Element,
    convertfilter: Element,
    audioresample: Element,
    audiomixer: Element,
}

const VIDEO_WIDTH: &str = "1280";
const VIDEO_HEIGHT: &str = "720";
const VIDEO_FPS: &str = "30/1";
const VIDEO_MAX_BUFFERS: i32 = 10;
const CHANNELS: [&str; 1] = [
    "return-video-feed",
    // "return-audio-feed-1",
    // "return-audio-feed-2",
    // "return-audio-feed-3",
    // "return-audio-feed-5",
];

struct GstVideo {
    appsrc: Element,
    queue: Element,
    jpegdec: Element,
    videoconvert: Element,
    h264encoder: Element,
    h264parse: Element,
}

struct Core {
    pipeline: Pipeline,
    video: GstVideo,
    audio: Vec<GstAudio>,
    flvmux: Element,
    muxtee: Element,
    filesink: Element,
    rtmpsink: Element,
}

fn handle_video() {}

fn process_video(video: &GstVideo, video_data: &'static mut Vec<u8>) {
    let mut buffer = gstreamer::buffer::Buffer::from_slice(video_data);
}

fn gst_video(pipeline: &Pipeline) -> Result<GstVideo, anyhow::Error> {
    let caps = Caps::builder("image/jpeg")
        .field("width", VIDEO_WIDTH)
        .field("height", VIDEO_HEIGHT)
        .field("colorimetry", "bt601")
        .field("interlace-mode", "progressive")
        .field("chroma-site", "jpeg")
        .build();

    let appsrc = ElementFactory::make("appsrc")
        .name("video")
        .property("caps", &caps)
        .build()?;
    let queue = ElementFactory::make("queue").build()?;
    let jpegdec = ElementFactory::make("jpegdec").build()?;
    let videoconvert = ElementFactory::make("videoconvert").build()?;
    let h264encoder = ElementFactory::make("vtenc_h264").build()?;
    let h264parse = ElementFactory::make("h264parse").build()?;
    let autovideosink = ElementFactory::make("autovideosink").build()?;

    pipeline
        .add_many([
            &appsrc,
            &queue,
            &jpegdec,
            &videoconvert,
            //&h264encoder,
            //&h264parse,
            &autovideosink,
        ])
        .expect("Failed to add elements to pipeline");

    Element::link_many([
        &appsrc,
        &queue,
        &jpegdec,
        &videoconvert,
        //&h264encoder,
        //&h264parse,
        &autovideosink,
    ])
    .expect("Failed to link video elements");

    Ok(GstVideo {
        appsrc,
        queue,
        jpegdec,
        videoconvert,
        h264encoder,
        h264parse,
    })
}

fn handle_audio() {}

fn process_audio(id: usize) {}

fn gst_audio(pipeline: &Pipeline, id: usize) -> Result<GstAudio, anyhow::Error> {
    // let audio = &mut core.audio[id];
    let queue = ElementFactory::make("queue").build()?;
    let audioconvert = ElementFactory::make("audioconvert").build()?;
    let audioresample = ElementFactory::make("audioresample").build()?;

    let convert_caps = gstreamer_audio::AudioCapsBuilder::new()
        .channels(1)
        .format(AUDIO_FORMAT)
        .layout(AUDIO_INPUT_INTERLEAVED)
        .build();
    let convertfilter = ElementFactory::make("capsfilter")
        .property("caps", &convert_caps)
        .build()?;

    let src_caps = gstreamer_audio::AudioCapsBuilder::new()
        .channels(1)
        .format(gstreamer_audio::AudioFormat::S16le)
        .layout(gstreamer_audio::AudioLayout::Interleaved)
        .build();
    let appsrc = ElementFactory::make("appsrc")
        .name(format!("audio{}", id))
        .property("caps", &src_caps)
        .build()?;

    let audiomixer = ElementFactory::make("audiomixer")
        .property("latency", 30000000 as u64)
        .build()?;

    pipeline
        .add_many([
            &appsrc,
            &queue,
            &audioconvert,
            &convertfilter,
            &audioresample,
            &audiomixer,
        ])
        .expect("Failed to add audio elements to pipeline");

    Element::link_many([
        &appsrc,
        &queue,
        &audioconvert,
        &convertfilter,
        &audioresample,
    ])
    .expect("Failed to link audio elements");

    let mixer_pad = audiomixer
        .request_pad_simple("sink_%u")
        .expect("Failed to request sink pad");
    let src_pad = audioresample
        .static_pad("src")
        .expect("failed to get static pad");
    src_pad.link(&mixer_pad)?;

    Ok(GstAudio {
        appsrc,
        queue,
        audioconvert,
        audioresample,
        convertfilter,
        audiomixer,
    })
}

fn setup_gst() -> Result<Core, anyhow::Error> {
    gstreamer::init();
    let pipeline = Pipeline::default();

    let flvmux = ElementFactory::make("flvmux").build()?;
    let muxtee = ElementFactory::make("tee").build()?;
    let filesink = ElementFactory::make("filesink").build()?;
    let rtmpsink = ElementFactory::make("rtmpsink").build()?;

    let video = gst_video(&pipeline)?;

    let mut audio = vec![gst_audio(&pipeline, 0).unwrap()];

    Ok(Core {
        pipeline,
        audio,
        filesink,
        flvmux,
        muxtee,
        rtmpsink,
        video,
    })
}

fn example_main(rx: Receiver<Vec<u8>>) -> Result<(), anyhow::Error> {
    let core = setup_gst().expect("Failed to setup gst");

    let appsrc = core
        .video
        .appsrc
        .dynamic_cast::<gst_app::AppSrc>()
        .expect("Source element is expected to be an appsrc!");

    appsrc.set_property("format", gstreamer::Format::Time);

    appsrc.set_callbacks(
        gst_app::AppSrcCallbacks::builder()
            .need_data(move |appsrc, _| {
                if let Ok(mut frame) = rx.recv() {
                    let mut buffer = gstreamer::Buffer::from_slice(frame);
                    let _ = appsrc.push_buffer(buffer);
                }
            })
            .build(),
    );

    core.pipeline.set_state(gstreamer::State::Playing);

    let bus = core
        .pipeline
        .bus()
        .expect("Pipeline without bus. Shouldn't happen!");

    for msg in bus.iter_timed(gstreamer::ClockTime::NONE) {
        use gstreamer::MessageView;

        match msg.view() {
            MessageView::Eos(..) => break,
            MessageView::Error(err) => {
                core.pipeline.set_state(gstreamer::State::Null)?;
                // return panic!("err: {}", err);
            }
            _ => (),
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();

    let _ = tokio::spawn(async move {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut pubsub_conn = client.get_async_pubsub().await.unwrap();

        // let _: () = pubsub_conn.subscribe("return-video-feed").await.unwrap();
        let _: () = pubsub_conn.subscribe(&CHANNELS).await.unwrap();

        let mut pubsub_stream = pubsub_conn.on_message();
        loop {
            let next = pubsub_stream.next().await.unwrap();
            let channel: String = next.get_channel().unwrap();
            let mut pubsub_msg: String = next.get_payload().unwrap();
            if channel == "return-video-feed" {
                let mut decoded = BASE64_STANDARD.decode(pubsub_msg.clone()).unwrap();
                tx.send(decoded.split_off(15))
                    .map_err(|err| println!("dropped frame -- reason: {}", err));
            }

            if channel.starts_with("return-audio-feed") {
                println!("audio received {}", channel);
            }
        }
    });

    main_loop::run(|| {
        example_main(rx);
    });

    Ok(())
}
