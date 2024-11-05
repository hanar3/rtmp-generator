#![allow(unused)]
use futures_util::StreamExt as _;
use gstreamer::{
    prelude::{ElementExt, ElementExtManual, GstBinExtManual, PadExt},
    Caps, Element, ElementFactory, Pipeline,
};

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
const CHANNELS: [&str; 5] = [
    "return-video-feed",
    "return-audio-feed-1",
    "return-audio-feed-2",
    "return-audio-feed-3",
    "return-audio-feed-5",
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

fn process_video() {}

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
    let h264encoder = ElementFactory::make("nvh264enc").build()?;
    let h264parse = ElementFactory::make("h264parse").build()?;

    pipeline.add_many([
        &appsrc,
        &queue,
        &jpegdec,
        &videoconvert,
        &h264encoder,
        &h264parse,
    ])?;

    Element::link_many([
        &appsrc,
        &queue,
        &jpegdec,
        &videoconvert,
        &h264encoder,
        &h264parse,
    ])?;

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
        .property("latency", 30000000)
        .build()?;

    pipeline.add_many([
        &appsrc,
        &queue,
        &audioconvert,
        &convertfilter,
        &audioresample,
    ])?;

    Element::link_many([
        &appsrc,
        &queue,
        &audioconvert,
        &convertfilter,
        &audioresample,
    ])?;

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
    let pipeline = Pipeline::default();

    let flvmux = ElementFactory::make("flvmux").build()?;
    let muxtee = ElementFactory::make("muxtee").build()?;
    let filesink = ElementFactory::make("filesink").build()?;
    let rtmpsink = ElementFactory::make("rtmpsink").build()?;

    let video = gst_video(&pipeline)?;
    video
        .h264parse
        .static_pad("src")
        .unwrap()
        .link(&flvmux.request_pad_simple("video").unwrap())?;

    let mut audio = Vec::<GstAudio>::with_capacity(AUDIO_POOLS);
    for i in 0..AUDIO_POOLS {
        audio[i] = gst_audio(&pipeline, i).unwrap();
    }

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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let _ = tokio::spawn(async {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut pubsub_conn = client.get_async_pubsub().await.unwrap();

        // let _: () = pubsub_conn.subscribe("return-video-feed").await.unwrap();
        let _: () = pubsub_conn.subscribe(&CHANNELS).await.unwrap();

        let mut pubsub_stream = pubsub_conn.on_message();

        loop {
            let channel = pubsub_stream
                .next()
                .await
                .unwrap()
                .get_channel_name()
                .to_string();
            let pubsub_msg: String = pubsub_stream.next().await.unwrap().get_payload().unwrap();

            if channel == "return-video-feed" {
                println!("Video received, {}", channel);
            }

            if channel.starts_with("return-audio-feed") {
                println!("audio received {}", channel);
            }
        }
    })
    .await;

    Ok(())
}
