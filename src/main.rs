#![allow(unused)]
extern crate gstreamer_app as gst_app;
extern crate gstreamer_video;
use base64::prelude::*;
use glib::{ffi::GError, object::Cast, property::PropertyGet};
use gst_app::prelude::GstBinExt;
use gstreamer::{self as gst, Bin, ClockTime};
use image::{codecs::jpeg::JpegEncoder, ImageBuffer};
use std::{
    ffi::CString,
    io::Write,
    sync::mpsc::{self, Receiver},
    time::Duration,
};
use tokio::time::sleep;

use futures_util::StreamExt as _;
use glib::{
    ffi::{g_base64_decode, g_free, gpointer},
    object::ObjectExt,
};
use gstreamer::{
    ffi::{gst_buffer_new_wrapped_full, gst_parse_launch, GstElement},
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

const VIDEO_WIDTH: i32 = 1280;
const VIDEO_HEIGHT: i32 = 720;
const VIDEO_FPS: &str = "30/1";
const VIDEO_MAX_BUFFERS: i32 = 10;
const CHANNELS: [&str; 1] = ["return-audio-feed-5"];

struct Core {
    pipeline: Pipeline,
    // audio: Vec<Element>,
    audio: Element,
    video: Element,
    muxtee: Element,
    filesink: Element,
    rtmpsink: Element,
}

#[cfg(target_os = "linux")]
const H264_ENCODER: &str = "nvh264enc preset=4 rc-mode=2 zerolatency=1 bitrate=3500";

#[cfg(target_os = "macos")]
const H264_ENCODER: &str = "vtenc_h264 realtime=true allow-frame-reordering=false bitrate=3500";

#[cfg(target_os = "linux")]
const AUDIO_ENCODER: &str = "fdkaacenc bitrate=128000";

#[cfg(target_os = "macos")]
const AUDIO_ENCODER: &str = "avenc_aac bitrate=128000";

const OUTPUT_STR: &str = concat!(
    "flvmux name=muxer streamable=1 latency=300000000 skip-backwards-streams=true ! tee name=splitter ! queue max-size-buffers=10 leaky=2 ! rtmp2sink name=rtmp sync=0 async=0",
    " splitter. ! queue max-size-buffers=10 leaky=2 ! filesink name=file sync=0 location=trash/output.flv",
);

const VIDEO_DECODE_STR: &str = concat!(
    "appsrc name=video_src format=3 block=0 is-live=1",
    " ! image/jpeg,width=1280,height=720,framerate=30/1,colorimetry=bt601,chroma-site=jpeg ! queue max-size-buffers=2",
    " ! jpegdec ! videoscale ! video/x-raw,width=1280,height=720 ! videoconvert"
);
const AUDIO_DECODE_STR: &str = concat!(
    "appsrc name=audio_src format=3 block=0 is-live=1",
    " ! audio/x-raw,rate=48000,format=S16LE,layout=interleaved,channels=1",
    " ! queue max-size-buffers=2 ! audioconvert ! audio/x-raw,layout=interleaved ! audioresample ! audio/x-raw,rate=44100"
);

fn setup_gst() -> Result<Core, anyhow::Error> {
    gstreamer::init();
    let launch_str = format!("{OUTPUT_STR} {VIDEO_DECODE_STR} ! {H264_ENCODER} ! h264parse ! muxer.video {AUDIO_DECODE_STR} ! {AUDIO_ENCODER} ! aacparse ! muxer.audio");
    println!("Pipeline: {launch_str}");

    let pipeline = gst::parse::launch(launch_str.as_str())
        .expect("Failed to create pipeline")
        .downcast::<Pipeline>()
        .expect("Failed to downcast to pipeline");
    let audio = pipeline.by_name("audio_src").unwrap();
    let video = pipeline.by_name("video_src").unwrap();
    let muxtee = pipeline.by_name("splitter").unwrap();
    let filesink = pipeline.by_name("file").unwrap();
    let rtmpsink = pipeline.by_name("rtmp").unwrap();
    rtmpsink.set_property("location", "rtmp://127.0.0.1/live/test");

    Ok(Core {
        pipeline,
        audio,
        video,
        muxtee,
        filesink,
        rtmpsink,
    })
}

fn create_empty_jpeg(width: u32, height: u32) -> Vec<u8> {
    let img = ImageBuffer::from_pixel(width, height, image::Rgb::<u8>([0, 0, 0]));
    let mut jpeg_data = Vec::new();
    let mut encoder = JpegEncoder::new_with_quality(&mut jpeg_data, 90);
    encoder.encode_image(&img).expect("Failed to encode JPEG");

    jpeg_data
}

fn example_main(
    videorx: Receiver<Vec<u8>>,
    audiorx: Receiver<Vec<u8>>,
) -> Result<(), anyhow::Error> {
    let core = setup_gst().expect("Failed to setup gst");

    let videosrc = core
        .video
        .dynamic_cast::<gst_app::AppSrc>()
        .expect("Video element is expected to be an appsrc!");
    let audiosrc = core
        .audio
        .dynamic_cast::<gst_app::AppSrc>()
        .expect("Audio element is expected to be an appsrc!");

    let mut video_pts = 0;
    let mut video_duration = ClockTime::from_seconds(1) / 30;
    let empty_jpeg = create_empty_jpeg(1280, 720);
    let mut last_frame: Vec<u8> = vec![0; 0];
    let mut last_video = std::time::SystemTime::now();
    videosrc.set_callbacks(
        gst_app::AppSrcCallbacks::builder()
            .need_data(move |appsrc, _| {
                let frame: Vec<u8>;
                let now = std::time::SystemTime::now();
                if let Ok(mut in_frame) =
                    videorx.recv_timeout(Duration::from(video_duration) + Duration::from_millis(5))
                {
                    // println!("Video duration: {:.4}ms", duration.seconds_f32() * 1000.0);
                    frame = in_frame;
                } else {
                    println!("Missing video!");
                    if last_frame.len() > 0 {
                        frame = last_frame.clone();
                    } else {
                        frame = empty_jpeg.clone();
                    }
                };

                println!(
                    "Video duration: {} {:.4}ms {:?}",
                    frame.len(),
                    video_duration.seconds_f32() * 1000.0,
                    now.duration_since(last_video)
                        .expect("video: Wrong time order")
                );

                last_frame = frame.clone();
                let mut buffer = gstreamer::Buffer::from_slice(frame);
                buffer
                    .get_mut()
                    .expect("video: Failed to get mut buffer")
                    .set_duration(video_duration);
                video_pts += video_duration.useconds();
                appsrc
                    .push_buffer(buffer)
                    .expect("video: Failed to push buffer");
            })
            .build(),
    );

    let mut audio_pts = 0;
    let mut audio_duration: ClockTime = ClockTime::from_nseconds(23333333);
    let mut last_audio = std::time::SystemTime::now();
    audiosrc.set_callbacks(
        gst_app::AppSrcCallbacks::builder()
            .need_data(move |appsrc, _| {
                let mut buffer: gstreamer::Buffer;
                let mut sample: Vec<u8>;
                let now = std::time::SystemTime::now();
                if let Ok(mut in_sample) =
                    audiorx.recv_timeout(Duration::from(audio_duration) + Duration::from_millis(5))
                {
                    sample = in_sample;
                } else {
                    println!("Missing audio!");
                    sample = vec![0 as u8; 2048];
                };

                audio_duration = ((sample.len() as u64 / 2) * ClockTime::from_seconds(1)) / 48000;
                println!(
                    "Audio duration: {} {:.4}ms {:?}",
                    sample.len(),
                    audio_duration.seconds_f32() * 1000.0,
                    now.duration_since(last_audio)
                        .expect("audio: Wrong time order"),
                );
                last_audio = now;
                let mut buffer = gstreamer::Buffer::from_slice(sample);
                buffer
                    .get_mut()
                    .expect("audio: Failed to get mut buffer")
                    .set_duration(audio_duration);
                audio_pts += audio_duration.useconds();
                appsrc
                    .push_buffer(buffer)
                    .expect("audio: Failed to push buffer");
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

        if audio_duration > video_duration && (audio_duration - video_duration).mseconds() > 100 {
            println!("Video too far behind. Synchronizing");
            video_duration = audio_duration;
        }

        if video_duration > audio_duration && (video_duration - audio_duration).mseconds() > 100 {
            println!("Audio too far behind. Synchronizing");
            audio_duration = video_duration;
        }

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
    let (videotx, videorx) = std::sync::mpsc::channel::<Vec<u8>>();
    let (audiotx, audiorx) = std::sync::mpsc::channel::<Vec<u8>>();

    let _ = tokio::spawn(async move {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut pubsub_conn = client.get_async_pubsub().await.unwrap();
        println!("Connected to redis");

        let _: () = pubsub_conn.subscribe("return-video-feed").await.unwrap();
        let _: () = pubsub_conn.subscribe(&CHANNELS).await.unwrap();
        println!("Subscribed to channels");

        let mut pubsub_stream = pubsub_conn.on_message();
        loop {
            let next = pubsub_stream.next().await.unwrap();
            let channel: String = next.get_channel().unwrap();
            // println!(
            //     "Received message: channel({}) size({})",
            //     channel,
            //     next.get_payload_bytes().len()
            // );
            let mut pubsub_msg: String = next.get_payload().unwrap();
            if channel == "return-video-feed" {
                let mut decoded = BASE64_STANDARD.decode(pubsub_msg.clone()).unwrap();
                let fixed = decoded.split_off(15);
                videotx
                    .send(fixed)
                    .map_err(|err| println!("dropped frame -- reason: {}", err));
            }

            if channel.starts_with("return-audio-feed") {
                let audio_id = channel.chars().last().unwrap();
                let decoded = BASE64_STANDARD.decode(pubsub_msg.clone()).unwrap();
                audiotx
                    .send(decoded)
                    .map_err(|err| println!("dropped audio sample -- reason: {}", err));
            }
        }
    });

    main_loop::run(|| {
        example_main(videorx, audiorx);
    });

    Ok(())
}
