use base64::{prelude::BASE64_STANDARD, Engine};
use glib::object::Cast;
use gstreamer::{
    prelude::{ElementExt, GstBinExt},
    Pipeline,
};
use redis::Commands;

fn audio() {
    let client =
        redis::Client::open("redis://127.0.0.1/").expect("audio: Failed to open redis connection");
    let mut conn = client
        .get_connection()
        .expect("audio: Failed to get redis connection");
    _ = gstreamer::init();
    let pipeline = gstreamer::parse::launch(
        "audiotestsrc ! audio/x-raw,rate=48000,channels=1,format=S16LE ! appsink name=sink",
    )
    .expect("audio: Failed to create audio pipeline")
    .downcast::<Pipeline>()
    .expect("audio: Failed to downcast audio pipeline");

    let sink = pipeline
        .by_name("sink")
        .expect("audio: Sink not found")
        .downcast::<gstreamer_app::AppSink>()
        .expect("audio: Sink downcast failed");

    _ = pipeline.set_state(gstreamer::State::Playing);

    loop {
        let sample = sink.pull_sample().expect("audio: Failed to get sample");

        let buffer = sample.buffer().expect("audio: Failed to get sample buffer");
        let map = buffer
            .map_readable()
            .ok()
            .expect("audio: Failed to map buffer");

        // println!("Received audio sample: {}", buffer.size());

        let fresult = BASE64_STANDARD.encode(map);

        let _: () = conn
            .publish("return-audio-feed-5", fresult)
            .expect("audio: Failed to publish to redis");
    }
}

fn video() {
    let client =
        redis::Client::open("redis://127.0.0.1/").expect("video: Failed to open redis connection");
    let mut conn = client
        .get_connection()
        .expect("video: Failed to get redis connection");
    _ = gstreamer::init();
    let pipeline =
        gstreamer::parse::launch("videotestsrc ! video/x-raw,width=1280,height=720 ! videoconvert ! jpegenc ! appsink name=sink")
            .expect("video: Failed to create pipeline")
            .downcast::<Pipeline>()
            .expect("video: Failed to downcast pipeline");
    let sink = pipeline
        .by_name("sink")
        .expect("video: Sink not found")
        .downcast::<gstreamer_app::AppSink>()
        .expect("video: Sink downcast failed");

    _ = pipeline.set_state(gstreamer::State::Playing);

    loop {
        let sample = sink.pull_sample().expect("video: Failed to get sample");

        let buffer = sample.buffer().expect("video: Failed to get sample buffer");
        let map = buffer
            .map_readable()
            .ok()
            .expect("video: Failed to map buffer");

        let mut arr = Vec::with_capacity(15 + map.size());
        arr.extend_from_slice(&[0u8; 15]);
        arr.extend_from_slice(&map);

        let fresult = BASE64_STANDARD.encode(arr);
        // println!("Received video sample:  {} / {}", map.size(), fresult.len());

        let _: () = conn
            .publish("return-video-feed", fresult.as_bytes())
            .expect("video: Failed to publish to redis");
    }
}

fn main() {
    let audio_t = std::thread::spawn(audio);
    let video_t = std::thread::spawn(video);

    _ = audio_t.join();
    _ = video_t.join();
}
