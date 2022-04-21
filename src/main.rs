use img_hash::{HashAlg, HasherConfig, ImageHash};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Serialize;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;

#[derive(Debug, Serialize, Clone)]
struct ScanningFile {
    path: PathBuf,
}

struct ScanningThread {
    id: usize,
    scanning_queue: Arc<RwLock<VecDeque<ScanningFile>>>,
    hashes: Arc<RwLock<HashMap<ImageHash, Vec<ScanningFile>>>>,
    main_progress_bar: Arc<RwLock<ProgressBar>>,
    progress_bar: ProgressBar,
}

impl ScanningThread {
    fn new(
        id: usize,
        scanning_queue: Arc<RwLock<VecDeque<ScanningFile>>>,
        hashes: Arc<RwLock<HashMap<ImageHash, Vec<ScanningFile>>>>,
        main_progress_bar: Arc<RwLock<ProgressBar>>,
    ) -> Self {
        let progress_bar = ProgressBar::new(0);
        progress_bar
            .set_style(ProgressStyle::default_spinner().template("{spinner} {prefix}: {wide_msg}"));
        progress_bar.set_prefix(format!("{}", id));
        progress_bar.set_message("started");

        Self {
            id,
            scanning_queue,
            hashes,
            progress_bar,
            main_progress_bar,
        }
    }

    async fn scan(self) {
        loop {
            let item = self.scanning_queue.write().await.pop_front();
            if let Some(file) = item {
                self.progress_bar.set_message(format!("Scanning image {}", file.path.to_str().unwrap()));
                let path = file.path.clone();
                let res: Result<ImageHash, ()> = tokio::task::spawn_blocking(move || {
                    let img = image::open(path).map_err(|_| ())?;
                    let hasher = HasherConfig::new()
                        .hash_size(10, 10)
                        .hash_alg(HashAlg::Gradient)
                        .to_hasher();
                    Ok(hasher.hash_image(&img))
                })
                .await
                .unwrap();

                let res = match res {
                    Ok(f) => f,
                    Err(_) => {
                        self.progress_bar.println(format!("failed to scan dir {}", file.path.to_str().unwrap()));
                        continue;
                    }
                };

                let mut hashes = self.hashes.write().await;
                if let std::collections::hash_map::Entry::Vacant(e) = hashes.entry(res.clone()) {
                    e.insert(vec![file]);
                } else {
                    hashes.get_mut(&res).unwrap().push(file);
                }

                self.main_progress_bar.write().await.inc(1);
            } else {
                break;
            }
        }
        self.progress_bar.finish_with_message("closing...");
        if self.id == 0 {
            self.main_progress_bar.write().await.finish();
        }
    }
}

fn recursively_load_paths(start: PathBuf, queue: Arc<RwLock<VecDeque<ScanningFile>>>) {
    let supported_extensions = ["PNG", "JPG", "jpg", "bmp", "png", "tiff", "TIFF"];
    let mut dirs = std::fs::read_dir(start).unwrap();
    while let Some(Ok(s)) = dirs.next() {
        if s.path().is_dir() {
            recursively_load_paths(s.path(), queue.clone());
        } else {
            if !supported_extensions.contains(&s.path().extension().unwrap().to_str().unwrap()) {
                continue;
            }
            queue.blocking_write().push_back(ScanningFile {
                path: s.path(),
            });
        }
    }
}

fn convert(input: &HashMap<ImageHash, Vec<ScanningFile>>) -> HashMap<String, Vec<ScanningFile>> {
    let mut output = HashMap::default();
    for (key, val) in input {
        output.insert(key.to_base64(), val.clone());
    }
    output
}

#[tokio::main]
async fn main() {
    let start_path = "/run/media/josiah/storage/main-disk/OldFamilyDrives/";
    let queue = Arc::new(RwLock::new(VecDeque::new()));
    let hashes = Arc::new(RwLock::new(HashMap::default()));

    println!("Generating inital file queue...");
    let task_queue = queue.clone();
    tokio::task::spawn_blocking(move || {
        recursively_load_paths(PathBuf::from(start_path), task_queue);
    }).await.unwrap();

    println!("Scanning for duplicates...");

    let progressbar = MultiProgress::new();
    let main_pb = Arc::new(RwLock::new(progressbar.add(ProgressBar::new(1))));
    main_pb.write().await.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed}]/[{eta}] {wide_bar:.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"),
    );
    main_pb.write().await.set_length(queue.read().await.len() as u64);

    let mut handles = vec![];
    for i in 0..num_cpus::get() {
        let thread = ScanningThread::new(i, queue.clone(), hashes.clone(), main_pb.clone());
        progressbar.insert(0, thread.progress_bar.clone());
        handles.push(tokio::task::spawn(thread.scan()));
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    progressbar.join().unwrap();

    futures::future::join_all(handles).await;

    // Finished processing
    // Write hashes
    let data = hashes.read().await;
    tokio::fs::write("output-drives.json", serde_json::to_string(&convert(&data)).unwrap())
        .await
        .unwrap();
}
