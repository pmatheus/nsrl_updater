use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use chrono;
use crossbeam::channel::bounded;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle, HumanBytes, HumanDuration};
use rusqlite::Connection;
use std::sync::Mutex;

struct Config {
    batch_size: usize,
    reader_buffer_size: usize,
    parser_threads: usize,
    queue_capacity: usize,
    use_mmap: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            batch_size: 25000,       // Increased batch size for better performance
            reader_buffer_size: 32 * 1024 * 1024, // 32MB buffer size for faster reading
            parser_threads: num_cpus::get(),
            queue_capacity: 100000,   // Larger queue for better throughput
            use_mmap: true,
        }
    }
}

#[derive(Clone)]
struct ImportStats {
    successful: Arc<AtomicUsize>,
    duplicates: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
}

impl ImportStats {
    fn new() -> Self {
        Self {
            successful: Arc::new(AtomicUsize::new(0)),
            duplicates: Arc::new(AtomicUsize::new(0)),
            errors: Arc::new(AtomicUsize::new(0)),
        }
    }
}

// Thread-safe logger with improved error handling and appending behavior
struct Logger {
    log_file: Mutex<Option<File>>,
    log_path: String,
}

impl Logger {
    fn new() -> Self {
        use std::fs;
        
        let now = chrono::Local::now();
        let log_path = format!("logs/sql_import_{}.log", now.format("%Y%m%d_%H%M%S"));
        
        // Create logs directory if it doesn't exist
        if let Some(parent) = std::path::Path::new(&log_path).parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                eprintln!("Warning: Failed to create logs directory: {}", e);
            }
        }
        
        // Try to open the log file immediately to verify
        let file = match fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path) {
                Ok(f) => {
                    println!("Log file created at: {}", log_path);
                    Some(f)
                },
                Err(e) => {
                    eprintln!("Warning: Failed to create log file: {}", e);
                    None
                }
            };
        
        Self {
            log_file: Mutex::new(file),
            log_path,
        }
    }
    
    fn log_message(&self, message: &str) -> io::Result<()> {
        use std::io::Write;
        
        let now = chrono::Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let log_line = format!("[{}] {}\n", timestamp, message);
        
        let mut file_guard = match self.log_file.lock() {
            Ok(guard) => guard,
            Err(e) => {
                eprintln!("Failed to lock log file mutex: {}", e);
                return Ok(());
            }
        };
        
        if let Some(file) = file_guard.as_mut() {
            file.write_all(log_line.as_bytes())?;
            file.flush()?;
        } else {
            // If file is missing, try to reopen it
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.log_path) {
                    Ok(mut f) => {
                        f.write_all(log_line.as_bytes())?;
                        f.flush()?;
                        *file_guard = Some(f);
                    },
                    Err(e) => {
                        eprintln!("Failed to reopen log file: {}", e);
                    }
                }
        }
        
        Ok(())
    }
    
    // Log errors and ensure they are written to disk immediately
    fn log_error(&self, message: &str) -> io::Result<()> {
        // Errors are only logged to file, not displayed on terminal
        self.log_message(&format!("ERROR: {}", message))
    }
}

struct SQLiteImporter {
    config: Config,
    stats: ImportStats,
    logger: Arc<Logger>,
}

impl SQLiteImporter {
    fn new(config: Config) -> Self {
        Self {
            config,
            stats: ImportStats::new(),
            logger: Arc::new(Logger::new()),
        }
    }

    fn log_message(&self, message: &str) -> io::Result<()> {
        self.logger.log_message(message)
    }

    fn import_file(self, db_path: PathBuf, sql_paths: Vec<PathBuf>) -> io::Result<()> {
        // Log start of the import process
        let _ = self.log_message(&format!("Starting import to database: {}", db_path.display()));
        let _ = self.log_message(&format!("Files to import: {}",
            sql_paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(", ")));
    
        // Set up the main database connection with performance settings
        let conn = Connection::open(&db_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        conn.execute_batch("
            PRAGMA foreign_keys = OFF;
            PRAGMA journal_mode = MEMORY;
            PRAGMA synchronous = OFF;
            PRAGMA cache_size = -16000000; /* Increased cache size to 16GB */
            PRAGMA temp_store = MEMORY;
            PRAGMA busy_timeout = 30000;
            PRAGMA mmap_size = 4294967296; /* Increased mmap size to 4GB */
            PRAGMA locking_mode = EXCLUSIVE;
            PRAGMA page_size = 4096;
        ").map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Share the database path with worker threads
        let db_path = Arc::new(db_path);
        
        // Create a bounded channel for batches of SQL statements
        let (sender, receiver) = bounded::<Vec<String>>(self.config.queue_capacity);
        // Create a flag to track when all files have been processed
        let all_files_processed = Arc::new(AtomicUsize::new(0));
        let stats = Arc::new(self.stats.clone());
        let statements_read = Arc::new(AtomicUsize::new(0));
        let statements_processed = Arc::new(AtomicUsize::new(0));
        let total_bytes = Arc::new(AtomicUsize::new(0));
        let start_time = Instant::now();
        let logger = Arc::clone(&self.logger);
        
        // Calculate the total size of SQL files for progress tracking
        let total_size: u64 = sql_paths.iter()
            .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
            .sum();
            
        // Set up multi-progress bars for overall, ETA, file, and processing stats
        let multi = MultiProgress::new();
        
        let main_pb = multi.add(ProgressBar::new(total_size));
        main_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}]\n{prefix:.bold.white.on_blue} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec})")
            .unwrap()
            .with_key("bytes", |state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                write!(w, "{}", HumanBytes(state.pos())).unwrap();
            })
            .with_key("total_bytes", |state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                write!(w, "{}", HumanBytes(state.len().unwrap_or(0))).unwrap();
            })
            .with_key("bytes_per_sec", |state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                write!(w, "{}/s", HumanBytes(state.per_sec() as u64)).unwrap();
            })
            .progress_chars("█▇▆▅▄▃▂   "));
        main_pb.set_prefix(" SQLite Importer ");
        main_pb.enable_steady_tick(std::time::Duration::from_millis(1000)); // Reduced update frequency
        
        let eta_pb = multi.add(ProgressBar::new(100));
        eta_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} {prefix:.bold.green} {wide_msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]));
        eta_pb.set_prefix(" Total Progress ");
        eta_pb.enable_steady_tick(std::time::Duration::from_millis(1000)); // Reduced update frequency
        
        let file_pb = multi.add(ProgressBar::new(100));
        file_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.blue} {prefix:.bold.blue}\n[{wide_bar:.cyan/blue}] {pos}% - {wide_msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
            .progress_chars("█▇▆▅▄▃▂   "));
        file_pb.set_prefix(" Current File ");
        file_pb.enable_steady_tick(std::time::Duration::from_millis(1000)); // Reduced update frequency
        
        // Add a dedicated queue progress bar to show queue processing status and ETA
        let queue_pb = multi.add(ProgressBar::new(100));
        queue_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.magenta} {prefix:.bold.magenta}\n[{wide_bar:.magenta/blue}] {pos}% - {wide_msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
            .progress_chars("█▇▆▅▄▃▂   "));
        queue_pb.set_prefix(" Queue Progress ");
        queue_pb.enable_steady_tick(std::time::Duration::from_millis(1000));
        
        let stats_pb = multi.add(ProgressBar::new(100));
        stats_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.yellow} {prefix:.bold.yellow}\n{wide_msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]));
        stats_pb.set_prefix(" Processing ");
        stats_pb.enable_steady_tick(std::time::Duration::from_millis(1500)); // Reduced update frequency
        
        // Spawn a monitor thread to update progress stats
        let worker_stats = stats.clone();
        let worker_read = statements_read.clone();
        let worker_processed = statements_processed.clone();
        let worker_stats_pb = stats_pb.clone();
        let worker_eta_pb = eta_pb.clone();
        let worker_queue_pb = queue_pb.clone();
        let worker_start = start_time;
        let worker_total_size = total_size;
        let worker_total_bytes = total_bytes.clone();
        let worker_all_files_processed = all_files_processed.clone();
        
        let monitor_thread = thread::spawn(move || {
            let mut last_update = Instant::now();
            let mut last_processed = 0;
            let mut last_read = 0;
            let mut last_bytes = 0;
            
            loop {
                // No sleep here - removed to improve performance
                let now = Instant::now();
                let elapsed = now.duration_since(last_update);
                let read = worker_read.load(Ordering::Relaxed);
                let processed = worker_processed.load(Ordering::Relaxed);
                let bytes = worker_total_bytes.load(Ordering::Relaxed);
                
                // Check if all statements have been processed and all files have been read
                // This ensures we don't exit prematurely
                if read == processed && read > 0 && worker_all_files_processed.load(Ordering::Relaxed) == 1 {
                    // No delay needed - removed sleep to improve performance
                    break;
                }
                
                // Only update if significant time has passed or very significant progress has been made
                if elapsed.as_millis() >= 2500 || 
                   (read - last_read) > 50000 || 
                   (processed - last_processed) > 50000 || 
                   (bytes - last_bytes) > 1024 * 1024 * 20 { // 20MB change threshold
                    let stmts_per_sec = if elapsed.as_secs() > 0 {
                        (processed - last_processed) as f64 / elapsed.as_secs_f64()
                    } else { 0.0 };
                    
                    let overall_stmts_per_sec = if now.duration_since(worker_start).as_secs() > 0 {
                        processed as f64 / now.duration_since(worker_start).as_secs_f64()
                    } else { 0.0 };
                    
                    let successful = worker_stats.successful.load(Ordering::Relaxed);
                    let duplicates = worker_stats.duplicates.load(Ordering::Relaxed);
                    let errors = worker_stats.errors.load(Ordering::Relaxed);
                    
                    // Calculate queue size and progress
                    let queue_size = read - processed;
                    
                    worker_stats_pb.set_message(format!(
                        "{} read, {} processed | Queue: {} | {:.0} stmts/s (avg: {:.0}) | S: {} D: {} E: {}",
                        read,
                        processed,
                        queue_size,
                        stmts_per_sec,
                        overall_stmts_per_sec,
                        successful,
                        duplicates,
                        errors
                    ));
                    
                    // Update queue progress bar
                    if read > 0 {
                        // Calculate queue progress percentage
                        let queue_percent = if queue_size == 0 && read > 0 {
                            100 // Queue is empty but we've processed statements
                        } else if read > 0 {
                            ((processed as f64 / read as f64) * 100.0).round() as u64
                        } else {
                            0
                        };
                        
                        worker_queue_pb.set_position(queue_percent);
                        
                        // Calculate queue ETA based on processing rate
                        if stmts_per_sec > 0.0 && queue_size > 0 {
                            let queue_eta_seconds = queue_size as f64 / stmts_per_sec;
                            worker_queue_pb.set_message(format!(
                                "Remaining: {} items | Processing: {:.0} stmts/s | ETA: {}",
                                queue_size,
                                stmts_per_sec,
                                HumanDuration(std::time::Duration::from_secs_f64(queue_eta_seconds))
                            ));
                        } else if queue_size == 0 {
                            worker_queue_pb.set_message("Queue empty - All items processed");
                        } else {
                            worker_queue_pb.set_message(format!("Remaining: {} items | ETA: calculating...", queue_size));
                        }
                    }
                    
                    if bytes > 0 && worker_total_size > 0 {
                        let percent_complete = (bytes as f64 / worker_total_size as f64) * 100.0;
                        worker_eta_pb.set_position(percent_complete.round() as u64);
                        
                        let bytes_per_sec = if elapsed.as_secs() > 0 {
                            (bytes - last_bytes) as f64 / elapsed.as_secs_f64()
                        } else if now.duration_since(worker_start).as_secs() > 0 {
                            bytes as f64 / now.duration_since(worker_start).as_secs_f64()
                        } else { 0.0 };
                        
                        if bytes_per_sec > 0.0 {
                            let remaining_bytes = worker_total_size as f64 - bytes as f64;
                            let eta_seconds = remaining_bytes / bytes_per_sec;
                            worker_eta_pb.set_message(format!(
                                "Total ETA: {}",
                                HumanDuration(std::time::Duration::from_secs_f64(eta_seconds))
                            ));
                        } else {
                            worker_eta_pb.set_message("Total ETA: calculating...".to_string());
                        }
                    }
                    
                    last_update = now;
                    last_processed = processed;
                    last_read = read;
                    last_bytes = bytes;
                }
            }
            
            let processed = worker_processed.load(Ordering::Relaxed);
            let successful = worker_stats.successful.load(Ordering::Relaxed);
            let duplicates = worker_stats.duplicates.load(Ordering::Relaxed);
            let errors = worker_stats.errors.load(Ordering::Relaxed);
            
            worker_stats_pb.finish_with_message(format!(
                "Completed: {} statements | Success: {} | Dupes: {} | Errors: {}",
                processed,
                successful,
                duplicates,
                errors
            ));
            
            // Complete the queue progress bar
            worker_queue_pb.set_position(100);
            worker_queue_pb.finish_with_message(format!(
                "Queue processed - {} statements completed at {:.1} stmts/s average",
                processed,
                if now.duration_since(worker_start).as_secs() > 0 {
                    processed as f64 / now.duration_since(worker_start).as_secs_f64()
                } else { 0.0 }
            ));
            
            worker_eta_pb.set_position(100);
            worker_eta_pb.finish_with_message("Import complete");
        });
        
        // Spawn worker threads to process batches of SQL statements
        let mut worker_handles = Vec::with_capacity(self.config.parser_threads);
        for worker_id in 0..self.config.parser_threads {
            let worker_receiver = receiver.clone();
            let worker_stats = stats.clone();
            let worker_processed = statements_processed.clone();
            let worker_db_path = db_path.clone();
            let worker_logger = logger.clone();
            
            let handle = thread::spawn(move || {
                // Each worker opens its own DB connection
                let mut worker_conn = match Connection::open(&*worker_db_path) {
                    Ok(conn) => conn,
                    Err(e) => {
                        let error_msg = format!("Worker {}: Failed to open database: {}", worker_id, e);
                        let _ = worker_logger.log_error(&error_msg);
                        return;
                    }
                };
                
                if let Err(e) = worker_conn.execute_batch("
                    PRAGMA foreign_keys = OFF;
                    PRAGMA synchronous = OFF;
                    PRAGMA cache_size = -8000000;
                    PRAGMA temp_store = MEMORY;
                    PRAGMA busy_timeout = 30000;
                    PRAGMA mmap_size = 1073741824;
                    PRAGMA locking_mode = EXCLUSIVE;
                    PRAGMA journal_mode = MEMORY;
                    PRAGMA page_size = 4096;
                    PRAGMA journal_size_limit = 268435456;
                ") {
                    let error_msg = format!("Worker {}: Failed to configure database: {}", worker_id, e);
                    let _ = worker_logger.log_error(&error_msg);
                    return;
                }
                
                // Process batches until the channel is closed
                while let Ok(batch) = worker_receiver.recv() {
                    let batch_size = batch.len();
                    if batch_size == 0 { continue; }
                    
                    // Begin transaction only once per batch
                    let tx = match worker_conn.transaction() {
                        Ok(tx) => tx,
                        Err(e) => {
                            let error_msg = format!("Worker {}: Transaction error: {}", worker_id, e);
                            let _ = worker_logger.log_error(&error_msg);
                            // Increment error count and continue with next batch
                            worker_stats.errors.fetch_add(batch_size, Ordering::Relaxed);
                            worker_processed.fetch_add(batch_size, Ordering::Release);
                            continue;
                        }
                    };
                    
                    let mut successful = 0;
                    let mut duplicates = 0;
                    let mut errors = 0;
                    
                    // Process statements in batch more efficiently
                    for stmt_str in batch.iter() {
                        if stmt_str.is_empty() { continue; }
                        let stmt_str = stmt_str.trim();
                        if !stmt_str.is_empty() {
                            match tx.execute_batch(stmt_str) {
                                Ok(_) => { successful += 1; },
                                Err(e) => {
                                    let error_msg = e.to_string();
                                    if error_msg.contains("UNIQUE constraint failed") {
                                        duplicates += 1;
                                    } else {
                                        errors += 1;
                                        // Only log serious errors to reduce overhead
                                        if !error_msg.contains("syntax error") {
                                            let log_msg = format!(
                                                "Worker {}: Error executing statement: {}",
                                                worker_id,
                                                error_msg
                                            );
                                            let _ = worker_logger.log_error(&log_msg);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    // Commit transaction and update stats
                    if let Err(e) = tx.commit() {
                        let error_msg = format!("Worker {}: Commit error: {}", worker_id, e);
                        let _ = worker_logger.log_error(&error_msg);
                        errors += successful + duplicates; // Count all as errors if commit fails
                        successful = 0;
                        duplicates = 0;
                    }
                    
                    // Update stats atomically
                    if successful > 0 {
                        worker_stats.successful.fetch_add(successful, Ordering::Relaxed);
                    }
                    if duplicates > 0 {
                        worker_stats.duplicates.fetch_add(duplicates, Ordering::Relaxed);
                    }
                    if errors > 0 {
                        worker_stats.errors.fetch_add(errors, Ordering::Relaxed);
                    }
                    
                    // Mark batch as processed
                    worker_processed.fetch_add(batch_size, Ordering::Release);
                }
            });
            
            worker_handles.push(handle);
        }
        
        // Main file processing loop: read each SQL file and split it into statements.
        let mut total_stmts = 0usize;
        
        for (idx, sql_path) in sql_paths.iter().enumerate() {
            let _ = self.log_message(&format!("Processing file: {}", sql_path.display()));
            let file_size = std::fs::metadata(sql_path)?.len();
            file_pb.set_length(100);
            file_pb.set_position(0);
            
            let file_name = sql_path.file_name().unwrap_or_default().to_string_lossy();
            file_pb.set_message(format!("{} ({}/{}) - {}", 
                file_name,
                idx + 1, 
                sql_paths.len(),
                HumanBytes(file_size)
            ));
            
            let file = File::open(sql_path)?;
            let reader = BufReader::with_capacity(self.config.reader_buffer_size, file);
            let mut file_bytes = 0u64;
            let mut file_statements = 0usize;
            let file_start = Instant::now();
            let mut current_statement = String::with_capacity(4096);
            let mut batch = Vec::with_capacity(self.config.batch_size);
            
            let mut last_update = Instant::now();
            let mut last_percent = 0;
            
            for line in reader.lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(e) => {
                        let error_msg = format!("Error reading line: {}", e);
                        let _ = self.logger.log_error(&error_msg);
                        continue;
                    }
                };
                
                file_bytes += line.len() as u64;
                total_bytes.fetch_add(line.len(), Ordering::Relaxed);
                main_pb.set_position(total_bytes.load(Ordering::Relaxed) as u64);
                
                current_statement.push_str(&line);
                current_statement.push('\n');
                
                if line.trim().ends_with(';') {
                    if !current_statement.trim().is_empty() {
                        batch.push(current_statement.clone());
                        file_statements += 1;
                        current_statement.clear();
                        
                        if batch.len() >= self.config.batch_size {
                            // Removed unused queue_diff variable
                            // No sleep when queue is nearly full - removed to improve performance
                            statements_read.fetch_add(batch.len(), Ordering::Relaxed);
                            
                            if sender.send(batch).is_err() {
                                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "Worker channel closed unexpectedly"));
                            }
                            
                            // Create a new batch vector instead of clearing the old one
                            // This avoids waiting for the previous batch to be processed
                            batch = Vec::with_capacity(self.config.batch_size);
                        }
                    }
                }
                
                let file_percent = ((file_bytes as f64 / file_size as f64) * 100.0) as u64;
                let now = Instant::now();
                
                // Update progress bar less frequently to reduce overhead
                if (file_percent != last_percent && (file_percent % 10 == 0 || file_percent > 98)) || 
                   now.duration_since(last_update).as_millis() > 5000 {
                    // Only update with the actual bytes processed, not duplicating the count
                    main_pb.set_position(total_bytes.load(Ordering::Relaxed) as u64);
                    file_pb.set_position(file_percent);
                    
                    let elapsed = file_start.elapsed();
                    let mb_processed = file_bytes as f64 / (1024.0 * 1024.0);
                    let mb_per_sec = if elapsed.as_secs() > 0 { mb_processed / elapsed.as_secs_f64() } else { 0.0 };
                    
                    let file_eta = if mb_per_sec > 0.0 {
                        let remaining_mb = (file_size - file_bytes) as f64 / (1024.0 * 1024.0);
                        let seconds = remaining_mb / mb_per_sec;
                        format!("File ETA: {}", HumanDuration(std::time::Duration::from_secs_f64(seconds)))
                    } else {
                        "File ETA: calculating...".to_string()
                    };
                    
                    file_pb.set_message(format!("{} ({}/{}) - {:.2} MB at {:.2} MB/s - {}",
                        file_name,
                        idx + 1,
                        sql_paths.len(),
                        mb_processed,
                        mb_per_sec,
                        file_eta
                    ));
                    
                    last_update = now;
                    last_percent = file_percent;
                }
            }
            
            if !current_statement.trim().is_empty() {
                if !current_statement.trim().ends_with(';') {
                    current_statement.push(';');
                }
                batch.push(current_statement.clone());
                file_statements += 1;
            }
            
            // Process any remaining statements in the batch
            // If buffer size is smaller than batch size, process them immediately
            if !batch.is_empty() {
                statements_read.fetch_add(batch.len(), Ordering::Relaxed);
                if sender.send(batch).is_err() {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "Worker channel closed unexpectedly"));
                }
                
                // Don't wait for batch to complete - continue processing next file
                // This improves throughput by allowing parallel processing
            }
            
            total_stmts += file_statements;
            
            let file_duration = file_start.elapsed();
            let file_mb = file_bytes as f64 / (1024.0 * 1024.0);
            let file_mb_per_sec = if file_duration.as_secs() > 0 { file_mb / file_duration.as_secs_f64() } else { 0.0 };
            let file_stmts_per_sec = if file_duration.as_secs() > 0 { file_statements as f64 / file_duration.as_secs_f64() } else { 0.0 };
            
            file_pb.set_position(100);
            file_pb.set_message(format!("{} ({}/{}) - COMPLETED - {} statements ({:.1} stmts/s), {:.2} MB ({:.2} MB/s)",
                file_name,
                idx + 1,
                sql_paths.len(),
                file_statements,
                file_stmts_per_sec,
                file_mb,
                file_mb_per_sec
            ));
        }
        
        // Make sure any remaining items in the batch are sent to the queue
        // This ensures we don't leave any unprocessed items
        // Process single statements if buffer size is smaller than batch size
        
        // Wait for all statements to be processed before setting the flag
        // This prevents unprocessed items issue
        let mut wait_count = 0;
        while statements_read.load(Ordering::Relaxed) > statements_processed.load(Ordering::Relaxed) {
            // Use backoff strategy instead of constant polling
            if wait_count < 10 {
                // Initial fast polling with spin loop
                std::hint::spin_loop();
            } else if wait_count < 20 {
                // Medium backoff - yield to other threads
                std::thread::yield_now();
            } else {
                // Longer backoff - short sleep
                std::thread::sleep(std::time::Duration::from_micros(10));
            }
            
            wait_count += 1;
            
            // Log progress less frequently to reduce overhead
            if wait_count % 20 == 0 {
                let read = statements_read.load(Ordering::Relaxed);
                let processed = statements_processed.load(Ordering::Relaxed);
                let remaining = read - processed;
                let _ = self.log_message(&format!("Waiting for {} remaining items to be processed...", remaining));
            }
            
            // Timeout after reasonable time to prevent infinite wait
            if wait_count > 1000 {
                let _ = self.log_message("Timeout waiting for all items to be processed. Proceeding with cleanup.");
                break;
            }
            
            // Use spin loop instead of sleep to improve performance
            std::hint::spin_loop();
        }
        
        // Set the flag indicating all files have been processed
        all_files_processed.store(1, Ordering::Release);
        
        // Drop the sender to close the channel
        drop(sender);
        
        // Wait for all worker threads to complete
        for handle in worker_handles {
            if let Err(e) = handle.join() {
                eprintln!("Worker thread panicked: {:?}", e);
            }
        }
        
        // Wait for the monitor thread to complete
        monitor_thread.join().expect("Monitor thread panicked");
        
        // Run ANALYZE on the main connection
        let analyze_pb = multi.add(ProgressBar::new_spinner());
        analyze_pb.set_style(ProgressStyle::default_spinner()
            .template("{spinner:.blue} {prefix:.bold.blue} {wide_msg}")
            .unwrap()
            .tick_strings(&["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"]));
        analyze_pb.set_prefix(" Database ");
        analyze_pb.set_message("Running ANALYZE to update database statistics...");
        analyze_pb.enable_steady_tick(std::time::Duration::from_millis(80));
        
        conn.execute_batch("ANALYZE;")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        
        analyze_pb.finish_with_message("Database optimization completed");
        
        let duration = start_time.elapsed();
        let total_mb = total_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
        let mb_per_sec = if duration.as_secs() > 0 { total_mb / duration.as_secs_f64() } else { 0.0 };
        let stmts_per_sec = if duration.as_secs() > 0 { total_stmts as f64 / duration.as_secs_f64() } else { 0.0 };
        
        let successful = stats.successful.load(Ordering::Relaxed);
        let duplicates = stats.duplicates.load(Ordering::Relaxed);
        let errors = stats.errors.load(Ordering::Relaxed);
        
        let summary = format!(
            "Import completed in {} | Processed {:.2} MB at {:.2} MB/s | {} statements at {:.2} stmts/s | Success: {}, Dupes: {}, Errors: {}",
            HumanDuration(duration),
            total_mb,
            mb_per_sec,
            total_stmts,
            stmts_per_sec,
            successful,
            duplicates,
            errors
        );
        let _ = self.log_message(&summary);
        
        main_pb.finish_with_message(format!(
            "Processed {} statements ({:.1} stmts/s) | {:.2} MB ({:.2} MB/s)",
            total_stmts,
            stmts_per_sec,
            total_mb,
            mb_per_sec
        ));
        
        println!("\nImport completed in {}", HumanDuration(duration));
        println!("Processed {:.2} MB at {:.2} MB/s", total_mb, mb_per_sec);
        println!("Processed {} statements at {:.2} stmts/s", total_stmts, stmts_per_sec);
        println!("Stats: {} successful, {} duplicates, {} errors", successful, duplicates, errors);
        println!("Log file: {}", self.logger.log_path);
        
        Ok(())
    }
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <database_path> <sql_file_path> [options]", args[0]);
        eprintln!("Options:");
        eprintln!("  --batch-size=N      Number of statements per transaction (default: 25000)");
        eprintln!("  --buffer-size=N     Buffer size in MB (default: 32)");
        eprintln!("  --threads=N         Number of parser threads (default: num_cpus)");
        eprintln!("  --queue-capacity=N  Maximum statements in queue (default: 100000)");
        eprintln!("  --no-mmap           Disable memory mapping");
        return Ok(());
    }
    
    let db_path = PathBuf::from(&args[1]);
    let sql_paths: Vec<PathBuf> = args[2..].iter()
        .filter(|arg| !arg.starts_with("--"))
        .map(PathBuf::from)
        .collect();
    
    let mut config = Config::default();
    
    for arg in &args[3..] {
        if let Some(val) = arg.strip_prefix("--batch-size=") {
            config.batch_size = val.parse().unwrap_or(config.batch_size);
        } else if let Some(val) = arg.strip_prefix("--buffer-size=") {
            let mb = val.parse().unwrap_or(32);
            config.reader_buffer_size = mb * 1024 * 1024;
        } else if let Some(val) = arg.strip_prefix("--threads=") {
            config.parser_threads = val.parse().unwrap_or(config.parser_threads);
        } else if let Some(val) = arg.strip_prefix("--queue-capacity=") {
            config.queue_capacity = val.parse().unwrap_or(config.queue_capacity);
        } else if arg == "--no-mmap" {
            config.use_mmap = false;
        }
    }
    
    let importer = SQLiteImporter::new(config);
    importer.import_file(db_path, sql_paths)
}
