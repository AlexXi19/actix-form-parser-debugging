use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use actix_multipart::{Field, Multipart};
use actix_web::{dev, web, App, FromRequest, HttpServer};
use anyhow::Result;
use futures::StreamExt;
use rand::{distributions::Alphanumeric, seq::SliceRandom, Rng};
use reqwest::multipart;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JSON_Value;
use tokio::time::sleep;

const PORT: u16 = 8080;
const CONCURRENCY: u16 = 40;

#[tokio::main]
async fn main() -> Result<()> {
    let mut handles = Vec::new();
    for i in 0..CONCURRENCY {
        let handle = tokio::spawn(async move {
            let worker = i;
            sleep(Duration::from_millis(500)).await;
            println!("Starting worker {}", worker);
            let mut counter = 0;
            loop {
                let handle = send_request();

                handle.await;

                counter = counter + 1;

                if counter % 500 == 0 {
                    println!("Worker {} send {} requests!", worker, counter);
                }
            }
        });

        handles.push(handle);
    }

    HttpServer::new(|| {
        App::new()
            .service(web::resource("/").route(web::get().to(hello_world)))
            .service(web::resource("/v2").route(web::post().to(handle_request)))
    })
    .bind(("127.0.0.1", PORT))?
    .workers(4)
    .run()
    .await
    .unwrap();

    for handle in handles {
        handle.await.unwrap();
    }

    Ok(())
}

async fn hello_world() -> std::io::Result<web::Json<serde_json::Value>> {
    Ok(web::Json(serde_json::json!({"hello": "world"}).into()))
}

async fn handle_request(
    req_ctx: actix_web::HttpRequest,
    payload: web::Payload,
) -> std::io::Result<web::Json<serde_json::Value>> {
    let multipart_form_res = Multipart::from_request(
        &req_ctx,
        &mut dev::Payload::Stream {
            payload: payload.boxed_local(),
        },
    )
    .await;

    let multipart_form = match multipart_form_res {
        Ok(form) => form,
        Err(err) => {
            println!("ERROR RESING!!");
            return Ok(web::Json(
                serde_json::json!({
                    "error": format!("Error parsing multipart form res: {}", err)
                })
                .into(),
            ));
        }
    };

    match try_parse(multipart_form, req_ctx).await {
        Ok(_) => (),
        Err(err) => {
            println!("ERROR PARSING!!!!!!!!");
            println!("{:?}", err);
            return Ok(web::Json(
                serde_json::json!({
                    "error": format!("Error try parsing multipart form: {}", err)
                })
                .into(),
            ));
        }
    }

    Ok(web::Json(serde_json::json!({"status": "ok"}).into()))
}

async fn send_request() {
    let client = Arc::new(reqwest::Client::new());
    let form = create_random_multipart_form();
    let _ = client
        .post(format!("http://localhost:{}/v2", PORT))
        .multipart(form)
        .send()
        .await
        .unwrap();
}

fn create_random_multipart_form() -> multipart::Form {
    let mut form = multipart::Form::new();
    let num_args = generate_random_number(0, 20);
    let mut map: HashMap<String, serde_json::Value> = HashMap::new();

    // Args
    for i in 0..num_args {
        let random = rand::thread_rng().gen::<f64>();
        let arg = if random > 0.3 {
            let arg = &generate_random_number(0, 1000000);
            serde_json::to_string(arg).unwrap()
        } else if random < 0.6 {
            let rng = &mut rand::thread_rng();
            let depth = generate_random_number(0, 100);
            let arg = generate_random_json(depth as usize, rng);
            serde_json::to_string(&arg).unwrap()
        } else {
            let arg = &generate_random_string(0, 10000000);
            serde_json::to_string(arg).unwrap()
        };

        let arg_name = format!("arg{}", i);
        map.insert(arg_name, serde_json::json!(arg));
    }

    // Timeout and priority
    let random = rand::thread_rng().gen::<f64>();
    if random > 0.5 {
        let timeout = generate_random_number(0, 10000);
        map.insert("timeout_ms".to_string(), serde_json::json!(timeout));
    }
    let random = rand::thread_rng().gen::<f64>();
    if random > 0.5 {
        let priority = generate_random_number(0, 100);
        map.insert("priority".to_string(), serde_json::json!(priority));
    }

    shuffle_hashmap(&mut map);

    for (key, value) in map {
        form = form.text(key, value.to_string());
    }

    form
}

fn shuffle_hashmap(map: &mut HashMap<String, serde_json::Value>) {
    let mut keys: Vec<_> = map.keys().cloned().collect();
    let mut rng = rand::thread_rng();
    keys.shuffle(&mut rng);

    let mut new_map = HashMap::new();
    for key in keys {
        if let Some(value) = map.remove(&key) {
            new_map.insert(key, value);
        }
    }

    *map = new_map;
}

fn maybe_return_value<T: Clone>(percentage: f64, value: T) -> Option<T> {
    if percentage < 0.0 || percentage > 1.0 {
        panic!("Percentage must be between 0.0 and 1.0 inclusive");
    }

    let random_value = rand::thread_rng().gen::<f64>();

    if random_value < percentage {
        Some(value)
    } else {
        None
    }
}

fn generate_random_number(min: i32, max: i32) -> i32 {
    let mut rng = rand::thread_rng();
    rng.gen_range(min..max)
}

fn generate_random_json(depth: usize, rng: &mut impl Rng) -> serde_json::Value {
    if depth == 0 {
        generate_random_leaf(rng)
    } else {
        let mut json_map = serde_json::Map::new();
        let num_fields = rng.gen_range(1..=5);
        for _ in 0..num_fields {
            let key = rng.gen::<usize>().to_string();
            let value = generate_random_json(depth - 1, rng);
            json_map.insert(key, value);
        }
        serde_json::Value::Object(json_map)
    }
}

fn generate_random_leaf(rng: &mut impl Rng) -> serde_json::Value {
    match rng.gen_range(0..=3) {
        0 => serde_json::Value::Null,
        1 => serde_json::Value::Bool(rng.gen::<bool>()),
        2 => serde_json::Value::Number(rng.gen::<i32>().into()),
        3 => serde_json::Value::String(rng.gen::<usize>().to_string()),
        _ => unreachable!(),
    }
}

fn generate_random_string(min_length: usize, max_length: usize) -> String {
    let mut rng = rand::thread_rng();
    let length = rng.gen_range(min_length..max_length);
    let random_string: String = rng
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect();
    random_string
}

pub async fn try_parse(mut form_data: Multipart, _: actix_web::HttpRequest) -> Result<()> {
    let mut args = HashMap::new();
    let mut priority_string: Option<String> = None;
    let mut timeout_string: Option<String> = None;
    let mut media_type: Option<mime::Mime> = None;

    // stream in multi part form data and pull out fields
    while let Some(item) = form_data.next().await {
        let field = item.unwrap();

        let name = field.name().to_string();
        match name.as_str() {
            key if key.starts_with("arg") => {
                // register media type if it exists and it is not octet stream
                let content_type = field.content_type();
                if let Some(content_type) = content_type {
                    if content_type != &mime::APPLICATION_OCTET_STREAM {
                        media_type = Some(content_type.clone());
                        let _ = media_type;
                    }
                }
                // parse arg
                let arg_result = parse_script_arg(field).await;
                args.insert(key.to_string(), arg_result);
            }
            "timeout_ms" => {
                timeout_string = Some(read_to_string(field).await?);
            }
            "priority" => {
                priority_string = Some(read_to_string(field).await?);
            }
            _ => {
                continue;
            }
        }
    }

    let default_timeout = 10000;
    let default_priority = 0;

    // try and parse timeout and priority
    let _ = match timeout_string {
        None => default_timeout,
        Some(s) => match s.trim().parse::<u64>() {
            Ok(t) => t,
            Err(e) => {
                anyhow::bail!(
                    "Failed to Parse timeout from \"{s}\": {e}. Using Default {default_timeout} instead"
                )
            }
        },
    };

    let _ = match priority_string {
        None => default_priority,
        Some(s) => match s.trim().parse::<u32>() {
            Ok(p) => p,
            Err(e) => {
                anyhow::bail!(
                    "WE GOTTEM! WE GOTTEM! Failed to Parse priority from \"{s}\": {e}. Using Default {default_timeout} instead"
                )
            }
        },
    };

    // unwrapping arg parsing errors here so that logger can have fields set even on failure
    let mut parsed_args = HashMap::new();
    for (name, arg) in args.into_iter() {
        parsed_args.insert(name, arg?);
    }

    Ok(())
}
// reads a multipart field into a String
async fn read_to_string(mut field: Field) -> Result<String> {
    let mut argument_bytes: Vec<u8> = vec![];
    while let Some(chunk) = field.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                anyhow::bail!(
                    "[READ TO STRING] WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! {}",
                    e
                );
            }
        };
        argument_bytes.extend_from_slice(&chunk);
    }
    let decoded_str = String::from_utf8(argument_bytes)?;
    Ok(decoded_str)
}

// Reads a multipart field into bytes
async fn read_to_bytes(mut field: Field) -> Result<Vec<u8>> {
    let mut argument_bytes: Vec<u8> = vec![];
    while let Some(chunk) = field.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                anyhow::bail!(
                    "[READ TO BYTES] WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! {}",
                    e
                );
            }
        };
        argument_bytes.extend_from_slice(&chunk);
    }
    Ok(argument_bytes)
}

async fn parse_script_arg(field: Field) -> Result<ScriptArg> {
    // FIXME: read file header from field instead of checking for content type
    let content_type = field.content_type();

    if let Some(content_type) = content_type {
        // parse file if it has content type
        match (content_type.type_(), content_type.subtype()) {
            (mime::IMAGE | mime::AUDIO | mime::VIDEO, _) => {
                anyhow::bail!("Im never passing in a file")
            }
            (mime::APPLICATION, mime::JSON) => (), // do nothing so we go to the json parsing below
            (mime::APPLICATION, mime::OCTET_STREAM) => (), // do nothing so we go to the json/file parsing below
            (_, _) => anyhow::bail!("Should never reach here, always an octet stream"),
        }
    }
    // Attempt to parse as JSON
    let decoded_bytes: Vec<u8> = read_to_bytes(field).await?;
    match serde_json::from_slice(&decoded_bytes) {
        Ok(val) => Ok(ScriptArg::Value(val)),
        // Error case originally will try to parse it to a file
        Err(_) => anyhow::bail!("WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! WE GOTTEM! ")
    }
}

// arguments to the Lua Script
#[derive(Serialize, Deserialize, Debug)]
pub enum ScriptArg {
    File(FileObject),
    Value(JSON_Value),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileObject {
    pub file_path: String,
    pub file_type: String,
    pub file_size: u64,
    pub media_type: String,
    pub duration: f64,
    pub fps: f64,
    pub height: u32,
    pub width: u32,
    pub timestamp: f64,
}
