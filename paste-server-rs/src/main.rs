use std::{
    io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Context, anyhow};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{Days, Local};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    PgPool, Pool, Postgres,
    types::{
        Uuid,
        time::{OffsetDateTime, PrimitiveDateTime},
    },
};
use tokio::{io::AsyncWriteExt, task::JoinHandle, time::sleep};
use tower_http::services::ServeDir;
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

// learned from https://github.com/tokio-rs/axum/blob/main/examples/anyhow-error-response/src/main.rs
pub struct AnyhowError(anyhow::Error);

impl IntoResponse for AnyhowError {
    fn into_response(self) -> Response {
        error!("Returning internal server error for {}", self.0);

        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json::from(Message {
                success: false,
                msg: self.0.to_string().into(),
            }),
        )
            .into_response()
    }
}

impl<E> From<E> for AnyhowError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[derive(Debug, Clone)]
struct AppState {
    db: Arc<Pool<Postgres>>,
    content_dir: PathBuf,
    local_url: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    success: bool,
    msg: Value,
}

#[derive(Debug)]
struct PasteResponse {
    id: Uuid,
    title: Option<String>,
    expiration: PrimitiveDateTime,
    language: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GetPasteMessage {
    id: String,
    title: Option<String>,
    expiration: i64,
    language: String,
    attachments: Vec<String>,
    content_path: String,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Attachment {
    id: i32,
    filename: String,
    paste_id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
struct PostPasteMessage {
    id: String,
    language: String,
    expiration: i64,
    content_path: String,
    attachments: Vec<String>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    // initialize tracing
    let env_log = EnvFilter::try_from_default_env();

    if let Ok(filter) = env_log {
        tracing_subscriber::registry()
            .with(fmt::layer().with_filter(filter))
            .init();
    } else {
        tracing_subscriber::registry().with(fmt::layer()).init();
    }

    let local_url = std::env::var("PASTE_URL").expect("PASTE_URL is not set");
    let pg = std::env::var("PASTE_DB").expect("PASTE_DB is not set.");
    let content_dir =
        PathBuf::from(std::env::var("PASTE_FILE_DIR").expect("PASTE_FILE_DIR is not set."));

    let db = PgPool::connect(&pg)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect database: {pg}: {e}"));

    let db = Arc::new(db);

    let serve_dir = ServeDir::new(&*content_dir);

    let router = Router::new()
        .fallback_service(serve_dir)
        .route("/{id}", get(get_paste))
        .route("/", post(post_paste))
        .with_state(AppState {
            db: db.clone(),
            content_dir: content_dir.to_path_buf(),
            local_url: local_url.clone(),
        })
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024));

    let listener = tokio::net::TcpListener::bind(&local_url).await.unwrap();

    tokio::try_join!(
        axum::serve(listener, router),
        clean_expiration(&db, &content_dir)
    )
    .expect("A task failed");
}

async fn clean_expiration(db: &Pool<Postgres>, dir: &std::path::Path) -> io::Result<()> {
    loop {
        let paste = sqlx::query_as!(
            PasteResponse,
            "SELECT id, title, expiration, language FROM paste"
        )
        .fetch_all(db)
        .await
        .map_err(io::Error::other)?;

        for i in paste {
            if i.expiration.as_utc() < SystemTime::now() {
                info!("Deleting id: {} from db: {i:?}", i.id);

                sqlx::query!("DELETE FROM paste WHERE id = $1", i.id)
                    .execute(db)
                    .await
                    .map_err(io::Error::other)?;

                sqlx::query!("DELETE FROM attachments WHERE paste_id = $1", i.id)
                    .execute(db)
                    .await
                    .map_err(io::Error::other)?;

                info!("Deleting id: {} dir", i.id);
                tokio::fs::remove_dir_all(dir.join(i.id.to_string())).await?;
            }
        }

        sleep(Duration::from_secs(1800)).await;
    }
}

async fn post_paste(
    State(AppState {
        db,
        content_dir,
        local_url,
        ..
    }): State<AppState>,
    mut form: Multipart,
) -> Result<impl IntoResponse, AnyhowError> {
    let uuid = Uuid::new_v4();
    let mut content = None;
    let mut language = "text".to_string();
    let mut expiration = Local::now()
        .checked_add_days(Days::new(7))
        .context("Failed to calculate date")?
        .timestamp();

    let mut title = "".to_string();
    let mut files = vec![];

    while let Some(field) = form.next_field().await? {
        match field.name() {
            Some("c") | Some("content") => {
                content = Some(field.bytes().await?);
            }
            Some("l") | Some("language") => {
                language = field.text().await?;
            }
            Some("e") | Some("expiration") => {
                expiration = field.text().await?.parse()?;
            }
            Some("f") | Some("file") => {
                files.push((
                    field.file_name().map(|x| x.to_string()),
                    field.bytes().await?,
                ));
            }
            Some("t") | Some("title") => {
                title = field.text().await?;
            }
            Some(x) => return Err(anyhow!("Unsupport field {x}").into()),
            None => {}
        }
    }

    if files.is_empty() && content.is_none() {
        return Err(anyhow!("Upload data is empty").into());
    }

    let mut write_file_tasks = vec![];

    let dir = content_dir.join(uuid.to_string());
    tokio::fs::create_dir_all(&dir).await?;

    let now = SystemTime::now();
    let path = dir.join("content");
    let task: JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
        let mut content_file = tokio::fs::File::create(path).await?;
        if let Some(b) = content {
            content_file.write_all(&b).await?;
        }
        Ok(())
    });

    write_file_tasks.push(task);

    let mut files_name = vec![];

    for (i, (file_name, file)) in files.into_iter().enumerate() {
        let file_name = file_name.unwrap_or_else(|| i.to_string());
        let file_name_clone = file_name.clone();

        let task: JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
            let mut f = tokio::fs::File::create(file_name_clone).await?;
            f.write_all(&file).await?;
            Ok(())
        });

        write_file_tasks.push(task);
        files_name.push(file_name);
    }

    let len = write_file_tasks.len();
    for task in write_file_tasks {
        task.await??;
    }

    debug!(
        "write {len} file use: {:?} micros",
        now.elapsed().map(|e| e.as_micros())
    );

    let time = OffsetDateTime::from_unix_timestamp(expiration)?;
    let time = PrimitiveDateTime::new(time.date(), time.time());

    sqlx::query!(
        r#"INSERT INTO paste VALUES ($1, $2, $3, $4)"#,
        uuid,
        title,
        time,
        language,
    )
    .execute(&*db)
    .await?;

    for name in &files_name {
        let id = sqlx::query_scalar!(
            r#"INSERT INTO attachments (filename, paste_id) VALUES ($1, $2) RETURNING id"#,
            name,
            uuid
        )
        .fetch_one(&*db)
        .await?;

        debug!("attachments id: {id}");
    }

    let dir = Url::parse(&format!("http://{local_url}"))
        .unwrap()
        .join(&format!("{uuid}/"))?;

    let content_path = dir.join("content")?;

    Ok(Json::from(Message {
        success: true,
        msg: serde_json::to_value(PostPasteMessage {
            id: uuid.to_string(),
            language,
            expiration,
            content_path: content_path.to_string(),
            attachments: files_name
                .into_iter()
                .flat_map(|x| dir.join(&x))
                .map(|x| x.to_string())
                .collect::<Vec<_>>(),
        })?,
    }))
}

async fn get_paste(
    State(AppState { db, local_url, .. }): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, AnyhowError> {
    let uuid = Uuid::parse_str(&id)?;

    let PasteResponse {
        id,
        title,
        expiration,
        language,
    } = sqlx::query_as!(
        PasteResponse,
        "SELECT id, title, expiration, language FROM paste WHERE id = $1",
        uuid
    )
    .fetch_one(&*db)
    .await?;

    let attachments = sqlx::query_as!(
        Attachment,
        "SELECT id, filename, paste_id FROM attachments WHERE paste_id = $1",
        id
    )
    .fetch_all(&*db)
    .await?;

    let dir = Url::parse(&format!("http://{local_url}"))
        .unwrap()
        .join(&format!("{uuid}/"))?;

    let content_path = dir.join("content")?.to_string();

    Ok(Json::from(Message {
        success: true,
        msg: serde_json::to_value(GetPasteMessage {
            id: id.to_string(),
            title,
            expiration: expiration.assume_utc().unix_timestamp(),
            language,
            content_path,
            attachments: attachments
                .iter()
                .flat_map(|x| dir.join(&x.filename))
                .map(|x| x.to_string())
                .collect::<Vec<_>>(),
        })?,
    }))
}
