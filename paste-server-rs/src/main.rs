use std::{borrow::Cow, path::PathBuf, sync::Arc};

use anyhow::{Context, anyhow};
use axum::{
    Json, Router,
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{Days, Local};
use serde::Serialize;
use sqlx::{
    PgPool, Pool, Postgres,
    types::{
        Uuid,
        time::{OffsetDateTime, PrimitiveDateTime},
    },
};
use tokio::io::AsyncWriteExt;
use tower_http::services::ServeDir;
use tracing::{debug, error};
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

// learned from https://github.com/tokio-rs/axum/blob/main/examples/anyhow-error-response/src/main.rs
pub struct AnyhowError(anyhow::Error);

impl IntoResponse for AnyhowError {
    fn into_response(self) -> Response {
        error!("Returning internal server error for {}", self.0);
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self.0)).into_response()
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
        .expect(&format!("Failed to connect database: {pg}"));

    let serve_dir = ServeDir::new(&content_dir);

    let router = Router::new()
        .fallback_service(serve_dir)
        .route("/{id}", get(get_paste))
        .route("/", post(post_paste))
        .with_state(AppState {
            db: Arc::new(db),
            content_dir,
            local_url: local_url.clone(),
        });

    let listener = tokio::net::TcpListener::bind(&local_url).await.unwrap();
    axum::serve(listener, router).await.unwrap();
}

struct PasteResponse {
    id: Uuid,
    title: Option<String>,
    expiration: PrimitiveDateTime,
    language: String,
}

#[derive(Debug, Serialize)]
struct PasteResult {
    id: String,
    title: Option<String>,
    expiration: i64,
    language: String,
    attachments: Vec<String>,
    content_path: String,
}

#[derive(Debug)]
struct Attachment {
    id: i32,
    filename: String,
    paste_id: Uuid,
}

#[derive(Debug, Serialize)]
struct PostPaste {
    id: String,
    language: String,
    expiration: i64,
    content_path: String,
    attachments: Vec<String>,
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
    let mut language = Cow::Borrowed("text");
    let mut expiration = Local::now()
        .checked_add_days(Days::new(7))
        .context("Failed to calc days")?
        .timestamp();
    let mut title = "".to_string();
    let mut f = vec![];

    while let Some(field) = form.next_field().await? {
        match field.name() {
            Some("c") => {
                content = Some(field.bytes().await?);
            }
            Some("l") => {
                language = Cow::Owned(field.text().await?);
            }
            Some("e") => {
                expiration = field.text().await?.parse()?;
            }
            Some("f") => {
                f.push(field.bytes().await?);
            }
            Some("t") => {
                title = field.text().await?;
            }
            Some(x) => return Err(anyhow!("Unsupport field {x}").into()),
            None => {}
        }
    }

    let dir = content_dir.join(uuid.to_string());
    tokio::fs::create_dir_all(&dir).await?;

    let path = dir.join("content");
    let mut content_file = tokio::fs::File::create(&path).await?;
    if let Some(b) = content {
        content_file.write_all(&b).await?;
    }

    for (i, c) in f.iter().enumerate() {
        let mut f = tokio::fs::File::create(i.to_string()).await?;
        f.write_all(&c).await?;
    }

    let time = OffsetDateTime::from_unix_timestamp(expiration)?;
    let time = PrimitiveDateTime::new(time.date(), time.time());

    sqlx::query!(
        r#"INSERT INTO paste VALUES ($1, $2, $3, $4)"#,
        uuid,
        title,
        time,
        language.to_string(),
    )
    .execute(&*db)
    .await?;

    if !f.is_empty() {
        for (index, _) in f.iter().enumerate() {
            let id = sqlx::query_scalar!(
                r#"INSERT INTO attachments (filename, paste_id) VALUES ($1, $2) RETURNING id"#,
                index.to_string(),
                uuid
            )
            .fetch_one(&*db)
            .await?;

            debug!("attachments id: {id}");
        }
    }

    let dir = Url::parse(&format!("http://{local_url}"))
        .unwrap()
        .join(&format!("{uuid}/"))?;

    let content_path = dir.join("content")?;

    Ok(Json::from(PostPaste {
        id: uuid.to_string(),
        language: language.to_string(),
        expiration,
        content_path: content_path.to_string(),
        attachments: (0..f.len())
            .into_iter()
            .map(|x| dir.join(&x.to_string()))
            .flatten()
            .map(|x| x.to_string())
            .collect::<Vec<_>>(),
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

    let a = sqlx::query_as!(
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

    Ok(Json::from(PasteResult {
        id: id.to_string(),
        title,
        expiration: expiration.assume_utc().unix_timestamp(),
        language,
        content_path,
        attachments: a
            .iter()
            .map(|x| dir.join(&x.filename))
            .flatten()
            .map(|x| x.to_string())
            .collect::<Vec<_>>(),
    }))
}
