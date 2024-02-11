use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

type ProjectMap = HashMap<String, Vec<Project>>;

#[derive(Debug, serde::Deserialize)]
struct HpcApiUser {
    project_username: String,
    username: String,
}

#[derive(Debug, serde::Deserialize)]
struct HpcApiProject {
    id: String,
    name: String,
    users: Vec<HpcApiUser>,
}

#[derive(Debug, serde::Deserialize)]
struct HpcApiProjects {
    projects: Vec<HpcApiProject>,
}

#[derive(Debug, serde::Deserialize)]
struct HpcApi {
    result: HpcApiProjects,
}

#[derive(Debug, serde::Serialize)]
struct Project {
    id: String,
    name: String,
    user: String,
}

#[derive(Debug, Clone)]
struct Store {
    user_projects: Arc<RwLock<ProjectMap>>,
    last_update: Arc<RwLock<Instant>>,
    api_token: String,
    api_url: String,
    auth_header: String,
    cache_timeout: Duration,
}

impl Store {
    fn new() -> Self {
        Store {
            user_projects: Arc::new(RwLock::new(ProjectMap::new())),
            last_update: Arc::new(RwLock::new(Instant::now())),
            api_token: env::var("API_TOKEN")
                .expect("Environment variable `API_TOKEN` should exist"),
            api_url: env::var("API_URL").expect("Environment variable `API_URL` should exist"),
            auth_header: format!(
                "Bearer {}",
                env::var("CLIENT_TOKEN").expect("Environment variable `CLIENT_TOKEN` should exist")
            ),
            cache_timeout: Duration::new(
                env::var("CACHE_TIMEOUT")
                    .expect("Environment variable `CACHE_TIMEOUT` should exist")
                    .parse()
                    .expect(
                        "Environment variable `CACHE_TIMEMOUT` should contain timeout in seconds",
                    ),
                0,
            ),
        }
    }
}

async fn handle_client_request(
    request: hyper::Request<hyper::body::Incoming>,
    store: &Store,
) -> hyper::Response<http_body_util::Full<hyper::body::Bytes>> {
    let auth_header = request.headers().get("Authorization");

    if auth_header.is_none() || !store.auth_header.eq(auth_header.unwrap()) {
        return hyper::Response::builder()
            .status(hyper::StatusCode::UNAUTHORIZED)
            .body(http_body_util::Full::new(hyper::body::Bytes::from(
                String::from("{}"),
            )))
            .unwrap();
    }

    let uri = request.uri().to_string();
    log::debug!("Handling request {:?}", uri);

    let elapsed = store.last_update.read().unwrap().elapsed();
    if elapsed > store.cache_timeout {
        *store.last_update.write().unwrap() = Instant::now();
        if elapsed > store.cache_timeout * 10 {
            // We await in case the cache is severely out of date.
            // The disadvantage is that the client needs to wait a bit.
            if let Err(err) = update_projects(store.clone()).await {
                log::warn!("Could not update the cache: {}", err);
            }
        } else {
            // Cache is not that old, refresh in the background
            let task_store = store.clone();
            tokio::task::spawn(async move {
                if let Err(err) = update_projects(task_store).await {
                    log::warn!("Could not update the cache: {}", err);
                }
            });
        }
    }

    let user_projects = store.user_projects.read().unwrap();
    match &user_projects.get(&uri) {
        Some(projects) => hyper::Response::builder()
            .body(http_body_util::Full::new(hyper::body::Bytes::from(
                String::from(serde_json::to_string(&projects).unwrap()),
            )))
            .unwrap(),
        None => hyper::Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .body(http_body_util::Full::new(hyper::body::Bytes::from(
                String::from("{}"),
            )))
            .unwrap(),
    }
}

async fn update_projects(store: Store) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    log::info!("Refreshing projects list.");
    let client = reqwest::Client::new();
    let resp: HpcApi = client
        .get(&store.api_url)
        .bearer_auth(&store.api_token)
        .send()
        .await?
        .json()
        .await?;

    let capacity: usize;
    {
        let user_projects = store.user_projects.read().unwrap();
        capacity = user_projects.capacity();
    }
    let mut update = ProjectMap::with_capacity(capacity);

    for project in resp.result.projects {
        for user in project.users {
            let username = user.username;
            let key = format!("/{username}");

            if !update.contains_key(&key) {
                update.insert(key.clone(), Vec::<Project>::new());
            }

            let project = Project {
                id: String::from(&project.id),
                name: String::from(&project.name),
                user: user.project_username,
            };

            update.get_mut(&key).unwrap().push(project);
        }
    }
    *store.user_projects.write().unwrap() = update;
    log::debug!("Refreshed projects list. Elapsed: {:?}", start.elapsed());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    let bind_addr: SocketAddr = env::var("BIND_ADDR")
        .expect("Environment variable `BIND_ADDR` should exist")
        .parse()
        .expect("Environment variable `BIND_ADDR` should contain valid socket address");

    let store = Store::new();

    if let Err(err) = update_projects(store.clone()).await {
        log::error!("Could not fetch project list from API: {}", err);
        process::exit(1);
    }

    log::debug!("Listening on {:?}", bind_addr);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        let io = hyper_util::rt::TokioIo::new(stream);

        let service_store = store.clone();
        let service = hyper::service::service_fn(move |request| {
            let handler_store = service_store.clone();
            async move { Ok::<_, hyper::Error>(handle_client_request(request, &handler_store).await) }
        });

        tokio::task::spawn(async move {
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                log::error!("Error serving connection from {addr}: {:?}", err);
            }
        });
    }
}
