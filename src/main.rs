use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
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
}

impl Store {
    fn new() -> Self {
        Store {
            user_projects: Arc::new(RwLock::new(ProjectMap::new())),
        }
    }
}


fn handle_client_request(request : hyper::Request<hyper::body::Incoming>, store : &Store) -> hyper::Response<http_body_util::Full<hyper::body::Bytes>>
{
    let client_token = env::var("CLIENT_TOKEN").unwrap();
    let expected = format!("Bearer {client_token}");
    let auth_header = request.headers().get("Authorization");

    if auth_header.is_none() || ! expected.eq(auth_header.unwrap()) {
        return hyper::Response::builder()
            .status(hyper::StatusCode::UNAUTHORIZED)
            .body(http_body_util::Full::new(hyper::body::Bytes::from(
                String::from("{}"),
            )))
            .unwrap();
    }

    let uri = request.uri().to_string();
    log::info!("Handling request {:?}", uri);
    let user_projects = store.user_projects.read().unwrap();

    match &user_projects.get(&uri) {
        Some(projects) => {
            let response_body = serde_json::to_string(&projects).unwrap();
            hyper::Response::new(http_body_util::Full::new(hyper::body::Bytes::from(
                response_body,
            )))
        }
        None => hyper::Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .body(http_body_util::Full::new(hyper::body::Bytes::from(
                String::from("{}"),
            )))
            .unwrap(),
    }
}


async fn update_projects(store: Store) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    log::info!("Refreshing projects list.");
    let token = env::var("API_TOKEN").unwrap();
    let api_url = env::var("API_URL").unwrap();
    let client = reqwest::Client::new();
    let resp: HpcApi = client
        .get(api_url)
        .bearer_auth(token)
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

    log::debug!("Waiting for write lock");
    let start_locking = Instant::now();
    {
        let mut user_projects = store.user_projects.write().unwrap();
        *user_projects = update;
    }
    log::debug!("Write finished. Elapsed: {:?}", start_locking.elapsed());

    log::debug!("Refreshed projects list. Elapsed: {:?}", start.elapsed());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let _ = env::var("CLIENT_TOKEN").expect("Environment variable `CLIENT_TOKEN` should exist");

    let _ = env::var("API_TOKEN").expect("Environment variable `API_TOKEN` should exist");
    
    let _ = env::var("API_URL").expect("Environment variable `API_URL` should exist");

    let cache_timeout = Duration::new(
        env::var("CACHE_TIMEOUT")
            .expect("Environment variable `CACHE_TIMEOUT` should exist")
            .parse()
            .expect("Environment variable `CACHE_TIMEMOUT` should contain timeout in seconds"),
        0,
    );

    let store = Store::new();
    let mut last_update = Instant::now();


    {
        let writer_lock = store.clone();
        tokio::task::spawn(async move {
            update_projects(writer_lock).await.unwrap();
        });
    }

    let addr = SocketAddr::from(([127, 0, 0, 1], 8282));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;

        // Check if we should refresh the projects cache
        if last_update.elapsed() > cache_timeout {
            last_update = Instant::now();
            let writer_lock = store.clone();
            tokio::task::spawn(async move {
                update_projects(writer_lock).await.unwrap();
            });
        }

        let lock = store.clone();
        let service = hyper::service::service_fn(move |request| {
            let response = handle_client_request(request, &lock);
            async move { Ok::<_, hyper::Error>(response) }
        });

        let io = hyper_util::rt::TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
