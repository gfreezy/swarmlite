use super::*;
use swarmlite_core::model::{ClusterState, GatewayClusterStatusResponse, ServicePortKey};

pub(super) fn routes() -> Router<Arc<UiState>> {
    Router::new()
        .route("/api/routes", get(routes_view))
        .route("/api/gateways", get(gateways))
        .route("/api/stacks/{name}/compare", get(compare))
}

async fn gateways(State(state): State<Arc<UiState>>) -> ApiResult<Value> {
    let report: GatewayClusterStatusResponse =
        bounded(state.connection.get_json("/v1/gateway")).await?;
    Ok(Json(
        json!({"generation":report.desired_generation,"nodes":report.nodes,"config":report.config}),
    ))
}

async fn routes_view(State(state): State<Arc<UiState>>) -> ApiResult<Value> {
    let status: StatusResponse = bounded(state.connection.get_json("/v1/status")).await?;
    Ok(Json(
        json!({"generation":status.state.gateway_generation,"routes":route_rows(&status.state)}),
    ))
}

fn address(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn route_rows(state: &ClusterState) -> Vec<Value> {
    let mut rows = Vec::new();
    // Use the retained Gateway fragment: recomputing from desired services would hide
    // last-known upstreams during recovery and mix desired with published routes.
    for (stack, fragment) in &state.gateway_routes {
        for (route_index, route) in fragment.gateway.http_routes.iter().enumerate() {
            for (rule_index, rule) in route.rules.iter().enumerate() {
                let service_id = rule
                    .backend
                    .service
                    .as_ref()
                    .map(|name| format!("{stack}.{name}"));
                let upstreams = if let Some(name) = &rule.backend.service {
                    fragment
                        .upstreams
                        .get(&ServicePortKey::new(
                            name,
                            rule.backend.port,
                            rule.backend.protocol,
                        ))
                        .cloned()
                        .unwrap_or_default()
                } else {
                    rule.backend
                        .host
                        .iter()
                        .map(|host| address(host, rule.backend.port))
                        .collect()
                };
                let replicas: Vec<_> = state.tasks.values().filter(|task|Some(&task.service_id) == service_id.as_ref()).map(|task| {
                    let host = state.nodes.get(&task.node_id).map(|node|node.address.as_str()).or_else(||state.members.get(&task.node_id).map(|node|node.address.as_str()));
                    let port = task.ports.iter().find(|port|port.target == rule.backend.port && port.protocol == "tcp").and_then(|port|port.published);
                    let endpoint = host.zip(port).map(|(host, port)|address(host, port));
                    json!({"id":task.id,"node":task.node_id,"endpoint":endpoint,"desired":task.desired,"observed":task.observed,
                        "routed":endpoint.as_ref().is_some_and(|value|upstreams.contains(value))})
                }).collect();
                rows.push(json!({"id":format!("{stack}:{route_index}:{rule_index}"),"stack":stack,"hostnames":route.hostnames,
                    "canonical_hostname":route.canonical_hostname,"tls":route.tls.unwrap_or(fragment.gateway.tls),"http":route.http.unwrap_or(fragment.gateway.http),
                    "matches":rule.matches,"rewrite":rule.rewrite,"backend":rule.backend,"service_id":service_id,"upstreams":upstreams,"replicas":replicas}));
            }
        }
    }
    rows
}

#[derive(Deserialize)]
struct CompareQuery {
    from: u64,
    to: u64,
}
async fn compare(
    State(state): State<Arc<UiState>>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<CompareQuery>,
) -> ApiResult<Value> {
    let status: StatusResponse = bounded(state.connection.get_json("/v1/status")).await?;
    let stack = status
        .state
        .stacks
        .get(&name)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Stack was not found.".into()))?;
    let snapshot = |generation| {
        stack
            .deployment
            .as_ref()
            .filter(|d| d.generation == generation)
            .or_else(|| stack.deployment_history.get(&generation))
            .ok_or_else(|| {
                ApiError(
                    StatusCode::NOT_FOUND,
                    format!("Generation #{generation} is no longer retained."),
                )
            })
    };
    let before =
        serde_json::to_value(&snapshot(query.from)?.snapshot).expect("snapshot serializes");
    let after = serde_json::to_value(&snapshot(query.to)?.snapshot).expect("snapshot serializes");
    let mut changes = Vec::new();
    diff("", Some(&before), Some(&after), &mut changes);
    Ok(Json(
        json!({"from":query.from,"to":query.to,"changes":changes}),
    ))
}

fn change(path: &str, before: Option<&Value>, after: Option<&Value>) -> Value {
    json!({"path":path,"kind":if before.is_none(){"added"}else if after.is_none(){"removed"}else{"changed"},
        "before":before,"after":after})
}
fn pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}
fn diff(path: &str, before: Option<&Value>, after: Option<&Value>, changes: &mut Vec<Value>) {
    if before == after {
        return;
    }
    if path.ends_with("/environment") {
        let vars = |value: Option<&Value>| -> BTreeMap<String, Value> {
            value
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(|entry| {
                    let (key, value) = entry.split_once('=').unwrap_or((entry, ""));
                    (key.to_owned(), json!(value))
                })
                .collect()
        };
        let a = vars(before);
        let b = vars(after);
        for key in a.keys().chain(b.keys()).collect::<BTreeSet<_>>() {
            if a.get(key) != b.get(key) {
                changes.push(change(
                    &format!("{path}/{}", pointer(key)),
                    a.get(key),
                    b.get(key),
                ));
            }
        }
        return;
    }
    let old_items = before.and_then(Value::as_array);
    let new_items = after.and_then(Value::as_array);
    if old_items
        .into_iter()
        .flatten()
        .chain(new_items.into_iter().flatten())
        .any(Value::is_object)
    {
        for index in 0..old_items
            .map_or(0, Vec::len)
            .max(new_items.map_or(0, Vec::len))
        {
            diff(
                &format!("{path}/{index}"),
                old_items.and_then(|items| items.get(index)),
                new_items.and_then(|items| items.get(index)),
                changes,
            );
        }
        return;
    }
    let a = before.and_then(Value::as_object);
    let b = after.and_then(Value::as_object);
    if a.is_some() || b.is_some() {
        let keys: BTreeSet<_> = a
            .into_iter()
            .flat_map(|v| v.keys())
            .chain(b.into_iter().flat_map(|v| v.keys()))
            .collect();
        for key in keys {
            diff(
                &format!("{path}/{}", pointer(key)),
                a.and_then(|v| v.get(key)),
                b.and_then(|v| v.get(key)),
                changes,
            );
        }
    } else {
        changes.push(change(path, before, after));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn snapshot_diff_displays_complete_configuration_changes() {
        let before = json!({"services":{"web":{"image":"nginx:1","replicas":2,"environment":["TOKEN=old-secret","REMOVED=secret"],"command":["old-secret"]}}});
        let after = json!({"services":{"web":{"image":"nginx:2","replicas":3,"environment":["TOKEN=new-secret","ADDED=secret"],"command":["new-secret"]},"new":{"environment":["PASSWORD=new-secret"]}},"gateway":{"http_routes":[{"hostnames":["example.com"]}]}});
        let mut changes = Vec::new();
        diff("", Some(&before), Some(&after), &mut changes);
        assert!(
            changes
                .iter()
                .any(|c| c["path"] == "/services/web/environment/TOKEN"
                    && c["before"] == "old-secret"
                    && c["after"] == "new-secret")
        );
        assert!(
            changes
                .iter()
                .any(|c| c["path"] == "/services/web/environment/TOKEN" && c["kind"] == "changed")
        );
        assert!(
            changes
                .iter()
                .any(|c| c["path"] == "/services/new/environment/PASSWORD" && c["kind"] == "added")
        );
        assert!(
            changes.iter().any(|c| c["path"] == "/services/web/replicas"
                && c["before"] == 2
                && c["after"] == 3)
        );
        let mut same = Vec::new();
        diff("", Some(&after), Some(&after), &mut same);
        assert!(same.is_empty());
    }
    #[test]
    fn routes_keep_retained_upstreams_even_without_live_nodes() {
        let gateway: swarmlite_stack::StackGatewaySpec=serde_json::from_value(json!({"http_routes":[{"hostnames":["example.com"],"rules":[{"matches":[{"path":"/api","type":"prefix"}],"backend":{"service":"web","port":80}}]}]})).unwrap();
        let mut state = ClusterState::default();
        state.gateway_routes.insert(
            "demo".into(),
            swarmlite_core::model::RecoveredStackGateway {
                gateway,
                upstreams: BTreeMap::from([(
                    ServicePortKey::new("web", 80, swarmlite_stack::HttpBackendProtocol::Http),
                    vec!["[::1]:21000".into()],
                )]),
            },
        );
        let rows = route_rows(&state);
        assert_eq!(rows[0]["service_id"], "demo.web");
        assert_eq!(rows[0]["upstreams"][0], "[::1]:21000");
        assert_eq!(rows[0]["replicas"], json!([]));
        assert_eq!(address("::1", 80), "[::1]:80");
    }

    #[test]
    fn route_differences_identify_the_changed_path_without_repeating_the_route() {
        let before = json!({"gateway":{"http_routes":[{"rules":[{"matches":[{"path":"/v1","type":"prefix"}]}]}]}});
        let after = json!({"gateway":{"http_routes":[{"rules":[{"matches":[{"path":"/api","type":"prefix"}]}]}]}});
        let mut changes = Vec::new();
        diff("", Some(&before), Some(&after), &mut changes);
        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0]["path"],
            "/gateway/http_routes/0/rules/0/matches/0/path"
        );
        assert_eq!(changes[0]["before"], "/v1");
        assert_eq!(changes[0]["after"], "/api");
    }
}
