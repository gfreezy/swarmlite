use std::collections::{BTreeMap, HashMap};

use anyhow::{Context as _, Result};
use gtmpl::{Func, FuncError, Template, Value};

/// Node values available to a service template when its task is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateNode {
    pub id: String,
    pub hostname: String,
    pub platform_architecture: String,
    pub platform_os: String,
}

/// Swarm-compatible values available while expanding a task's container settings.
#[derive(Debug, Clone, Copy)]
pub struct TemplateContext<'a> {
    pub service_id: &'a str,
    pub service_name: &'a str,
    pub service_labels: &'a BTreeMap<String, String>,
    pub node: &'a TemplateNode,
    pub task_id: &'a str,
    pub task_name: &'a str,
    pub task_slot: &'a str,
}

impl TemplateContext<'_> {
    /// Expand environment values like SwarmKit's `template.expandEnv`: only the
    /// portion after the first `=` is a template, while the variable name is literal.
    pub fn expand_environment(&self, environment: &[String]) -> Result<Vec<String>> {
        environment
            .iter()
            .map(|entry| {
                let Some((name, value)) = entry.split_once('=') else {
                    return Ok(entry.clone());
                };
                let expanded = self
                    .expand(value)
                    .with_context(|| format!("failed to expand environment variable {name:?}"))?;
                Ok(format!("{name}={expanded}"))
            })
            .collect()
    }

    fn expand(&self, value: &str) -> Result<String> {
        let mut template = Template::default();
        template.add_func("join", swarm_join as Func);
        template
            .parse(value)
            .with_context(|| format!("invalid template {value:?}"))?;
        template
            .render(&gtmpl::Context::from(self.as_value()))
            .with_context(|| format!("failed to render template {value:?}"))
    }

    fn as_value(&self) -> Value {
        let service = object([
            ("ID", Value::from(self.service_id)),
            ("Name", Value::from(self.service_name)),
            (
                "Labels",
                Value::Map(
                    self.service_labels
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::from(value)))
                        .collect(),
                ),
            ),
        ]);
        let platform = object([
            (
                "Architecture",
                Value::from(self.node.platform_architecture.as_str()),
            ),
            ("OS", Value::from(self.node.platform_os.as_str())),
        ]);
        let node = object([
            ("ID", Value::from(self.node.id.as_str())),
            ("Hostname", Value::from(self.node.hostname.as_str())),
            ("Platform", platform),
        ]);
        let task = object([
            ("ID", Value::from(self.task_id)),
            ("Name", Value::from(self.task_name)),
            ("Slot", Value::from(self.task_slot)),
        ]);
        object([("Service", service), ("Node", node), ("Task", task)])
    }
}

pub(crate) fn validate_environment_templates(
    environment: &[String],
    service_labels: &BTreeMap<String, String>,
) -> Result<()> {
    let node = TemplateNode {
        id: "node-id".to_owned(),
        hostname: "node-hostname".to_owned(),
        platform_architecture: "architecture".to_owned(),
        platform_os: "os".to_owned(),
    };
    TemplateContext {
        service_id: "service-id",
        service_name: "service-name",
        service_labels,
        node: &node,
        task_id: "task-id",
        task_name: "service-name.1.task-id",
        task_slot: "1",
    }
    .expand_environment(environment)
    .map(|_| ())
}

fn object<const N: usize>(entries: [(&str, Value); N]) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<HashMap<_, _>>(),
    )
}

fn swarm_join(arguments: &[Value]) -> std::result::Result<Value, FuncError> {
    let Some((separator, values)) = arguments.split_first() else {
        return Err(FuncError::AtLeastXArgs("join".to_owned(), 1));
    };
    let Value::String(separator) = separator else {
        return Err(FuncError::UnableToConvertFromValue);
    };
    let values = values
        .iter()
        .map(|value| match value {
            Value::String(value) => Ok(value.as_str()),
            _ => Err(FuncError::UnableToConvertFromValue),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(Value::from(values.join(separator)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_the_swarm_template_context_in_environment_values() {
        let labels = BTreeMap::from([("tier".to_owned(), "backend".to_owned())]);
        let node = TemplateNode {
            id: "node-id".into(),
            hostname: "worker-01".into(),
            platform_architecture: "x86_64".into(),
            platform_os: "linux".into(),
        };
        let context = TemplateContext {
            service_id: "service-id",
            service_name: "api",
            service_labels: &labels,
            node: &node,
            task_id: "task-id",
            task_name: "api.2.task-id",
            task_slot: "2",
        };

        let expanded = context
            .expand_environment(&[
                "UNCHANGED=value".into(),
                "{{.NotExpanded}}=value".into(),
                "SERVICE={{.Service.ID}}/{{.Service.Name}}".into(),
                "LABEL={{index .Service.Labels \"tier\"}}".into(),
                "NODE={{.Node.ID}}/{{.Node.Hostname}}".into(),
                "PLATFORM={{.Node.Platform.OS}}/{{.Node.Platform.Architecture}}".into(),
                "TASK={{.Task.ID}}/{{.Task.Name}}/{{.Task.Slot}}".into(),
                "JOINED={{join \"-\" .Service.Name .Task.Slot}}".into(),
                "INHERITED".into(),
            ])
            .unwrap();

        assert_eq!(
            expanded,
            [
                "UNCHANGED=value",
                "{{.NotExpanded}}=value",
                "SERVICE=service-id/api",
                "LABEL=backend",
                "NODE=node-id/worker-01",
                "PLATFORM=linux/x86_64",
                "TASK=task-id/api.2.task-id/2",
                "JOINED=api-2",
                "INHERITED",
            ]
        );
    }

    #[test]
    fn rejects_unknown_context_fields_and_invalid_templates() {
        for value in ["VALUE={{.Unknown}}", "VALUE={{.Task.Unknown}}", "VALUE={{"] {
            let error =
                validate_environment_templates(&[value.into()], &BTreeMap::new()).unwrap_err();
            assert!(format!("{error:#}").contains("VALUE"));
        }
    }
}
