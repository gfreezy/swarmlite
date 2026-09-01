mod cache;
mod proxy;
mod reference;
mod relay;
mod service;

pub use cache::{RegistryCacheConfig, RegistryCacheStats};
pub use oci_client::secrets::RegistryAuth;
pub use proxy::OutboundProxyConfig;
pub use reference::{ImageReference, RegistryRequest, RegistryResource};
pub use relay::{RelayHandle, spawn_relay};
pub use service::{RegistryService, RegistryServiceConfig};
