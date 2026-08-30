pub mod data_plane;

// Control-plane payloads deliberately use the stable serialized domain model.
// Re-exporting it here gives transport consumers one protocol-facing boundary
// without duplicating types or risking drift in the persisted and wire schemas.
pub use swarmlite_core::model;
