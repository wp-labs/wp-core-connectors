use crate::registry;

pub fn register_builtin_factories<S, R>(register_sinks: S, register_sources: R)
where
    S: FnOnce(),
    R: FnOnce(),
{
    register_sinks();
    register_sources();
}

pub fn init_runtime_registries<S, R>(register_sinks: S, register_sources: R)
where
    S: FnOnce(),
    R: FnOnce(),
{
    register_builtin_factories(register_sinks, register_sources);
    log_registered_kinds();
}

pub fn log_registered_kinds() {
    let sinks = registry::sink_diagnostics();
    if sinks.is_empty() {
        log::warn!("no sinks registered");
    } else {
        for (k, loc) in sinks {
            log::info!("sink kind='{}' at {}:{}", k, loc.file(), loc.line());
        }
    }
    let srcs = registry::source_diagnostics();
    if srcs.is_empty() {
        log::warn!("no sources registered");
    } else {
        for (k, loc) in srcs {
            log::info!("source kind='{}' at {}:{}", k, loc.file(), loc.line());
        }
    }
}
