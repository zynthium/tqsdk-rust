use tqsdk_core::{AdapterRegistry, ProtocolAdapter, RuntimeHandle};

pub struct TestRuntimeBuilder {
    adapters: AdapterRegistry,
}

impl Default for TestRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestRuntimeBuilder {
    pub fn new() -> Self {
        Self {
            adapters: AdapterRegistry::new(),
        }
    }

    pub fn with_default_adapters(mut self) -> Self {
        self.adapters.register_default_adapters();
        self
    }

    pub fn with_adapter<A>(mut self, adapter: A) -> Self
    where
        A: ProtocolAdapter + 'static,
    {
        self.adapters.register_adapter(adapter);
        self
    }

    pub fn build(self) -> RuntimeHandle {
        RuntimeHandle::with_adapters(self.adapters)
    }
}
