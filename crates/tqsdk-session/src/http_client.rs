#![cfg_attr(not(test), forbid(unsafe_code))]

pub(crate) fn direct_reqwest_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().no_proxy()
}

pub(crate) fn direct_reqwest_client() -> reqwest::Client {
    direct_reqwest_client_builder()
        .build()
        .expect("direct reqwest client should build")
}
