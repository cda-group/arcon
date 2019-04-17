use hyper::rt::Future;
use hyper::Client;

use hyper_tls::HttpsConnector;

// TODO: Refactor

pub struct HttpsClient {
    client: Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>,
}

impl HttpsClient {
    pub fn new() -> HttpsClient {
        let https = HttpsConnector::new(4).unwrap();
        let client = Client::builder().build::<_, hyper::Body>(https);
        HttpsClient { client }
    }

    pub fn get(&self, url: String) -> impl Future<Item = hyper::Response<hyper::Body>, Error = ()> {
        let url = url.parse::<hyper::Uri>().unwrap(); // Fix
        self.client.get(url).map_err(|err| {
            eprintln!("Error {}", err);
        })
    }
}

pub struct HttpClient {
    client: Client<hyper::client::HttpConnector>,
}

impl HttpClient {
    pub fn new() -> HttpClient {
        HttpClient {
            client: Client::new(),
        }
    }

    pub fn get(&self, url: String) -> impl Future<Item = hyper::Response<hyper::Body>, Error = ()> {
        let url = url.parse::<hyper::Uri>().unwrap(); // Fix
        self.client.get(url).map_err(|err| {
            eprintln!("Error {}", err);
        })
    }
}
