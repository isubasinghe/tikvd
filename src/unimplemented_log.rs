use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::{HeaderMap, Request, Response};
use http_body::{Body, Frame, SizeHint};
use tower_layer::Layer;
use tower_service::Service;
use tracing::warn;

const GRPC_STATUS: &str = "grpc-status";
const UNIMPLEMENTED_CODE: &[u8] = b"12";

pub fn reports_unimplemented(headers: &HeaderMap) -> bool {
    headers
        .get(GRPC_STATUS)
        .is_some_and(|status| status.as_bytes() == UNIMPLEMENTED_CODE)
}

fn log_unimplemented(path: &str) {
    warn!(path, "responded UNIMPLEMENTED");
}

#[derive(Clone, Copy)]
pub struct UnimplementedLogLayer;

impl<S> Layer<S> for UnimplementedLogLayer {
    type Service = UnimplementedLog<S>;

    fn layer(&self, inner: S) -> Self::Service {
        UnimplementedLog { inner }
    }
}

#[derive(Clone)]
pub struct UnimplementedLog<S> {
    inner: S,
}

impl<S, RequestBody, ResponseBody> Service<Request<RequestBody>> for UnimplementedLog<S>
where
    S: Service<Request<RequestBody>, Response = Response<ResponseBody>>,
    S::Future: Send + 'static,
{
    type Response = Response<LoggedBody<ResponseBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<RequestBody>) -> Self::Future {
        let path = request.uri().path().to_owned();
        let response = self.inner.call(request);
        Box::pin(async move {
            let response = response.await?;
            if reports_unimplemented(response.headers()) {
                log_unimplemented(&path);
            }
            Ok(response.map(|inner| LoggedBody { inner, path }))
        })
    }
}

pub struct LoggedBody<B> {
    inner: B,
    path: String,
}

impl<B: Body + Unpin> Body for LoggedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &polled
            && frame.trailers_ref().is_some_and(reports_unimplemented)
        {
            log_unimplemented(&self.path);
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};

    fn with_status(status: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("grpc-status", HeaderValue::from_static(status));
        headers
    }

    #[test]
    fn status_twelve_is_unimplemented() {
        assert!(reports_unimplemented(&with_status("12")));
    }

    #[test]
    fn other_statuses_and_no_status_are_not() {
        assert!(!reports_unimplemented(&with_status("0")));
        assert!(!reports_unimplemented(&with_status("14")));
        assert!(!reports_unimplemented(&with_status("120")));
        assert!(!reports_unimplemented(&HeaderMap::new()));
    }
}
