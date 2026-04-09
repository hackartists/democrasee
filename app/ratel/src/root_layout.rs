use crate::common::components::{Button, ButtonShape, ButtonStyle};
use crate::*;

#[component]
pub fn RootLayout() -> Element {
    rsx! {
        ErrorBoundary {
            handle_error: move |error: ErrorContext| {
                error!("Error in component tree: {:?}", error);
                rsx! {
                    ErrorPage { to: Route::Index {}, ctx: error }
                }
            },
            SuspenseBoundary { Outlet::<Route> {} }
        }
        PopupZone {}
        ToastProvider {}
    }
}
