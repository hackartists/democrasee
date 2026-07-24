use crate::*;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub enum Platform {
    #[default]
    Desktop,
    Mobile,
    Tablet,
}

pub fn use_platform() -> Signal<Platform> {
    let mut platform = use_signal(|| {
        #[cfg(not(feature = "server"))]
        {
            let window = web_sys::window().unwrap();
            let width = window.inner_width().unwrap().as_f64().unwrap_or_default();

            if width < 550.0 {
                Platform::Mobile
            } else if width < 1024.0 {
                Platform::Tablet
            } else {
                Platform::Desktop
            }
        }

        #[cfg(feature = "server")]
        {
            use dioxus::fullstack::FullstackContext;
            let Some(ctx) = FullstackContext::current() else {
                return Platform::Desktop;
            };

            let parts = ctx.parts_mut();
            let user_agent = parts
                .headers
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();


            if user_agent.contains("mobile") {
                Platform::Mobile
            } else if user_agent.contains("tablet") {
                Platform::Tablet
            } else {
                Platform::Desktop
            }
        }
    });

    use_effect(move || {
        use wasm_bindgen::closure::Closure;
        use wasm_bindgen::JsCast;

        let closure = Closure::wrap(Box::new(move || {
            let width = web_sys::window().unwrap().inner_width().unwrap().as_f64().unwrap_or_default();
            platform.set(if width < 550.0 {
                Platform::Mobile
            } else if width < 1024.0 {
                Platform::Tablet
            } else {
                Platform::Desktop
            });
        }) as Box<dyn FnMut()>);
        web_sys::window().unwrap()
            .add_event_listener_with_callback("resize", closure.as_ref().unchecked_ref())
            .unwrap();
        closure.forget();
    });

    platform
}

/// True when running on an iOS device — the Tauri iOS WKWebView or iOS Safari
/// (iPhone/iPad/iPod). Client-side user-agent check; always `false` on the
/// server (and for non-iOS clients). Used to hide Google sign-in on iOS, where
/// the in-WebView Google OAuth flow is unreliable and Apple's App Store
/// guideline 4.8 would otherwise require offering Sign in with Apple.
#[cfg(not(feature = "server"))]
pub fn is_ios() -> bool {
    web_sys::window()
        .map(|w| {
            let nav = w.navigator();
            let ua = nav.user_agent().unwrap_or_default();
            let iphone = ua.contains("iPhone") || ua.contains("iPad") || ua.contains("iPod");

            // iPadOS 13+ (and the iPad WKWebView the App Store review runs)
            // masquerades as DESKTOP: the UA carries "Macintosh" with no "iPad"
            // token AND navigator.platform reports "MacIntel". A plain UA check
            // therefore misses every iPad — exactly why the reviewer (iPad Air)
            // still saw the Google/Wallet buttons. A real Mac has NO touchscreen
            // (max_touch_points == 0), so any Apple-desktop signal reporting
            // touch points can only be an iPad. Combine UA + platform so we
            // catch it regardless of which one the WebView chose to spoof.
            let touch = nav.max_touch_points() > 1;
            let platform = nav.platform().unwrap_or_default();
            let apple_desktop = ua.contains("Macintosh")
                || platform.contains("Mac")
                || platform.contains("iP");
            let ipad_masquerade = touch && apple_desktop;

            iphone || ipad_masquerade
        })
        .unwrap_or(false)
}

#[cfg(feature = "server")]
pub fn is_ios() -> bool {
    false
}

/// True when running on an Android device — the native Android WebView or a
/// mobile browser (UA contains "Android"). Client-side user-agent check;
/// always `false` on the server. Used together with `is_ios()` to keep
/// wallet sign-in web-only (hidden on both native mobile apps).
#[cfg(not(feature = "server"))]
pub fn is_android() -> bool {
    web_sys::window()
        .and_then(|w| w.navigator().user_agent().ok())
        .map(|ua| ua.contains("Android"))
        .unwrap_or(false)
}

#[cfg(feature = "server")]
pub fn is_android() -> bool {
    false
}
