import asyncio
import json
from dotenv import load_dotenv
import os
from playwright.async_api import async_playwright


load_dotenv()

# =========================
# Environment config
# =========================

FB_EMAIL = os.getenv("FB_EMAIL")
FB_PASSWORD = os.getenv("FB_PASSWORD")

HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
SLOW_MO_MS = int(os.getenv("SLOW_MO_MS", "0"))
SCREENSHOT_EVERY_STEP = os.getenv("SCREENSHOT_EVERY_STEP", "false").lower() == "true"
PAUSE_ON_START = os.getenv("PAUSE_ON_START", "false").lower() == "true"

WS_HOST = os.getenv("WS_HOST", "localhost")
WS_PORT = int(os.getenv("WS_PORT", "8787"))

PROFILE_NAME = os.getenv("PROFILE_NAME", "Gidraf Orenja")
SAVE_LOGIN_INFO = os.getenv("SAVE_LOGIN_INFO", "true").lower() == "true"

USER_DATA_DIR = "./facebook-playwright-profile"
SCREENSHOT_DIR = os.getenv("SCREENSHOT_DIR", ".")
BROWSER_CHANNEL = os.getenv("BROWSER_CHANNEL", "").strip() or None

connected_clients = set()
otp_future = None


# =========================
# Utility functions
# =========================

def now_iso():
    return datetime.utcnow().isoformat() + "Z"


def timestamp_for_file():
    return datetime.now().strftime("%Y%m%d-%H%M%S")


async def notify(event: dict):
    """
    Sends event notifications to CLI and WebSocket clients.

    CLI is always active.
    WebSocket is optional.
    """
    payload = {
        **event,
        "timestamp": now_iso()
    }

    message = json.dumps(payload, ensure_ascii=False)

    # CLI notification
    print(f"[event] {message}", flush=True)

    # WebSocket notification
    dead_clients = []

    for client in connected_clients:
        try:
            await client.send(message)
        except Exception:
            dead_clients.append(client)

    for client in dead_clients:
        connected_clients.discard(client)


async def websocket_handler(websocket):
    """
    WebSocket server handler.

    Connect to:
      ws://localhost:8787

    Send OTP:
      {"type": "otp", "code": "123456"}
    """
    global otp_future

    connected_clients.add(websocket)

    await websocket.send(json.dumps({
        "type": "connected",
        "message": "Connected to Facebook automation WebSocket",
        "timestamp": now_iso()
    }))

    try:
        async for raw_message in websocket:
            try:
                data = json.loads(raw_message)

                if data.get("type") == "otp" and data.get("code"):
                    code = str(data["code"]).strip()

                    if otp_future and not otp_future.done():
                        otp_future.set_result({
                            "source": "websocket",
                            "code": code
                        })

                    await notify({
                        "type": "otp_received",
                        "source": "websocket",
                        "maskedCode": "*" * len(code)
                    })

                else:
                    await websocket.send(json.dumps({
                        "type": "warning",
                        "message": 'Unsupported message. Expected {"type":"otp","code":"123456"}',
                        "timestamp": now_iso()
                    }))

            except json.JSONDecodeError:
                await websocket.send(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON payload",
                    "timestamp": now_iso()
                }))

    finally:
        connected_clients.discard(websocket)


async def is_visible(locator, timeout=1000):
    try:
        await locator.first.wait_for(state="visible", timeout=timeout)
        return True
    except Exception:
        return False


async def is_enabled_and_editable(locator, timeout=1000):
    """
    Checks if an input is visible, enabled and editable.
    Prevents Playwright from timing out trying to fill disabled fields.
    """
    try:
        first = locator.first
        await first.wait_for(state="visible", timeout=timeout)

        return await first.evaluate(
            """
            el => {
                const disabled = el.disabled || el.getAttribute('aria-disabled') === 'true';
                const readonly = el.readOnly || el.getAttribute('readonly') !== null;
                return !disabled && !readonly;
            }
            """
        )
    except Exception:
        return False


async def wait_for_stable_page(page, stable_seconds=4, max_seconds=35):
    """
    Waits until the URL has stopped changing for a few seconds.
    Useful after login, OTP, save-login screen, and redirects.
    """
    await notify({
        "type": "waiting_for_redirects",
        "message": "Waiting for redirects/page transitions to stabilize"
    })

    previous_url = page.url
    stable_started = asyncio.get_event_loop().time()
    started = asyncio.get_event_loop().time()

    while True:
        await asyncio.sleep(1)

        current_url = page.url
        now = asyncio.get_event_loop().time()

        if current_url != previous_url:
            previous_url = current_url
            stable_started = now

            await notify({
                "type": "redirect_detected",
                "url": current_url
            })

        if now - stable_started >= stable_seconds:
            break

        if now - started >= max_seconds:
            await notify({
                "type": "redirect_wait_timeout",
                "message": "Redirect wait timed out; continuing anyway",
                "url": page.url
            })
            break

    try:
        await page.wait_for_load_state("domcontentloaded", timeout=10000)
    except Exception:
        pass

    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass


async def save_screenshot(page, label):
    Path(SCREENSHOT_DIR).mkdir(parents=True, exist_ok=True)

    screenshot_path = Path(SCREENSHOT_DIR) / f"{label}-{timestamp_for_file()}.png"

    await page.screenshot(
        path=str(screenshot_path),
        full_page=True
    )

    await notify({
        "type": "screenshot_saved",
        "label": label,
        "path": str(screenshot_path),
        "url": page.url
    })

    return str(screenshot_path)


async def wait_for_otp_from_cli_or_websocket():
    """
    Waits for OTP from either CLI or WebSocket.

    You can either:
    1. Type OTP in terminal
    2. Send OTP through WebSocket:
       {"type": "otp", "code": "123456"}
    """
    global otp_future

    loop = asyncio.get_event_loop()
    otp_future = loop.create_future()

    await notify({
        "type": "waiting_for_otp",
        "message": 'Enter OTP in CLI or send via WebSocket: {"type":"otp","code":"123456"}'
    })

    async def cli_input_task():
        code = await asyncio.to_thread(
            input,
            "\nEnter OTP in CLI, or send through WebSocket: "
        )

        code = code.strip()

        if code and not otp_future.done():
            otp_future.set_result({
                "source": "cli",
                "code": code
            })

    cli_task = asyncio.create_task(cli_input_task())

    result = await otp_future

    if not cli_task.done():
        cli_task.cancel()

    await notify({
        "type": "otp_selected",
        "source": result["source"],
        "maskedCode": "*" * len(result["code"])
    })

    return result["code"]


# =========================
# Screen handlers
# =========================

async def handle_saved_profile_picker(page):
    """
    Handles saved profile picker screen.
    Example:
      aria-label="Gidraf Orenja"
    """
    profile_button = page.get_by_role("button", name=PROFILE_NAME)

    use_another_profile_button = page.get_by_role(
        "button",
        name=re.compile(r"use another profile", re.I)
    )

    if await is_visible(profile_button, timeout=1500):
        await notify({
            "type": "saved_profile_detected",
            "profile": PROFILE_NAME
        })

        await profile_button.click()

        await notify({
            "type": "saved_profile_selected",
            "profile": PROFILE_NAME
        })

        await page.wait_for_timeout(3000)
        return True

    if await is_visible(use_another_profile_button, timeout=800):
        await notify({
            "type": "profile_picker_detected",
            "message": "Profile picker visible, but configured profile was not found",
            "configuredProfile": PROFILE_NAME
        })

    return False


async def handle_login_form(page):
    """
    Handles:
      Mobile number or email
      Password
      Log in

    Important fix:
    After clicking Log in, Facebook may keep the same form visible
    but disabled while it is processing. We detect that and skip refill.
    """
    email_input = page.locator(
        '#m_login_email, '
        'input[name="email"], '
        'input[aria-label="Mobile number or email"], '
        'input[aria-label="Mobile number or email address"]'
    )

    password_input = page.locator(
        '#m_login_password, '
        'input[name="pass"], '
        'input[aria-label="Password"]'
    )

    login_button = page.get_by_role(
        "button",
        name=re.compile(r"^log in$", re.I)
    )

    email_visible = await is_visible(email_input, timeout=1500)
    password_visible = await is_visible(password_input, timeout=1500)

    if not email_visible or not password_visible:
        return False

    email_ready = await is_enabled_and_editable(email_input, timeout=1000)
    password_ready = await is_enabled_and_editable(password_input, timeout=1000)

    if not email_ready or not password_ready:
        await notify({
            "type": "login_form_processing",
            "message": "Login form is visible but disabled. Facebook is likely processing login. Skipping refill."
        })

        try:
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass

        await page.wait_for_timeout(4000)
        return True

    await notify({
        "type": "login_form_detected",
        "message": "Login form detected and enabled"
    })

    if not FB_EMAIL or not FB_PASSWORD:
        raise RuntimeError("FB_EMAIL and FB_PASSWORD must be set in .env")

    await email_input.first.click()
    await email_input.first.fill(FB_EMAIL)

    await password_input.first.click()
    await password_input.first.fill(FB_PASSWORD)

    await notify({
        "type": "credentials_filled",
        "message": "Credentials filled from .env"
    })

    await login_button.click()

    await notify({
        "type": "login_submitted",
        "message": "Clicked Log in"
    })

    try:
        await page.wait_for_load_state("domcontentloaded", timeout=10000)
    except Exception:
        pass

    await page.wait_for_timeout(4000)

    return True


async def handle_otp_screen(page):
    """
    Handles WhatsApp OTP / authentication screen.
    """
    otp_heading = page.get_by_role(
        "heading",
        name=re.compile(
            r"check your whatsapp messages|enter code|two-factor|authentication|login code",
            re.I
        )
    )

    code_input = page.locator(
        'input[aria-label="Code"], '
        'input[inputmode="numeric"], '
        'input[name="approvals_code"], '
        'input[type="text"]'
    )

    continue_button = page.get_by_role(
        "button",
        name=re.compile(r"^continue$", re.I)
    )

    otp_heading_visible = await is_visible(otp_heading, timeout=1500)
    code_input_visible = await is_visible(code_input, timeout=1500)

    if not otp_heading_visible and not code_input_visible:
        return False

    await notify({
        "type": "otp_required",
        "method": "whatsapp_or_authentication",
        "message": "OTP / re-authentication screen detected"
    })

    code = await wait_for_otp_from_cli_or_websocket()

    await code_input.first.click()
    await code_input.first.fill(code)

    await notify({
        "type": "otp_filled",
        "maskedCode": "*" * len(code)
    })

    if await is_visible(continue_button, timeout=3000):
        await continue_button.click()

        await notify({
            "type": "otp_submitted",
            "message": "Clicked Continue"
        })
    else:
        await notify({
            "type": "continue_button_not_found",
            "message": "OTP was filled, but Continue button was not found"
        })

    await page.wait_for_timeout(5000)

    return True


async def handle_save_login_info(page):
    """
    Handles:
      Save your login info?
      Save
      Not now
    """
    heading = page.get_by_role(
        "heading",
        name=re.compile(r"save your login info", re.I)
    )

    save_button = page.get_by_role(
        "button",
        name=re.compile(r"^save$", re.I)
    )

    not_now_button = page.get_by_role(
        "button",
        name=re.compile(r"^not now$", re.I)
    )

    if not await is_visible(heading, timeout=1500):
        return False

    await notify({
        "type": "save_login_info_prompt",
        "message": "Save login info prompt detected"
    })

    if SAVE_LOGIN_INFO:
        if await is_visible(save_button, timeout=3000):
            await save_button.click()

            await notify({
                "type": "save_login_info_selected",
                "action": "save"
            })
        else:
            await notify({
                "type": "save_button_not_found",
                "message": "Save prompt detected, but Save button not found"
            })
    else:
        if await is_visible(not_now_button, timeout=3000):
            await not_now_button.click()

            await notify({
                "type": "save_login_info_selected",
                "action": "not_now"
            })
        else:
            await notify({
                "type": "not_now_button_not_found",
                "message": "Save prompt detected, but Not now button not found"
            })

    await page.wait_for_timeout(5000)

    return True


async def handle_continue_as_or_confirm(page):
    """
    Handles possible intermediate buttons after login.
    Facebook sometimes shows extra confirmation buttons.
    """
    possible_buttons = [
        page.get_by_role("button", name=re.compile(r"^continue$", re.I)),
        page.get_by_role("button", name=re.compile(r"continue as", re.I)),
        page.get_by_role("button", name=re.compile(r"^ok$", re.I)),
        page.get_by_role("button", name=re.compile(r"^yes$", re.I)),
    ]

    for button in possible_buttons:
        if await is_visible(button, timeout=800):
            label = "unknown"

            try:
                label = await button.first.inner_text(timeout=1000)
            except Exception:
                pass

            await notify({
                "type": "intermediate_button_detected",
                "label": label
            })

            await button.first.click()

            await notify({
                "type": "intermediate_button_clicked",
                "label": label
            })

            await page.wait_for_timeout(3000)
            return True

    return False


async def detect_login_error(page):
    """
    Detects common login error/checkpoint states.
    """
    error_texts = [
        r"incorrect password",
        r"password you entered is incorrect",
        r"couldn.t log in",
        r"try again",
        r"account temporarily locked",
        r"confirm your identity",
        r"checkpoint",
        r"suspicious login",
    ]

    body_text = ""

    try:
        body_text = await page.locator("body").inner_text(timeout=2000)
    except Exception:
        return False

    for pattern in error_texts:
        if re.search(pattern, body_text, re.I):
            await notify({
                "type": "login_error_or_checkpoint_detected",
                "pattern": pattern,
                "url": page.url
            })

            await save_screenshot(page, "facebook-login-error-or-checkpoint")
            return True

    return False


async def detect_successful_login(page):
    """
    Detects common logged-in Facebook markers.

    Avoids false-positive success when the login form is still present.
    """
    login_email = page.locator(
        '#m_login_email, input[name="email"], '
        'input[aria-label="Mobile number or email"], '
        'input[aria-label="Mobile number or email address"]'
    )

    login_password = page.locator(
        '#m_login_password, input[name="pass"], input[aria-label="Password"]'
    )

    if await is_visible(login_email, timeout=300) or await is_visible(login_password, timeout=300):
        return False

    current_url = page.url.lower()

    # Avoid claiming success on login/checkpoint URLs.
    bad_url_patterns = [
        "login",
        "checkpoint",
        "recover",
        "two_factor",
        "approvals",
    ]

    if any(pattern in current_url for pattern in bad_url_patterns):
        return False

    markers = [
        page.locator('div[role="feed"]'),
        page.locator('a[href*="/home.php"]'),
        page.get_by_role("button", name=re.compile(r"menu", re.I)),
        page.get_by_role("link", name=re.compile(r"home", re.I)),
        page.locator('[aria-label*="Stories"]'),
        page.locator('[aria-label*="Create"]'),
    ]

    for marker in markers:
        if await is_visible(marker, timeout=800):
            await notify({
                "type": "login_success_marker_detected",
                "url": page.url
            })
            return True

    # URL fallback.
    success_url_patterns = [
        "m.facebook.com/home",
        "m.facebook.com/?",
        "m.facebook.com/stories",
        "m.facebook.com/profile",
    ]

    if any(pattern in current_url for pattern in success_url_patterns):
        await notify({
            "type": "login_success_url_detected",
            "url": page.url
        })
        return True

    return False


# =========================
# Automation loop
# =========================

async def automation_loop(page):
    max_steps = 30

    for step in range(1, max_steps + 1):
        await notify({
            "type": "automation_step",
            "step": step,
            "url": page.url
        })

        await page.bring_to_front()

        if SCREENSHOT_EVERY_STEP:
            await save_screenshot(page, f"step-{step}")

        await page.wait_for_timeout(1500)

        if await detect_login_error(page):
            return False

        if await detect_successful_login(page):
            await wait_for_stable_page(page)
            await save_screenshot(page, "facebook-home")
            return True

        if await handle_saved_profile_picker(page):
            continue

        if await handle_login_form(page):
            continue

        if await handle_otp_screen(page):
            continue

        if await handle_save_login_info(page):
            continue

        if await handle_continue_as_or_confirm(page):
            continue

        await notify({
            "type": "unknown_or_waiting_screen",
            "step": step,
            "url": page.url,
            "message": "No known screen matched. Waiting before retry."
        })

        await save_screenshot(page, f"unknown-screen-step-{step}")

        await page.wait_for_timeout(3000)

    await notify({
        "type": "automation_timeout",
        "message": "Reached max automation steps without confirming successful login"
    })

    await save_screenshot(page, "automation-timeout")

    return False


# =========================
# Main
# =========================

async def main():
    Path(USER_DATA_DIR).mkdir(parents=True, exist_ok=True)
    Path(SCREENSHOT_DIR).mkdir(parents=True, exist_ok=True)

    websocket_server = await websockets.serve(
        websocket_handler,
        WS_HOST,
        WS_PORT
    )

    await notify({
        "type": "websocket_server_started",
        "url": f"ws://{WS_HOST}:{WS_PORT}"
    })

    async with async_playwright() as playwright:
        launch_kwargs = {
            "headless": HEADLESS,
            "viewport": {
                "width": 676,
                "height": 1472
            },
            "user_agent": (
                "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Mobile Safari/537.36"
            ),
            "locale": "en-US",
            "slow_mo": SLOW_MO_MS,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--window-size=676,1472",
                "--window-position=80,40",
            ],
        }

        if BROWSER_CHANNEL:
            launch_kwargs["channel"] = BROWSER_CHANNEL

        context = await playwright.chromium.launch_persistent_context(
            USER_DATA_DIR,
            **launch_kwargs
        )

        page = context.pages[0] if context.pages else await context.new_page()
        await page.bring_to_front()

        async def on_navigation(frame):
            if frame == page.main_frame:
                await notify({
                    "type": "navigation",
                    "url": page.url
                })

        async def on_dialog(dialog):
            await notify({
                "type": "dialog_detected",
                "message": dialog.message
            })

            await dialog.dismiss()

        page.on(
            "framenavigated",
            lambda frame: asyncio.create_task(on_navigation(frame))
        )

        page.on(
            "dialog",
            lambda dialog: asyncio.create_task(on_dialog(dialog))
        )

        await notify({
            "type": "browser_started",
            "headless": HEADLESS,
            "profileDir": USER_DATA_DIR,
            "slowMoMs": SLOW_MO_MS,
            "screenshotEveryStep": SCREENSHOT_EVERY_STEP
        })

        await page.goto(
            "https://m.facebook.com/",
            wait_until="domcontentloaded",
            timeout=60000
        )

        await page.bring_to_front()

        if PAUSE_ON_START:
            await notify({
                "type": "paused",
                "message": "Paused on start. Press Enter in CLI to continue."
            })

            await asyncio.to_thread(
                input,
                "Browser opened. Press Enter to continue automation..."
            )

        success = await automation_loop(page)

        if success:
            await notify({
                "type": "automation_finished",
                "status": "success",
                "message": "Login flow completed and home screenshot saved"
            })
        else:
            await notify({
                "type": "automation_finished",
                "status": "not_confirmed",
                "message": "Automation finished but login success was not confirmed"
            })

        if HEADLESS:
            await context.close()
            websocket_server.close()
            await websocket_server.wait_closed()
        else:
            print("\nBrowser is still open for inspection.")
            print("Press CTRL+C to stop.")

            await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")

import os
import re
from datetime import datetime
from pathlib import Path

