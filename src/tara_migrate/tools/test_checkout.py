#!/usr/bin/env python3
"""Place test orders via Playwright to verify checkout flow.

Requires Shopify Payments test mode or Bogus Gateway to be enabled.
Uses Shopify's official test credit card numbers.

Prerequisites:
    pip install playwright
    playwright install chromium

Usage:
    python test_checkout.py                                    # One test order (Visa)
    python test_checkout.py --headed                           # Visible browser
    python test_checkout.py --card visa --card mastercard      # Multiple cards
    python test_checkout.py --card all                         # Test all card types
    python test_checkout.py --test-decline                     # Test declined card
    python test_checkout.py --base-url https://your-store.com  # Custom store URL
    python test_checkout.py --locale-prefix /ar                # Arabic checkout
    python test_checkout.py --bogus                            # Use Bogus Gateway cards
    python test_checkout.py --screenshot-dir data/checkout     # Save screenshots
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from urllib.parse import urlparse

from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Test card data (Shopify Payments test mode)
# https://help.shopify.com/en/manual/checkout-settings/test-orders
# ---------------------------------------------------------------------------

SHOPIFY_PAYMENTS_CARDS = {
    "visa":       {"number": "4242424242424242", "brand": "Visa"},
    "mastercard": {"number": "5555555555554444", "brand": "Mastercard"},
    "amex":       {"number": "378282246310005",  "brand": "American Express"},
    "discover":   {"number": "6011111111111111", "brand": "Discover"},
    "diners":     {"number": "30569309025904",   "brand": "Diners Club"},
    "jcb":        {"number": "3566002020360505", "brand": "JCB"},
    "unionpay":   {"number": "6200000000000005", "brand": "UnionPay"},
}

DECLINE_CARDS = {
    "declined":     {"number": "4000000000000002", "reason": "Generic decline"},
    "insufficient": {"number": "4000000000009995", "reason": "Insufficient funds"},
    "lost":         {"number": "4000000000009987", "reason": "Lost card"},
    "stolen":       {"number": "4000000000009979", "reason": "Stolen card"},
    "expired":      {"number": "4000000000000069", "reason": "Expired card"},
    "bad_cvc":      {"number": "4000000000000127", "reason": "Incorrect CVC"},
    "processing":   {"number": "4000000000000119", "reason": "Processing error"},
}

BOGUS_GATEWAY = {
    "approved": {"number": "1", "name": "Bogus Gateway", "brand": "Bogus (approved)"},
    "declined": {"number": "2", "name": "Bogus Gateway", "brand": "Bogus (declined)"},
    "failure":  {"number": "3", "name": "Bogus Gateway", "brand": "Bogus (failure)"},
}

# Test customer info
TEST_CUSTOMER = {
    "email": "test-order@taraformula.com",
    "first_name": "Test",
    "last_name": "Order",
    "address1": "123 Test Street",
    "city": "Riyadh",
    "province": "Riyadh",
    "zip": "12345",
    "country": "Saudi Arabia",
    "phone": "+966500000000",
}


# ---------------------------------------------------------------------------
# Checkout automation
# ---------------------------------------------------------------------------

def find_and_add_product(page, base_url, locale_prefix):
    """Navigate to a product page and add it to cart. Returns True on success."""
    # Try collections/all first to find products
    collections_url = f"{base_url.rstrip('/')}{locale_prefix}/collections/all"
    print(f"    Finding products at {collections_url}")

    try:
        page.goto(collections_url, wait_until="networkidle", timeout=30000)
        time.sleep(2)
    except Exception:
        # Fallback: try the homepage
        page.goto(f"{base_url.rstrip('/')}{locale_prefix}", wait_until="networkidle",
                  timeout=30000)
        time.sleep(2)

    # Find product links
    product_links = page.evaluate("""() => {
        const links = [];
        document.querySelectorAll('a[href*="/products/"]').forEach(a => {
            const href = a.href;
            if (!href.includes('/products/') || href.includes('?') ||
                href.endsWith('/products/') || href.endsWith('/products'))
                return;
            if (!links.includes(href)) links.push(href);
        });
        return links.slice(0, 5);
    }""")

    if not product_links:
        print("    ERROR: No product links found on the page")
        return False

    # Visit first product
    product_url = product_links[0]
    print(f"    Product: {urlparse(product_url).path}")
    page.goto(product_url, wait_until="networkidle", timeout=30000)
    time.sleep(2)

    # Click "Add to Cart" button
    added = page.evaluate("""() => {
        // Try form submission first
        const form = document.querySelector('form[action*="/cart/add"]');
        if (form) {
            const btn = form.querySelector('button[type="submit"], input[type="submit"]');
            if (btn && !btn.disabled) {
                btn.click();
                return 'clicked';
            }
            // Try submitting form directly via fetch
            const data = new FormData(form);
            fetch('/cart/add.js', { method: 'POST', body: data });
            return 'fetched';
        }
        // Fallback: look for any add-to-cart button
        const btns = document.querySelectorAll(
            '[data-action="add-to-cart"], .add-to-cart, #AddToCart, ' +
            'button[name="add"], [class*="add-to-cart"], [class*="AddToCart"]'
        );
        for (const b of btns) {
            if (!b.disabled) { b.click(); return 'clicked-fallback'; }
        }
        return null;
    }""")

    if not added:
        print("    ERROR: Could not find add-to-cart button")
        return False

    print(f"    Added to cart ({added})")
    time.sleep(3)
    return True


def fill_checkout_form(page, customer, card_number, card_name=None, cvv="111",
                       expiry_month="12", expiry_year="2028"):
    """Fill in the Shopify checkout form fields.

    Shopify checkout uses iframes for card fields. This handles both
    the legacy checkout and the newer Checkout Extensibility forms.
    """
    print("    Filling customer info...")

    # Email
    _fill_field(page, '[id*="email"], [name*="email"], [autocomplete="email"]',
                customer["email"])
    time.sleep(0.5)

    # Shipping address
    field_map = {
        '[id*="firstName"], [name*="firstName"], [autocomplete="given-name"]': customer["first_name"],
        '[id*="lastName"], [name*="lastName"], [autocomplete="family-name"]': customer["last_name"],
        '[id*="address1"], [name*="address1"], [autocomplete="address-line1"]': customer["address1"],
        '[id*="city"], [name*="city"], [autocomplete="address-level2"]': customer["city"],
        '[id*="postalCode"], [name*="postalCode"], [autocomplete="postal-code"]': customer["zip"],
    }

    for selector, value in field_map.items():
        _fill_field(page, selector, value)
        time.sleep(0.3)

    # Country dropdown (may be pre-selected)
    _try_select_country(page, customer["country"])

    # Phone
    _fill_field(page, '[id*="phone"], [name*="phone"], [autocomplete="tel"]',
                customer["phone"])

    time.sleep(1)

    # Continue to shipping / payment (click through sections)
    _click_continue(page)
    time.sleep(3)

    # Shipping method — select first available if needed
    _click_continue(page)
    time.sleep(3)

    # Payment — card fields are in iframes
    print("    Filling payment info...")
    _fill_card_fields(page, card_number, card_name or f"{customer['first_name']} {customer['last_name']}",
                      cvv, expiry_month, expiry_year)


def _fill_field(page, selector, value):
    """Fill a form field, trying multiple selectors."""
    for sel in selector.split(", "):
        sel = sel.strip()
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click()
                el.fill(value)
                return True
        except Exception:
            pass
    return False


def _try_select_country(page, country):
    """Try to select country from dropdown."""
    try:
        selects = page.query_selector_all('select[id*="country"], select[name*="country"], select[autocomplete="country-name"]')
        for sel in selects:
            if sel.is_visible():
                sel.select_option(label=country)
                return True
    except Exception:
        pass
    return False


def _click_continue(page):
    """Click the continue/submit button in checkout."""
    page.evaluate("""() => {
        const btns = document.querySelectorAll(
            'button[type="submit"], [id*="continue"], [class*="continue"], ' +
            '[data-step] button, .step__footer button'
        );
        for (const btn of btns) {
            if (btn.offsetWidth > 0 && btn.offsetHeight > 0 && !btn.disabled) {
                btn.click();
                return true;
            }
        }
        return false;
    }""")


def _fill_card_fields(page, number, name, cvv, exp_month, exp_year):
    """Fill credit card fields, handling Shopify's card iframes."""
    # Shopify checkout uses iframes for PCI-compliant card entry
    # Try direct fields first (Bogus Gateway doesn't use iframes)
    direct_filled = _fill_field(page, '[id*="number"], [data-card-field="number"]', number)

    if direct_filled:
        _fill_field(page, '[id*="name"], [data-card-field="name"]', name)
        _fill_field(page, '[id*="expiry"], [data-card-field="expiry"]', f"{exp_month}/{exp_year[-2:]}")
        _fill_field(page, '[id*="verification"], [id*="cvv"], [data-card-field="verification"]', cvv)
        return

    # Try Shopify's card iframes
    card_frames = page.frames
    for frame in card_frames:
        try:
            # Card number iframe
            num_field = frame.query_selector('input[id="number"], input[name="number"], input[placeholder*="card number" i]')
            if num_field:
                num_field.fill(number)
                print(f"      Card number filled in iframe")

            # Name on card
            name_field = frame.query_selector('input[id="name"], input[name="name"], input[placeholder*="name" i]')
            if name_field:
                name_field.fill(name)

            # Expiry
            exp_field = frame.query_selector('input[id="expiry"], input[name="expiry"], input[placeholder*="expir" i]')
            if exp_field:
                exp_field.fill(f"{exp_month}/{exp_year[-2:]}")

            # CVV
            cvv_field = frame.query_selector('input[id="verification_value"], input[name="verification_value"], input[placeholder*="security" i]')
            if cvv_field:
                cvv_field.fill(cvv)
        except Exception:
            continue


def submit_order(page):
    """Click the final pay/complete order button."""
    print("    Submitting order...")
    page.evaluate("""() => {
        const btns = document.querySelectorAll(
            '[id*="pay-button"], [id*="complete"], [data-testid*="pay"], ' +
            'button[type="submit"], .shown-if-js button'
        );
        for (const btn of btns) {
            const text = (btn.textContent || '').toLowerCase();
            if (btn.offsetWidth > 0 && (text.includes('pay') || text.includes('complete') ||
                text.includes('place') || text.includes('order') ||
                text.includes('إتمام') || text.includes('دفع'))) {
                btn.click();
                return true;
            }
        }
        // Fallback: any visible submit button in the payment section
        for (const btn of btns) {
            if (btn.offsetWidth > 0 && !btn.disabled) {
                btn.click();
                return true;
            }
        }
        return false;
    }""")


def check_order_result(page, expect_success=True):
    """Check if order was placed successfully or declined as expected."""
    time.sleep(8)  # Checkout processing takes a few seconds

    url = page.url
    page_text = page.evaluate("() => document.body.innerText.substring(0, 2000)")

    result = {
        "url": url,
        "timestamp": datetime.now().isoformat(),
    }

    if "/thank_you" in url or "/thank-you" in url or "order-confirmation" in url:
        # Extract order number if visible
        order_num = page.evaluate("""() => {
            const el = document.querySelector('[class*="order-number"], [class*="order_number"]');
            return el ? el.textContent.trim() : null;
        }""")
        result["status"] = "success"
        result["order_number"] = order_num
        if expect_success:
            print(f"    ORDER PLACED SUCCESSFULLY!")
            if order_num:
                print(f"    Order: {order_num}")
        else:
            print(f"    UNEXPECTED: Order went through (expected decline)")
    elif "error" in page_text.lower() or "declined" in page_text.lower() or "failed" in page_text.lower():
        result["status"] = "declined"
        # Extract error message
        error_msg = page.evaluate("""() => {
            const el = document.querySelector(
                '[class*="error"], [class*="notice"], [role="alert"], .banner--error'
            );
            return el ? el.textContent.trim().substring(0, 200) : null;
        }""")
        result["error"] = error_msg
        if not expect_success:
            print(f"    Correctly declined: {error_msg or 'Card declined'}")
        else:
            print(f"    ORDER FAILED: {error_msg or 'Unknown error'}")
    else:
        result["status"] = "unknown"
        result["page_text_preview"] = page_text[:300]
        print(f"    Result unclear — page URL: {url}")
        print(f"    Page text preview: {page_text[:200]}")

    return result


# ---------------------------------------------------------------------------
# Main test runner
# ---------------------------------------------------------------------------

def run_checkout_test(page, base_url, locale_prefix, card_info, customer,
                      expect_success=True, screenshot_dir=None, bogus=False):
    """Run a single checkout test with the given card."""
    brand = card_info.get("brand", card_info.get("reason", "Unknown"))
    number = card_info["number"]
    card_name = card_info.get("name")

    print(f"\n  {'=' * 60}")
    print(f"  Testing: {brand}")
    print(f"  Card: {number}")
    if not expect_success:
        print(f"  Expected: DECLINE ({card_info.get('reason', '')})")
    print(f"  {'=' * 60}")

    # Step 1: Add product to cart
    if not find_and_add_product(page, base_url, locale_prefix):
        return {"status": "error", "error": "Could not add product to cart", "card": brand}

    # Step 2: Go to checkout
    checkout_url = f"{base_url.rstrip('/')}/checkout"
    print(f"    Navigating to checkout...")
    page.goto(checkout_url, wait_until="networkidle", timeout=60000)
    time.sleep(3)

    if screenshot_dir:
        page.screenshot(path=os.path.join(screenshot_dir, f"checkout_{brand.lower().replace(' ', '_')}_start.png"))

    # Step 3: Fill the form
    cvv = "1111" if brand == "American Express" else "111"
    fill_checkout_form(page, customer, number, card_name=card_name, cvv=cvv)
    time.sleep(2)

    if screenshot_dir:
        page.screenshot(path=os.path.join(screenshot_dir, f"checkout_{brand.lower().replace(' ', '_')}_filled.png"))

    # Step 4: Submit
    submit_order(page)

    # Step 5: Check result
    result = check_order_result(page, expect_success=expect_success)
    result["card"] = brand
    result["card_number"] = number

    if screenshot_dir:
        page.screenshot(path=os.path.join(screenshot_dir, f"checkout_{brand.lower().replace(' ', '_')}_result.png"))

    # Clear cart for next test
    try:
        page.goto(f"{base_url.rstrip('/')}/cart/clear", wait_until="networkidle", timeout=10000)
    except Exception:
        pass

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Place test orders via Playwright to verify checkout flow")

    parser.add_argument("--base-url", default="https://sa.taraformula.com",
                        help="Store URL (default: sa.taraformula.com)")
    parser.add_argument("--locale-prefix", default="",
                        help="Locale path prefix (default: none)")
    parser.add_argument("--card", nargs="*", default=["visa"],
                        help="Card type(s) to test: visa, mastercard, amex, etc. or 'all'")
    parser.add_argument("--test-decline", action="store_true",
                        help="Also test a declined card")
    parser.add_argument("--decline-type", default="declined",
                        choices=list(DECLINE_CARDS.keys()),
                        help="Type of decline to test (default: generic declined)")
    parser.add_argument("--bogus", action="store_true",
                        help="Use Bogus Gateway test cards instead of Shopify Payments")
    parser.add_argument("--headed", action="store_true",
                        help="Run with visible browser")
    parser.add_argument("--screenshot-dir", default=None,
                        help="Save screenshots to this directory")
    parser.add_argument("--email", default=None,
                        help="Override test customer email")
    parser.add_argument("--json-out", default=None,
                        help="Save results to JSON file")
    parser.add_argument("--slow-mo", type=int, default=0,
                        help="Slow down actions by N ms (for debugging)")

    args = parser.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: Playwright not installed.")
        print("  pip install playwright")
        print("  playwright install chromium")
        sys.exit(1)

    load_dotenv()

    # Build card list
    cards_to_test = []
    card_names = args.card or ["visa"]

    if "all" in card_names:
        if args.bogus:
            cards_to_test = [(BOGUS_GATEWAY["approved"], True)]
        else:
            cards_to_test = [(info, True) for info in SHOPIFY_PAYMENTS_CARDS.values()]
    else:
        for name in card_names:
            name = name.lower()
            if args.bogus:
                cards_to_test.append((BOGUS_GATEWAY.get("approved", BOGUS_GATEWAY["approved"]), True))
            elif name in SHOPIFY_PAYMENTS_CARDS:
                cards_to_test.append((SHOPIFY_PAYMENTS_CARDS[name], True))
            else:
                print(f"WARNING: Unknown card type '{name}', skipping")

    if args.test_decline:
        if args.bogus:
            cards_to_test.append((BOGUS_GATEWAY["declined"], False))
        else:
            cards_to_test.append((DECLINE_CARDS[args.decline_type], False))

    if not cards_to_test:
        print("ERROR: No valid cards to test")
        sys.exit(1)

    # Customer info
    customer = TEST_CUSTOMER.copy()
    if args.email:
        customer["email"] = args.email

    # Screenshots
    if args.screenshot_dir:
        os.makedirs(args.screenshot_dir, exist_ok=True)

    print("=" * 70)
    print("  SHOPIFY CHECKOUT TEST")
    print(f"  Store: {args.base_url}")
    print(f"  Cards to test: {len(cards_to_test)}")
    print(f"  Gateway: {'Bogus Gateway' if args.bogus else 'Shopify Payments (test mode)'}")
    print("=" * 70)

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not args.headed,
            slow_mo=args.slow_mo,
        )
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="ar-SA" if "/ar" in args.locale_prefix else "en-US",
        )
        page = context.new_page()

        for card_info, expect_success in cards_to_test:
            try:
                result = run_checkout_test(
                    page, args.base_url, args.locale_prefix,
                    card_info, customer,
                    expect_success=expect_success,
                    screenshot_dir=args.screenshot_dir,
                    bogus=args.bogus,
                )
                results.append(result)
            except Exception as e:
                print(f"    ERROR: {e}")
                results.append({
                    "status": "error",
                    "card": card_info.get("brand", "unknown"),
                    "error": str(e),
                })
                # Take error screenshot
                if args.screenshot_dir:
                    try:
                        page.screenshot(path=os.path.join(
                            args.screenshot_dir, f"error_{card_info.get('brand', 'unknown').lower()}.png"))
                    except Exception:
                        pass

        browser.close()

    # Summary
    print(f"\n{'=' * 70}")
    print("  TEST RESULTS SUMMARY")
    print(f"{'=' * 70}")
    for r in results:
        status_icon = {"success": "PASS", "declined": "DECLINE", "error": "ERROR", "unknown": "???"}
        icon = status_icon.get(r["status"], "???")
        card = r.get("card", "unknown")
        extra = ""
        if r.get("order_number"):
            extra = f" (Order: {r['order_number']})"
        elif r.get("error"):
            extra = f" ({r['error'][:60]})"
        print(f"  [{icon:7s}] {card}: {r['status']}{extra}")

    # Save JSON
    if args.json_out:
        os.makedirs(os.path.dirname(args.json_out) or ".", exist_ok=True)
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n  Results saved to: {args.json_out}")

    # Exit code
    failures = [r for r in results if r["status"] == "error"]
    if failures:
        print(f"\n  {len(failures)} test(s) had errors")
        sys.exit(1)

    print()


if __name__ == "__main__":
    main()
