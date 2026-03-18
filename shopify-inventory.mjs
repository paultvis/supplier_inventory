import puppeteer from 'puppeteer';
import mysql from 'mysql2/promise';
import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        target_site:    { type: 'string' },
        sku_identifier: { type: 'string', default: 'sku' },
        mpn_identifier: { type: 'string', default: 'barcode' },
        lead_selector:  { type: 'string', default: '.product-stock-level__text, .lead-time, .dispatch-message, .stock-status' },
        lead_timeout:   { type: 'string', default: '10000' }, // ms to wait for lead time module to render
        threads:        { type: 'string', default: '3' },     // reduced default to avoid rate limiting
        request_delay:  { type: 'string', default: '500' },   // ms pause between products per worker
        cookie:         { type: 'string', default: '' },
        user_agent:     { type: 'string', default: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' },
        db_host:        { type: 'string' },
        db_user:        { type: 'string' },
        db_pass:        { type: 'string' },
        db_name:        { type: 'string' },
        db_table:       { type: 'string' }
    },
    strict: false
});

const requiredArgs = ['target_site', 'db_host', 'db_user', 'db_pass', 'db_name', 'db_table'];
for (const arg of requiredArgs) {
    if (!values[arg]) {
        console.error(`❌ Missing required parameter: --${arg}`);
        process.exit(1);
    }
}

const baseUrl       = values.target_site.replace(/\/$/, '');
const targetDomain  = new URL(baseUrl).hostname;
const maxConcurrent = parseInt(values.threads, 10) || 3;
const leadTimeout   = parseInt(values.lead_timeout, 10) || 10000;
const requestDelay  = parseInt(values.request_delay, 10) || 500;

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function applySession(page) {
    await page.setUserAgent(values.user_agent);
    if (values.cookie) {
        const cookieObjs = values.cookie.split(';').filter(c => c.trim() !== '').map(pair => {
            const [name, ...rest] = pair.trim().split('=');
            return { name: name.trim(), value: rest.join('=').trim(), domain: targetDomain };
        });
        if (cookieObjs.length > 0) await page.setCookie(...cookieObjs);
    }
}

// Creates a fresh worker tab with request interception configured
async function createWorkerTab(browser) {
    const tab = await browser.newPage();
    await applySession(tab);
    await tab.setRequestInterception(true);
    tab.on('request', (req) => {
        if (['image', 'stylesheet', 'font', 'media'].includes(req.resourceType())) req.abort();
        else req.continue();
    });
    return tab;
}

// --- 2. MAIN EXECUTION ---
async function main() {
    let pool;
    let browser;
    try {
        console.log(`Connecting to database ${values.db_name}...`);
        pool = mysql.createPool({
            host: values.db_host, user: values.db_user, password: values.db_pass,
            database: values.db_name, connectionLimit: maxConcurrent + 2
        });

        console.log(`Recreating table \`${values.db_table}\`...`);
        const createTableQuery = `
            CREATE TABLE \`${values.db_table}\` (
              \`id\`                INT AUTO_INCREMENT PRIMARY KEY,
              \`variant_id\`        BIGINT NOT NULL,
              \`supplier_url\`      VARCHAR(255) NOT NULL,
              \`product_url\`       VARCHAR(500) NOT NULL,
              \`sku\`               VARCHAR(128) NOT NULL,
              \`mpn\`               VARCHAR(128),
              \`title\`             TEXT,
              \`variant_title\`     VARCHAR(255),
              \`price\`             DECIMAL(12,2),
              \`stock_qty\`         INT,
              \`lead_time_message\` VARCHAR(255),
              \`scraped_at\`        DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY \`idx_variant_supplier\` (\`variant_id\`, \`supplier_url\`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;
        await pool.query(`DROP TABLE IF EXISTS \`${values.db_table}\``);
        await pool.query(createTableQuery);
        console.log(`✅ Table ready.`);

        console.log(`Launching headless browser...`);
        browser = await puppeteer.launch({
            headless: "new",
            protocolTimeout: 120000,
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu']
        });

        const discoveryPage = await browser.newPage();
        await applySession(discoveryPage);

        console.log(`Navigating to ${baseUrl} to initialize session and bypass CORS...`);
        await discoveryPage.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});

        // --- LOGIN VERIFICATION ---
        // Only runs when --cookie is provided.
        // Inspects the already-loaded homepage for indicators that the session is authenticated.
        // We stay on the main domain throughout — following the account link to its portal
        // doesn't work because it uses a separate subdomain with its own cookie scope.
        //
        // Three signals checked in priority order:
        //   1. Account link contains B2B company params (company_location_id) — definitive for trade portals
        //   2. Page contains a logout link — definitive for any logged-in Shopify session
        //   3. Page still shows a "Log in" link with no logout — definitive for guest sessions
        if (values.cookie) {
            console.log(`\n🔐 Verifying login session...`);

            const loginState = await discoveryPage.evaluate(() => {
                const links = Array.from(document.querySelectorAll('a'));
                const body = (document.body.innerText || '').toLowerCase();

                const accountLink = links.find(a => {
                    const text = a.textContent.trim().toLowerCase();
                    const href = (a.getAttribute('href') || '').toLowerCase();
                    return text.includes('account') || href.includes('/account') || href.includes('account.');
                });

                return {
                    accountHref:          accountLink ? accountLink.href : null,
                    accountText:          accountLink ? accountLink.textContent.trim() : null,
                    hasCompanyLocationId: accountLink ? accountLink.href.includes('company_location_id') : false,
                    hasLogout:            !!links.find(a => {
                                              const t = a.textContent.trim().toLowerCase();
                                              const h = (a.getAttribute('href') || '').toLowerCase();
                                              return t.includes('log out') || t.includes('logout') || t.includes('sign out') ||
                                                     h.includes('logout') || h.includes('sign_out');
                                          }),
                    hasLoginLink:         !!links.find(a => {
                                              const t = a.textContent.trim().toLowerCase();
                                              const h = (a.getAttribute('href') || '').toLowerCase();
                                              return (t === 'log in' || t === 'login' || t === 'sign in') ||
                                                     h.includes('/account/login') || h.endsWith('/login');
                                          }),
                };
            });

            if (loginState.hasCompanyLocationId) {
                console.log(`✅ Login verified — B2B trade session active (company_location_id present in account link).`);
                console.log(`   Account link: ${loginState.accountHref}`);
            } else if (loginState.hasLogout) {
                console.log(`✅ Login verified — logout link found on homepage.`);
            } else if (loginState.hasLoginLink) {
                console.error(`\n❌ LOGIN FAILED — homepage shows a "Log in" link, session cookies are not active.`);
                console.error(`   Account link found: "${loginState.accountText}" → ${loginState.accountHref}`);
                console.error(`\n   To fix: log in to ${baseUrl} in your browser, copy fresh`);
                console.error(`   cookies from DevTools → Application → Cookies, and update --cookie.\n`);
                await browser.close();
                await pool.end();
                process.exit(1);
            } else {
                // Could not find any definitive login/logout indicator — warn and continue.
                // The price sanity check below is the fallback confirmation.
                console.warn(`⚠️  Login check inconclusive — no logout or login link found on homepage.`);
                console.warn(`   Account link: "${loginState.accountText}" → ${loginState.accountHref}`);
                console.warn(`   Continuing — verify trade pricing in the price check output below.\n`);
            }
        } else {
            console.log(`ℹ️  No cookie provided — running as guest (public pricing).`);
        }

        // Discovery: fetch full product objects from the authenticated /products.json endpoint.
        // Prices captured here reflect the logged-in trade price, which is what we use for the DB.
        // The per-product .json fetch later is used only for inventory_quantity.
        console.log(`\n🔍 Fetching entire product catalog via master JSON endpoint...`);
        const allProducts = []; // [{ url, product }]
        let page = 1;
        let hasMore = true;
        let previousPageFirstHandle = '';

        while (hasMore) {
            const jsonUrl = `${baseUrl}/products.json?limit=250&page=${page}`;
            console.log(` -> Fetching catalog page ${page}...`);

            const data = await discoveryPage.evaluate(async (url) => {
                try {
                    const response = await fetch(url);
                    if (!response.ok) return { status: response.status, products: null };
                    const json = await response.json();
                    return { status: 200, products: json.products };
                } catch (e) {
                    return { status: 0, products: null };
                }
            }, jsonUrl);

            if (data && data.products && data.products.length > 0) {
                // Guard against Shopify pagination bug: page N returning same products as page N-1
                if (data.products[0].handle === previousPageFirstHandle) {
                    hasMore = false;
                    break;
                }
                previousPageFirstHandle = data.products[0].handle;
                for (const p of data.products) {
                    allProducts.push({ url: `${baseUrl}/products/${p.handle}`, product: p });
                }
                page++;
            } else {
                hasMore = false;
            }
        }

        // Deduplicate by URL
        const seen = new Set();
        const uniqueProducts = allProducts.filter(({ url }) => {
            if (seen.has(url)) return false;
            seen.add(url);
            return true;
        });

        console.log(`✅ Discovery complete. Found ${uniqueProducts.length} unique products.`);

        // Price sanity check — log the first product's first variant price so you can
        // visually confirm trade pricing is active before all 49 products are processed.
        if (uniqueProducts.length > 0) {
            const firstVariant = uniqueProducts[0].product?.variants?.[0];
            if (firstVariant) {
                console.log(`💰 Price check — "${uniqueProducts[0].product.title}" first variant: £${firstVariant.price} (${firstVariant.sku || firstVariant.id})`);
                console.log(`   ⚠️  If this looks like RRP rather than trade price, your session cookies have expired.\n`);
            }
        }

        console.log(`🚀 Launching ${maxConcurrent} browser threads to extract lead times & inventory...\n`);

        await discoveryPage.close();

        let currentIndex = 0;
        let completedCount = 0;
        // Items that returned 429 are pushed here and retried after the main queue is drained
        const retryQueue = [];

        async function worker(workerId) {
            let pageTab = await createWorkerTab(browser);

            // Processes the main queue then the retry queue
            const getNext = () => {
                if (currentIndex < uniqueProducts.length) return { item: uniqueProducts[currentIndex++], isRetry: false };
                if (retryQueue.length > 0) return { item: retryQueue.shift(), isRetry: true };
                return null;
            };

            while (true) {
                // Wait if main queue empty but retries may still be added by other workers
                let next = getNext();
                if (!next) {
                    // Small wait to allow other workers to potentially push to retryQueue
                    await sleep(200);
                    next = getNext();
                    if (!next) break;
                }

                const { item, isRetry } = next;
                const { url: productUrl, product } = item;

                if (isRetry) {
                    console.log(`[Thread ${workerId}] 🔄 Retrying: ${productUrl.split('/').pop()}`);
                    await sleep(5000); // back off before retry
                } else {
                    console.log(`[Thread ${workerId}] 🌐 Loading: ${productUrl.split('/').pop()}`);
                }

                try {
                    const navResponse = await pageTab.goto(productUrl, { waitUntil: 'domcontentloaded', timeout: 30000 })
                        .catch(err => {
                            console.warn(`[Thread ${workerId}] ⚠️ Navigation failed: ${err.message}`);
                            return null;
                        });

                    if (!navResponse) {
                        console.error(`[Thread ${workerId}] ❌ Skipping — page did not load: ${productUrl}`);
                        await pageTab.close().catch(() => {});
                        pageTab = await createWorkerTab(browser);
                        continue;
                    }

                    // Wait for the lead time module — injected by a third-party app after render
                    console.log(`[Thread ${workerId}] ⏳ Waiting for lead time module (up to ${leadTimeout}ms)...`);
                    await pageTab.waitForSelector(values.lead_selector, { timeout: leadTimeout })
                        .catch(() => {
                            console.log(`[Thread ${workerId}] ℹ️ Lead time module not found within timeout.`);
                        });

                    const leadTimeMessage = await pageTab.evaluate((selector) => {
                        const el = document.querySelector(selector);
                        return (el && el.innerText.trim()) ? el.innerText.trim() : null;
                    }, values.lead_selector);

                    if (leadTimeMessage) {
                        console.log(`[Thread ${workerId}] ⏱️ Lead time: "${leadTimeMessage}"`);
                    } else {
                        console.log(`[Thread ${workerId}] ℹ️ No lead time message found.`);
                    }

                    // Fetch per-product JSON for inventory_quantity.
                    // Price and all other variant fields come from the discovery data (trade price).
                    console.log(`[Thread ${workerId}] 📦 Fetching inventory data...`);
                    const inventoryData = await pageTab.evaluate(async (url) => {
                        const controller = new AbortController();
                        const timeoutId = setTimeout(() => controller.abort(), 10000);
                        try {
                            const response = await fetch(url + '.json', { signal: controller.signal });
                            clearTimeout(timeoutId);
                            if (!response.ok) return { status: response.status, variants: null };
                            const json = await response.json();
                            // Return only the fields we need to keep the payload small
                            const variants = (json.product?.variants || []).map(v => ({
                                id: v.id,
                                inventory_quantity: v.inventory_quantity
                            }));
                            return { status: 200, variants };
                        } catch (e) {
                            return { status: 0, variants: null };
                        }
                    }, productUrl);

                    if (inventoryData.status === 429) {
                        console.warn(`[Thread ${workerId}] ⚠️ Rate limited (429) — queuing for retry: ${productUrl.split('/').pop()}`);
                        retryQueue.push(item);
                        continue;
                    }

                    if (inventoryData.status !== 200) {
                        console.warn(`[Thread ${workerId}] ⚠️ Inventory fetch failed (HTTP ${inventoryData.status}) — saving without stock qty.`);
                    }

                    // Build a lookup map of variantId → inventory_quantity from the per-product fetch
                    const inventoryMap = new Map(
                        (inventoryData.variants || []).map(v => [v.id, v.inventory_quantity])
                    );

                    // Use discovery product data for price, sku, mpn, title, variant_title
                    const variants = product?.variants;
                    if (!Array.isArray(variants) || variants.length === 0) {
                        console.warn(`[Thread ${workerId}] ⚠️ No variants in discovery data for: ${productUrl}`);
                        continue;
                    }

                    console.log(`[Thread ${workerId}] 💾 Saving ${variants.length} variant(s) to DB...`);
                    const insertRows = [];
                    for (const variant of variants) {
                        const variantId = variant.id;
                        const sku       = variant[values.sku_identifier] || variant.sku || variantId.toString();
                        const mpn       = variant[values.mpn_identifier] || null;
                        // Prefer inventory from the per-product fetch; fall back to discovery value
                        const stockQty  = inventoryMap.has(variantId)
                            ? (inventoryMap.get(variantId) ?? null)
                            : (variant.inventory_quantity ?? null);

                        insertRows.push([
                            variantId, baseUrl, productUrl, sku, mpn,
                            product.title,
                            variant.title !== 'Default Title' ? variant.title : null,
                            variant.price, // trade price from authenticated discovery fetch
                            stockQty,
                            leadTimeMessage
                        ]);
                    }

                    if (insertRows.length > 0) {
                        const query = `
                            INSERT INTO \`${values.db_table}\`
                            (variant_id, supplier_url, product_url, sku, mpn, title, variant_title, price, stock_qty, lead_time_message)
                            VALUES ?
                            ON DUPLICATE KEY UPDATE
                            sku=VALUES(sku), mpn=VALUES(mpn), title=VALUES(title), variant_title=VALUES(variant_title),
                            price=VALUES(price), stock_qty=VALUES(stock_qty), lead_time_message=VALUES(lead_time_message),
                            scraped_at=NOW()
                        `;
                        await pool.query(query, [insertRows]);
                    }

                    completedCount++;
                    console.log(`[Thread ${workerId}] ✅ DONE (${completedCount}/${uniqueProducts.length})`);

                } catch (err) {
                    console.error(`[Thread ${workerId}] ❌ CRASH: ${err.message}`);
                    await pageTab.close().catch(() => {});
                    pageTab = await createWorkerTab(browser);
                }

                // Throttle between products to avoid overwhelming the server
                if (requestDelay > 0) await sleep(requestDelay);
            }

            await pageTab.close().catch(() => {});
        }

        const workers = [];
        for (let i = 1; i <= maxConcurrent; i++) {
            workers.push(worker(i));
        }
        await Promise.all(workers);

        console.log(`\n🎉 Scanner finished! ${completedCount}/${uniqueProducts.length} products saved to ${values.db_table}.`);

    } catch (error) {
        console.error('❌ Script failed:', error);
        process.exit(1);
    } finally {
        if (browser) await browser.close();
        if (pool) await pool.end();
    }
}

main();
