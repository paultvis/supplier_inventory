import puppeteer from 'puppeteer';
import mysql from 'mysql2/promise';
import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        target_site:    { type: 'string' },
        sku_identifier: { type: 'string', default: 'sku' },
        mpn_identifier: { type: 'string', default: 'barcode' },
        lead_selector:  { type: 'string', default: '.lead-time, .dispatch-message, .stock-status, .inventory' },
        lead_timeout:   { type: 'string', default: '10000' }, // ms to wait for lead time module to render
        threads:        { type: 'string', default: '5' },
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

const baseUrl        = values.target_site.replace(/\/$/, '');
const targetDomain   = new URL(baseUrl).hostname;
const maxConcurrent  = parseInt(values.threads, 10) || 5;
const leadTimeout    = parseInt(values.lead_timeout, 10) || 10000;

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

        console.log(`Ensuring table \`${values.db_table}\` exists and truncating...`);
        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS \`${values.db_table}\` (
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
        await pool.query(createTableQuery);
        await pool.query(`TRUNCATE TABLE \`${values.db_table}\``);
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

        console.log(`\n🔍 Fetching entire product catalog via master JSON endpoint...`);
        let allProductUrls = [];
        let page = 1;
        let hasMore = true;
        let previousPageFirstHandle = '';

        while (hasMore) {
            const jsonUrl = `${baseUrl}/products.json?limit=250&page=${page}`;
            console.log(` -> Fetching catalog page ${page}...`);

            const data = await discoveryPage.evaluate(async (url) => {
                try {
                    const response = await fetch(url);
                    if (!response.ok) return null;
                    return await response.json();
                } catch (e) {
                    return null;
                }
            }, jsonUrl);

            if (data && data.products && data.products.length > 0) {
                // Guard against Shopify pagination bug: page N returning same products as page N-1
                if (data.products[0].handle === previousPageFirstHandle) {
                    hasMore = false;
                    break;
                }
                previousPageFirstHandle = data.products[0].handle;
                allProductUrls = allProductUrls.concat(data.products.map(p => `${baseUrl}/products/${p.handle}`));
                page++;
            } else {
                hasMore = false;
            }
        }

        allProductUrls = [...new Set(allProductUrls)];
        console.log(`✅ JSON Discovery complete. Found ${allProductUrls.length} unique products.`);
        console.log(`🚀 Launching ${maxConcurrent} browser threads to extract trade prices & lead times...\n`);

        await discoveryPage.close();

        let currentIndex = 0;
        let completedCount = 0;

        async function worker(workerId) {
            // FIX: Create one tab per worker and reuse it — avoids create/destroy overhead per product
            let pageTab = await createWorkerTab(browser);

            while (currentIndex < allProductUrls.length) {
                const productUrl = allProductUrls[currentIndex++];

                try {
                    console.log(`[Thread ${workerId}] 🌐 Loading: ${productUrl.split('/').pop()}`);

                    // FIX: Use domcontentloaded instead of networkidle2.
                    // networkidle2 hangs indefinitely on stores running live chat, analytics,
                    // and other third-party widgets that make continuous background requests.
                    // domcontentloaded fires as soon as the HTML is parsed and scripts have run,
                    // which is all we need before waiting for the lead time module.
                    const navResponse = await pageTab.goto(productUrl, { waitUntil: 'domcontentloaded', timeout: 30000 })
                        .catch(err => {
                            console.warn(`[Thread ${workerId}] ⚠️ Navigation failed: ${err.message}`);
                            return null;
                        });

                    // FIX: Check navigation actually succeeded before attempting extraction
                    if (!navResponse) {
                        console.error(`[Thread ${workerId}] ❌ Skipping — page did not load: ${productUrl}`);
                        // Recreate the tab in case it is in a broken state
                        await pageTab.close().catch(() => {});
                        pageTab = await createWorkerTab(browser);
                        continue;
                    }

                    // FIX: Wait for the lead time module selector to appear in the DOM.
                    // The module is injected by a third-party Shopify app after the page
                    // has finished rendering, so we poll for it rather than using a fixed sleep.
                    // waitForSelector resolves as soon as the element appears (fast path) or
                    // rejects after leadTimeout ms (slow/missing path) — both are handled.
                    console.log(`[Thread ${workerId}] ⏳ Waiting for lead time module (up to ${leadTimeout}ms)...`);
                    await pageTab.waitForSelector(values.lead_selector, { timeout: leadTimeout })
                        .catch(() => {
                            console.log(`[Thread ${workerId}] ℹ️ Lead time module not found within timeout.`);
                        });

                    // FIX: Extract only from the configured selectors — removed the broad keyword
                    // fallback that matched footer text, shipping banners, and other irrelevant content
                    const leadTimeMessage = await pageTab.evaluate((selector) => {
                        const el = document.querySelector(selector);
                        return (el && el.innerText.trim()) ? el.innerText.trim() : null;
                    }, values.lead_selector);

                    if (leadTimeMessage) {
                        console.log(`[Thread ${workerId}] ⏱️ Lead time: "${leadTimeMessage}"`);
                    } else {
                        console.log(`[Thread ${workerId}] ℹ️ No lead time message found.`);
                    }

                    console.log(`[Thread ${workerId}] 📦 Fetching secure JSON data...`);

                    const productData = await pageTab.evaluate(async (url) => {
                        const controller = new AbortController();
                        const timeoutId = setTimeout(() => controller.abort(), 10000);
                        try {
                            const response = await fetch(url + '.json', { signal: controller.signal });
                            clearTimeout(timeoutId);
                            if (!response.ok) return { error: `HTTP ${response.status}` };
                            const text = await response.text();
                            return { product: JSON.parse(text).product };
                        } catch (e) {
                            return { error: e.name === 'AbortError' ? 'Fetch timed out after 10s' : e.message };
                        }
                    }, productUrl);

                    if (productData.error) {
                        console.error(`[Thread ${workerId}] ⚠️ Skipped: JSON fetch error — ${productData.error}`);
                        continue;
                    }

                    // FIX: Guard against malformed API responses with no variants array
                    const variants = productData?.product?.variants;
                    if (!Array.isArray(variants) || variants.length === 0) {
                        console.warn(`[Thread ${workerId}] ⚠️ No variants in response for: ${productUrl}`);
                        continue;
                    }

                    console.log(`[Thread ${workerId}] 💾 Saving ${variants.length} variant(s) to DB...`);
                    const insertRows = [];
                    for (const variant of variants) {
                        const variantId = variant.id;
                        const sku       = variant[values.sku_identifier] || variant.sku || variantId.toString();
                        const mpn       = variant[values.mpn_identifier] || null;

                        // FIX: Preserve null rather than coercing to 0.
                        // inventory_quantity is null when the store hides stock levels (common on B2B portals).
                        // Storing 0 would make every product appear out of stock — null is the honest value.
                        const stockQty = variant.inventory_quantity ?? null;
                        if (stockQty === null) {
                            console.warn(`[Thread ${workerId}] ⚠️ inventory_quantity is null for SKU "${sku}" — store may be hiding stock levels`);
                        }

                        insertRows.push([
                            variantId, baseUrl, productUrl, sku, mpn,
                            productData.product.title,
                            variant.title !== 'Default Title' ? variant.title : null,
                            variant.price, stockQty, leadTimeMessage
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
                    console.log(`[Thread ${workerId}] ✅ DONE (${completedCount}/${allProductUrls.length})`);

                } catch (err) {
                    console.error(`[Thread ${workerId}] ❌ CRASH: ${err.message}`);
                    // Recreate tab to recover from any bad browser state before continuing
                    await pageTab.close().catch(() => {});
                    pageTab = await createWorkerTab(browser);
                }
            }

            await pageTab.close().catch(() => {});
        }

        const workers = [];
        for (let i = 1; i <= maxConcurrent; i++) {
            workers.push(worker(i));
        }
        await Promise.all(workers);

        console.log(`\n🎉 Scanner finished! Data saved to ${values.db_table}.`);

    } catch (error) {
        console.error('❌ Script failed:', error);
        process.exit(1);
    } finally {
        if (browser) await browser.close();
        if (pool) await pool.end();
    }
}

main();
