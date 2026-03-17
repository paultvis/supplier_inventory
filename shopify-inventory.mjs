import puppeteer from 'puppeteer';
import mysql from 'mysql2/promise';
import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        target_site: { type: 'string' },
        sku_identifier: { type: 'string', default: 'sku' },
        mpn_identifier: { type: 'string', default: 'barcode' }, 
        lead_selector: { type: 'string', default: '.lead-time, .dispatch-message, .stock-status, .inventory' }, 
        threads: { type: 'string', default: '5' }, 
        cookie: { type: 'string', default: '' },
        user_agent: { type: 'string', default: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' },
        db_host: { type: 'string' },
        db_user: { type: 'string' },
        db_pass: { type: 'string' },
        db_name: { type: 'string' },
        db_table: { type: 'string' }
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

const baseUrl = values.target_site.replace(/\/$/, '');
const targetDomain = new URL(baseUrl).hostname;
const maxConcurrent = parseInt(values.threads, 10) || 5;

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
              \`id\` INT AUTO_INCREMENT PRIMARY KEY,
              \`variant_id\` BIGINT NOT NULL,
              \`supplier_url\` VARCHAR(255) NOT NULL,
              \`product_url\` VARCHAR(500) NOT NULL,
              \`sku\` VARCHAR(128) NOT NULL,
              \`mpn\` VARCHAR(128),
              \`title\` TEXT,
              \`variant_title\` VARCHAR(255),
              \`price\` DECIMAL(12,2),
              \`stock_qty\` INT,
              \`lead_time_message\` VARCHAR(255),
              \`scraped_at\` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY \`idx_variant_supplier\` (\`variant_id\`, \`supplier_url\`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;
        await pool.query(createTableQuery);
        await pool.query(`TRUNCATE TABLE \`${values.db_table}\``);
        console.log(`✅ Table ready.`);

        console.log(`Launching headless browser...`);
        browser = await puppeteer.launch({ 
            headless: "new",
            protocolTimeout: 120000, // FIX: Increased internal communication timeout to 2 minutes
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
        let previousPageFirstHandle = "";

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
                if (data.products[0].handle === previousPageFirstHandle) {
                    hasMore = false;
                    break;
                }
                previousPageFirstHandle = data.products[0].handle;
                const newUrls = data.products.map(p => `${baseUrl}/products/${p.handle}`);
                allProductUrls = allProductUrls.concat(newUrls);
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
            while (currentIndex < allProductUrls.length) {
                const productUrl = allProductUrls[currentIndex++];
                let pageTab;
                
                try {
                    pageTab = await browser.newPage();
                    await applySession(pageTab); 
                    
                    await pageTab.setRequestInterception(true);
                    pageTab.on('request', (req) => {
                        if (['image', 'stylesheet', 'font', 'media'].includes(req.resourceType())) req.abort();
                        else req.continue();
                    });

                    // VERBOSE LOGGING ADDED HERE
                    console.log(`[Thread ${workerId}] 🌐 Loading: ${productUrl.split('/').pop()}`);
                    await pageTab.goto(productUrl, { waitUntil: 'networkidle2', timeout: 20000 }).catch(() => {});
                    
                    console.log(`[Thread ${workerId}] ⏳ Waiting 2.5s for dynamic widgets...`);
                    await new Promise(resolve => setTimeout(resolve, 2500)); 
                    
                    let leadTimeMessage = await pageTab.evaluate((selector) => {
                        const el = document.querySelector(selector);
                        if (el && el.innerText.trim()) return el.innerText.trim();

                        const allTextElements = Array.from(document.querySelectorAll('p, span, div'));
                        const keywords = ['lead time', 'dispatch', 'delivery', 'days', 'in stock', 'out of stock'];
                        for (const element of allTextElements) {
                            const text = element.innerText.toLowerCase();
                            if (keywords.some(kw => text.includes(kw)) && text.length < 100) return element.innerText.trim();
                        }
                        return null;
                    }, values.lead_selector);

                    console.log(`[Thread ${workerId}] 📦 Fetching secure JSON data...`);
                    
                    // FIX: Added AbortController to prevent infinite hanging if the server doesn't respond
                    const productData = await pageTab.evaluate(async (url) => {
                        const controller = new AbortController();
                        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout on the fetch
                        
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
                        console.error(`[Thread ${workerId}] ⚠️ Skipped: JSON Fetch Error - ${productData.error}`);
                        continue; 
                    }

                    if (productData && productData.product) {
                        console.log(`[Thread ${workerId}] 💾 Saving ${productData.product.variants.length} variant(s) to DB...`);
                        const insertQueries = [];
                        for (const variant of productData.product.variants) {
                            const variantId = variant.id; 
                            const sku = variant[values.sku_identifier] || variant.sku || variantId.toString();
                            const mpn = variant[values.mpn_identifier] || null;
                            
                            insertQueries.push([
                                variantId, baseUrl, productUrl, sku, mpn, productData.product.title,
                                variant.title !== 'Default Title' ? variant.title : null,
                                variant.price, variant.inventory_quantity || 0, leadTimeMessage
                            ]);
                        }

                        if (insertQueries.length > 0) {
                            const query = `
                                INSERT INTO \`${values.db_table}\` 
                                (variant_id, supplier_url, product_url, sku, mpn, title, variant_title, price, stock_qty, lead_time_message) 
                                VALUES ? 
                                ON DUPLICATE KEY UPDATE 
                                sku=VALUES(sku), mpn=VALUES(mpn), title=VALUES(title), variant_title=VALUES(variant_title), price=VALUES(price), 
                                stock_qty=VALUES(stock_qty), lead_time_message=VALUES(lead_time_message), scraped_at=NOW()
                            `;
                            await pool.query(query, [insertQueries]);
                        }
                    }
                    
                    completedCount++;
                    console.log(`[Thread ${workerId}] ✅ DONE (${completedCount}/${allProductUrls.length})`);

                } catch (err) {
                    console.error(`[Thread ${workerId}] ❌ CRASH: ${err.message}`);
                } finally {
                    if (pageTab) await pageTab.close().catch(() => {});
                }
            }
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