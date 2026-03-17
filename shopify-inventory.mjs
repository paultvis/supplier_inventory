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
        // NEW: Optional Login Parameters
        login_url: { type: 'string', default: '' },
        auth_user: { type: 'string', default: '' },
        auth_pass: { type: 'string', default: '' },
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
const maxConcurrent = parseInt(values.threads, 10) || 5;

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
              UNIQUE KEY \`idx_sku_supplier\` (\`sku\`, \`supplier_url\`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;
        await pool.query(createTableQuery);
        await pool.query(`TRUNCATE TABLE \`${values.db_table}\``);
        console.log(`✅ Table ready.`);

        console.log(`Launching headless browser...`);
        browser = await puppeteer.launch({ 
            headless: "new",
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu'] 
        });

        const discoveryPage = await browser.newPage();

        // STEP 0: Optional Authentication
        if (values.login_url && values.auth_user && values.auth_pass) {
            console.log(`\n🔐 Attempting to log into Trade Portal: ${values.login_url}`);
            await discoveryPage.goto(values.login_url, { waitUntil: 'networkidle2' });
            
            // Standard Shopify login selectors
            await discoveryPage.type('input[name="customer[email]"]', values.auth_user);
            await discoveryPage.type('input[name="customer[password]"]', values.auth_pass);
            
            // Press Enter instead of finding the submit button to ensure theme compatibility
            await Promise.all([
                discoveryPage.waitForNavigation({ waitUntil: 'networkidle2' }),
                discoveryPage.keyboard.press('Enter')
            ]);
            console.log(`✅ Login flow complete. Session cookies established.`);
        }

        // STEP 1: All-JSON Catalog Discovery (Executed inside the browser to use cookies)
        console.log(`\n🔍 Fetching entire product catalog via JSON endpoint...`);
        let allProducts = [];
        let page = 1;
        let hasMore = true;

        while (hasMore) {
            const jsonUrl = `${baseUrl}/products.json?limit=250&page=${page}`;
            console.log(` -> Fetching page ${page}...`);
            
            // Fetch the JSON *inside* the browser page to ensure it uses the logged-in session
            const data = await discoveryPage.evaluate(async (url) => {
                const response = await fetch(url);
                if (!response.ok) return null;
                return await response.json();
            }, jsonUrl);

            if (data && data.products && data.products.length > 0) {
                allProducts = allProducts.concat(data.products);
                page++;
            } else {
                hasMore = false;
            }
        }

        console.log(`✅ JSON Discovery complete. Found ${allProducts.length} base products.`);
        console.log(`🚀 Launching ${maxConcurrent} browser threads to extract trade prices & lead times...\n`);

        await discoveryPage.close();

        // STEP 2: Multi-Threaded Lead Time Extraction & DB Insert
        let currentIndex = 0;
        let completedCount = 0;

        async function worker(workerId) {
            while (currentIndex < allProducts.length) {
                const product = allProducts[currentIndex++];
                const productUrl = `${baseUrl}/products/${product.handle}`;
                let pageTab;
                
                try {
                    pageTab = await browser.newPage();
                    await pageTab.setRequestInterception(true);
                    pageTab.on('request', (req) => {
                        // Block images/fonts to speed up extraction, but allow scripts for the lead-time widget
                        if (['image', 'stylesheet', 'font', 'media'].includes(req.resourceType())) req.abort();
                        else req.continue();
                    });

                    // A. Go to page and wait 2.5s for dynamic widgets to load
                    await pageTab.goto(productUrl, { waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
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

                    // B. Fetch the exact trade prices and inventory for the variants (inside the browser context)
                    const productData = await pageTab.evaluate(async (url) => {
                        const response = await fetch(url + '.json');
                        if (!response.ok) return null;
                        return await response.json();
                    }, productUrl);

                    if (productData && productData.product) {
                        const insertQueries = [];
                        for (const variant of productData.product.variants) {
                            const sku = variant[values.sku_identifier] || variant.sku || variant.id.toString();
                            const mpn = variant[values.mpn_identifier] || null;
                            
                            insertQueries.push([
                                baseUrl, productUrl, sku, mpn, productData.product.title,
                                variant.title !== 'Default Title' ? variant.title : null,
                                variant.price, variant.inventory_quantity || 0, leadTimeMessage
                            ]);
                        }

                        if (insertQueries.length > 0) {
                            const query = `
                                INSERT INTO \`${values.db_table}\` 
                                (supplier_url, product_url, sku, mpn, title, variant_title, price, stock_qty, lead_time_message) 
                                VALUES ? 
                                ON DUPLICATE KEY UPDATE 
                                mpn=VALUES(mpn), title=VALUES(title), variant_title=VALUES(variant_title), price=VALUES(price), 
                                stock_qty=VALUES(stock_qty), lead_time_message=VALUES(lead_time_message), scraped_at=NOW()
                            `;
                            await pool.query(query, [insertQueries]);
                        }
                    }
                    
                    completedCount++;
                    console.log(`[Thread ${workerId}] ✅ Processed (${completedCount}/${allProducts.length}): ${productUrl}`);

                } catch (err) {
                    console.error(`[Thread ${workerId}] ❌ Failed on ${productUrl}: ${err.message}`);
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