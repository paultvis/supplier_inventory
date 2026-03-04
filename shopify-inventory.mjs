import puppeteer from 'puppeteer';
import mysql from 'mysql2/promise';
import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        target_site: { type: 'string' },
        menus: { type: 'string' }, 
        sku_identifier: { type: 'string', default: 'sku' },
        mpn_identifier: { type: 'string', default: 'barcode' }, 
        lead_selector: { type: 'string', default: '.lead-time, .dispatch-message, .stock-status, .inventory' }, 
        db_host: { type: 'string' },
        db_user: { type: 'string' },
        db_pass: { type: 'string' },
        db_name: { type: 'string' },
        db_table: { type: 'string' }
    },
    strict: false
});

const requiredArgs = ['target_site', 'menus', 'db_host', 'db_user', 'db_pass', 'db_name', 'db_table'];
for (const arg of requiredArgs) {
    if (!values[arg]) {
        console.error(`❌ Missing required parameter: --${arg}`);
        process.exit(1);
    }
}

const baseUrl = values.target_site.replace(/\/$/, '');
const menuPaths = values.menus.split(',').map(m => m.trim());

// --- 2. MAIN EXECUTION ---
async function main() {
    let db;
    let browser;
    try {
        console.log(`Connecting to database ${values.db_name}...`);
        db = await mysql.createConnection({ host: values.db_host, user: values.db_user, password: values.db_pass, database: values.db_name });

        console.log(`Launching headless browser...`);
        browser = await puppeteer.launch({ 
            headless: "new",
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu'] 
        });
        const page = await browser.newPage();
        
        await page.setRequestInterception(true);
        page.on('request', (req) => {
            if (['image', 'stylesheet', 'font', 'media'].includes(req.resourceType())) {
                req.abort();
            } else {
                req.continue();
            }
        });

        const productUrls = new Set();

        // STEP 1: Discover Products
        for (const menuPath of menuPaths) {
            let currentPage = 1;
            let hasNextPage = true;

            while (hasNextPage) {
                const url = `${baseUrl}${menuPath}?page=${currentPage}`;
                console.log(`Scanning menu: ${url}`);
                await page.goto(url, { waitUntil: 'domcontentloaded' });

                const links = await page.evaluate(() => {
                    return Array.from(document.querySelectorAll('a[href*="/products/"]'))
                                .map(a => a.pathname)
                                .filter(href => href.includes('/products/')); 
                });

                if (links.length === 0) {
                    hasNextPage = false;
                } else {
                    const beforeCount = productUrls.size;
                    links.forEach(link => productUrls.add(`${baseUrl}${link.split('?')[0]}`));
                    const added = productUrls.size - beforeCount;
                    
                    console.log(`  -> Found ${links.length} product links (${added} new).`);
                    
                    if (added === 0) hasNextPage = false; 
                    else currentPage++;
                }
            }
        }

        console.log(`\n✅ Discovery complete. Found ${productUrls.size} unique products. Beginning extraction...`);

        // STEP 2: Hybrid Extraction (Puppeteer + JSON)
        for (const productUrl of productUrls) {
            try {
                console.log(`Extracting: ${productUrl}`);
                
                await page.goto(productUrl, { waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
                
                let leadTimeMessage = await page.evaluate((selector) => {
                    const el = document.querySelector(selector);
                    if (el && el.innerText.trim()) return el.innerText.trim();

                    const allTextElements = Array.from(document.querySelectorAll('p, span, div'));
                    const keywords = ['lead time', 'dispatch', 'delivery', 'days', 'in stock', 'out of stock'];
                    for (const element of allTextElements) {
                        const text = element.innerText.toLowerCase();
                        if (keywords.some(kw => text.includes(kw)) && text.length < 100) {
                            return element.innerText.trim();
                        }
                    }
                    return null;
                }, values.lead_selector);

                const jsonResponse = await fetch(`${productUrl}.json`);
                if (!jsonResponse.ok) continue;
                
                const jsonData = await jsonResponse.json();
                const product = jsonData.product;

                const insertQueries = [];
                for (const variant of product.variants) {
                    // Extract SKU and MPN based on your parameterized identifiers
                    const sku = variant[values.sku_identifier] || variant.sku || variant.id.toString();
                    const mpn = variant[values.mpn_identifier] || null;
                    
                    insertQueries.push([
                        baseUrl,
                        productUrl,
                        sku,
                        mpn,
                        product.title,
                        variant.title !== 'Default Title' ? variant.title : null,
                        variant.price,
                        variant.inventory_quantity || 0,
                        leadTimeMessage
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
                    await db.query(query, [insertQueries]);
                }

            } catch (err) {
                console.error(`  -> Failed to extract ${productUrl}: ${err.message}`);
            }
        }

        console.log(`\n🎉 Scanner finished successfully! Data saved to ${values.db_table}.`);

    } catch (error) {
        console.error('❌ Script failed:', error);
        process.exit(1);
    } finally {
        if (browser) await browser.close();
        if (db) await db.end();
    }
}

main();