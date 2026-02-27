import { CheerioCrawler, log } from 'crawlee';
import puppeteer from 'puppeteer';
import * as cheerio from 'cheerio';
import mysql from 'mysql2/promise';
import crypto from 'crypto';
import { parseArgs } from 'util';
import fs from 'fs/promises'; 

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        url: { type: 'string', default: 'https://batterymegastore.b2bwave.com' },
        b_email: { type: 'string' },
        b_pass: { type: 'string' }, 
        db_host: { type: 'string' },
        db_user: { type: 'string' },
        db_pass: { type: 'string' },
        db_name: { type: 'string' },
        db_table: { type: 'string' },
    },
    strict: false
});

const baseUrl = values.url.replace(/\/$/, '');
const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// --- 2. JAVASCRIPT-INJECTED LOGIN ---
async function getSessionCookies() {
    console.log('Launching hidden browser for B2BWave login...');
    const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox', '--disable-setuid-sandbox'] });
    const page = await browser.newPage();
    await page.setUserAgent(USER_AGENT);
    
    console.log('Navigating to login page...');
    await page.goto(`${baseUrl}/customers/sign_in`, { waitUntil: 'networkidle2' });

    console.log('Injecting credentials directly into the DOM...');
    // This forces the values into the fields, bypassing anti-bot typing resets
    await page.evaluate((email, pass) => {
        document.querySelector('#customer_email').value = email;
        document.querySelector('#customer_password').value = pass;
    }, values.b_email, values.b_pass);
    
    console.log('Submitting login form...');
    await Promise.all([
        page.waitForNavigation({ waitUntil: 'networkidle2' }),
        page.click('input[name="commit"]') 
    ]);

    const currentUrl = page.url();
    if (currentUrl.includes('sign_in')) {
        await page.screenshot({ path: 'debug_login_failed.png' });
        await browser.close();
        throw new Error("❌ Login failed. Form submitted but stayed on login page. Check debug_login_failed.png");
    }

    const cookies = await page.cookies();
    await browser.close();

    const cookieString = cookies.map(c => `${c.name}=${c.value}`).join('; ');
    console.log('✅ Login successful! Session cookies retrieved.');
    return cookieString;
}

// --- 3. BRAND MAPPING ---
async function getBrandDictionary(cookieString) {
    console.log('Fetching brand category dictionary...');
    const response = await fetch(`${baseUrl}/products/list?category=7`, {
        headers: { 'Cookie': cookieString, 'User-Agent': USER_AGENT }
    });
    
    const html = await response.text();
    const $ = cheerio.load(html);
    const dictionary = new Map();

    $('.card-product-title').each((i, el) => {
        const brandName = $(el).text().trim();
        const href = $(el).attr('href');
        if (href) {
            dictionary.set(brandName.toLowerCase(), baseUrl + href);
        }
    });

    console.log(`Found ${dictionary.size} brands in the B2BWave directory.`);
    return dictionary;
}

// --- 4. MAIN EXECUTION ---
async function main() {
    let db;
    try {
        console.log(`Connecting to database ${values.db_name}...`);
        db = await mysql.createConnection({
            host: values.db_host, user: values.db_user, password: values.db_pass, database: values.db_name
        });

        const [rows] = await db.query(`SELECT manufacturer FROM supplier_partno_prefix WHERE Supplier = 'BMS' AND In_scope = 1`);
        const targetBrands = rows.map(r => r.manufacturer);
        console.log(`Found ${targetBrands.length} in-scope brands to scrape.`);

        if (targetBrands.length === 0) return;

        await db.execute(`DROP TABLE IF EXISTS \`${values.db_table}\``);
        await db.execute(`
            CREATE TABLE \`${values.db_table}\` (
                \`API_Vis_Product_List ID\` CHAR(36) PRIMARY KEY, \`BrandName\` VARCHAR(255), \`Sku\` VARCHAR(255),
                \`Status\` VARCHAR(50), \`Price\` DECIMAL(10,2), \`Name\` TEXT, \`Only_x_left_in_stock\` DECIMAL(10,2)
            )
        `);

        const cookieHeader = await getSessionCookies();
        const brandDict = await getBrandDictionary(cookieHeader);

        const startRequests = targetBrands.map(brand => {
            const normalizedBrand = brand.toLowerCase();
            const startUrl = brandDict.has(normalizedBrand) 
                ? `${brandDict.get(normalizedBrand)}&per_page=96`
                : `${baseUrl}/products/search_list?utf8=%E2%9C%93&search=${encodeURIComponent(brand)}&per_page=96`;

            return { url: startUrl, userData: { brandName: brand, isSearchFallback: false } };
        });

        let productBuffer = [];
        let totalScraped = 0;

        async function flushBufferToDb() {
            if (productBuffer.length === 0) return;
            const insertQuery = `INSERT INTO \`${values.db_table}\` (\`API_Vis_Product_List ID\`, \`BrandName\`, \`Sku\`, \`Status\`, \`Price\`, \`Name\`, \`Only_x_left_in_stock\`) VALUES ?`;
            await db.query(insertQuery, [productBuffer]);
            totalScraped += productBuffer.length;
            productBuffer = []; 
        }

        const crawler = new CheerioCrawler({
            preNavigationHooks: [
                (crawlingContext) => {
                    crawlingContext.request.headers = { 'Cookie': cookieHeader, 'User-Agent': USER_AGENT };
                }
            ],
            
            async requestHandler({ $, request, enqueueLinks }) {
                log.info(`Processing: ${request.url}`);
                const { brandName, isSearchFallback } = request.userData;

                if ($('.alert-danger').text().includes('no products available')) {
                    if (!isSearchFallback) {
                        const searchUrl = `${baseUrl}/products/search_list?utf8=%E2%9C%93&search=${encodeURIComponent(brandName)}&per_page=96`;
                        await enqueueLinks({ urls: [searchUrl], userData: { brandName, isSearchFallback: true } });
                    }
                    return; 
                }

                // EXACT EXTRACTORS BASED ON YOUR SCREENSHOTS
                $('table.preferred-products tbody tr, .card-product').each((i, el) => {
                    // Skip hidden spacer rows
                    if ($(el).hasClass('second-row')) return;

                    // Support both List (table) and Grid (.card-product) views just in case
                    const title = $(el).find('td.product-title a, .card-product-title').text().trim();
                    const rawCode = $(el).find('td.line-item.code a, .code-smaller, .product-code').text().replace(/Code:|SKU:/i, '').trim();
                    
                    // The screenshot shows data-price="6.03" on the span, which is much cleaner than parsing text
                    let rawPrice = $(el).find('td.price-col span.price').attr('data-price');
                    if (!rawPrice) rawPrice = $(el).find('.price').text().replace(/[^0-9.]/g, '');
                    
                    const qtyText = $(el).find('td.avl-qty, .in-stock').text().replace(/[^0-9.]/g, '');
                    const qtyInput = $(el).find('input[name="quantity"]').attr('max');
                    
                    const qty = parseFloat(qtyText || qtyInput || 0);
                    const status = qty > 0 ? 'IN_STOCK' : 'OUT_OF_STOCK';

                    if (rawCode && title) {
                        productBuffer.push([crypto.randomUUID(), brandName, rawCode, status, parseFloat(rawPrice || 0), title, qty]);
                    }
                });

                if (productBuffer.length >= 100) await flushBufferToDb();

                await enqueueLinks({
                    selector: '.pagination a[rel="next"], .next_page a',
                    userData: { brandName, isSearchFallback }
                });
            }
        });

        console.log('Starting CheerioCrawler...');
        await crawler.run(startRequests);
        await flushBufferToDb();
        console.log(`\n✅ Scrape complete! Saved ${totalScraped} products to ${values.db_table}.`);

    } catch (error) {
        console.error('\nScript failed:', error.message);
    } finally {
        if (db) await db.end();
    }
}

main();