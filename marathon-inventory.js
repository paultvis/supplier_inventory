import mysql from 'mysql2/promise';
import crypto from 'crypto';
import { parseArgs } from 'util';
import OAuth from 'oauth-1.0a';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        url: { type: 'string' },
        consumer_key: { type: 'string' },
        consumer_secret: { type: 'string' },
        access_token: { type: 'string' },
        token_secret: { type: 'string' },
        db_host: { type: 'string' },
        db_user: { type: 'string' },
        db_pass: { type: 'string' },
        db_name: { type: 'string' },
        db_table: { type: 'string' },
    },
    strict: false
});

const requiredArgs = ['url', 'consumer_key', 'consumer_secret', 'access_token', 'token_secret', 'db_host', 'db_user', 'db_pass', 'db_name', 'db_table'];
for (const arg of requiredArgs) {
    if (!values[arg]) {
        console.error(`Missing required parameter: --${arg}`);
        process.exit(1);
    }
}

const baseUrl = values.url.replace(/\/$/, '');

// --- 2. OAUTH 1.0a SETUP (SHA-256) ---
const oauth = new OAuth({
    consumer: {
        key: values.consumer_key,
        secret: values.consumer_secret
    },
    signature_method: 'HMAC-SHA256',
    hash_function(base_string, key) {
        return crypto.createHmac('sha256', key).update(base_string).digest('base64');
    },
});

const token = {
    key: values.access_token,
    secret: values.token_secret
};

// --- 3. API FETCHING ---
async function fetchCustomApiPage(page) {
    const requestUrl = `${baseUrl}/rest/V1/customerapi/products?searchCriteria[pageSize]=100&searchCriteria[currentPage]=${page}&searchCriteria[mapConfigurables]=1`;
    
    const requestData = {
        url: requestUrl,
        method: 'GET',
    };

    const headers = oauth.toHeader(oauth.authorize(requestData, token));

    const response = await fetch(requestUrl, {
        method: 'GET',
        headers: {
            ...headers,
            'Content-Type': 'application/json'
        }
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API fetch failed: ${response.status} ${response.statusText} - ${errorText}`);
    }

    return await response.json();
}

// --- 4. MAIN EXECUTION ---
async function main() {
    let db;
    try {
        console.log(`Connecting to database ${values.db_name}...`);
        db = await mysql.createConnection({
            host: values.db_host,
            user: values.db_user,
            password: values.db_pass,
            database: values.db_name
        });

        console.log(`Dropping and recreating table \`${values.db_table}\`...`);
        await db.execute(`DROP TABLE IF EXISTS \`${values.db_table}\``);
        
        // Added the `Rrp` column right after `Price`
        const createTableQuery = `
            CREATE TABLE \`${values.db_table}\` (
                \`API_Vis_Product_List ID\` CHAR(36) PRIMARY KEY,
                \`Sku\` VARCHAR(255),
                \`Id\` VARCHAR(255),
                \`Attribute_set_id\` INT,
                \`Status\` VARCHAR(50),
                \`Price\` DECIMAL(10,2),
                \`Rrp\` DECIMAL(10,2),
                \`Name\` TEXT,
                \`Type id\` VARCHAR(50),
                \`Only_x_left_in_stock\` DECIMAL(10,2),
                \`Special_price\` DECIMAL(10,2),
                \`Special_from_date\` VARCHAR(50),
                \`Special_to_date\` VARCHAR(50),
                \`Url_key\` VARCHAR(255),
                \`Manufacturer\` VARCHAR(255),
                \`Category_IDs\` TEXT,
                \`Category_Names\` TEXT
            )
        `;
        await db.execute(createTableQuery);

        let currentPage = 1;
        let maxPage = 1;
        let totalFetched = 0;

        console.log(`Starting fetch from ${baseUrl}...`);

        while (currentPage <= maxPage) {
            console.log(`Fetching page ${currentPage} of ${maxPage}...`);
            const responseData = await fetchCustomApiPage(currentPage);
            
            const metadata = responseData[0];
            const productsDict = responseData[1];

            if (currentPage === 1) {
                maxPage = metadata.max_page;
                console.log(`Total catalog size to fetch: ${metadata.total_products} products.`);
            }

            if (productsDict) {
                const uniqueProducts = new Map();

                for (const [key, item] of Object.entries(productsDict)) {
                    uniqueProducts.set(item.sku, item);

                    if (item.children && Array.isArray(item.children)) {
                        for (const child of item.children) {
                            if (!uniqueProducts.has(child.sku)) {
                                uniqueProducts.set(child.sku, child);
                            }
                        }
                    }
                }

                const rowValues = Array.from(uniqueProducts.values()).map(item => {
                    return [
                        crypto.randomUUID(), 
                        item.sku, 
                        null, 
                        null, 
                        item.in_stock === 1 ? 'IN_STOCK' : 'OUT_OF_STOCK', 
                        parseFloat(item.price_ex_vat_gbp || 0), 
                        parseFloat(item.rrp_ex_vat_gbp || 0), // Added RRP extraction here
                        item.name, 
                        item.type, 
                        parseFloat(item.stock_level || 0),
                        null, 
                        null, 
                        null, 
                        null, 
                        null, 
                        null, 
                        null  
                    ];
                });

                if (rowValues.length > 0) {
                    // Added `Rrp` to the INSERT statement
                    const insertQuery = `
                        INSERT INTO \`${values.db_table}\` 
                        (\`API_Vis_Product_List ID\`, \`Sku\`, \`Id\`, \`Attribute_set_id\`, \`Status\`, \`Price\`, \`Rrp\`, 
                         \`Name\`, \`Type id\`, \`Only_x_left_in_stock\`, \`Special_price\`, \`Special_from_date\`, 
                         \`Special_to_date\`, \`Url_key\`, \`Manufacturer\`, \`Category_IDs\`, \`Category_Names\`) 
                        VALUES ?
                    `;
                    
                    await db.query(insertQuery, [rowValues]);
                    totalFetched += rowValues.length;
                }
            }
            currentPage++;
        }

        console.log(`\nSync complete! Successfully saved ${totalFetched} products to ${values.db_table}.`);

    } catch (error) {
        console.error('Script failed:', error);
    } finally {
        if (db) await db.end();
    }
}

main();