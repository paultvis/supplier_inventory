import mysql from 'mysql2/promise';
import crypto from 'crypto';
import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        url: { type: 'string' },        // e.g., https://yoursupplier.b2bwave.com
        b_email: { type: 'string' },    // B2BWave Customer Email (Username)
        b_token: { type: 'string' },    // B2BWave API Token (Password)
        db_host: { type: 'string' },
        db_user: { type: 'string' },
        db_pass: { type: 'string' },
        db_name: { type: 'string' },
        db_table: { type: 'string' },
    },
    strict: false
});

const requiredArgs = ['url', 'b_email', 'b_token', 'db_host', 'db_user', 'db_pass', 'db_name', 'db_table'];
for (const arg of requiredArgs) {
    if (!values[arg]) {
        console.error(`Missing required parameter: --${arg}`);
        process.exit(1);
    }
}

// Clean URL to ensure no trailing slash
const baseUrl = values.url.replace(/\/$/, '');

// Create the Basic Auth header string
const authHeader = 'Basic ' + Buffer.from(`${values.b_email}:${values.b_token}`).toString('base64');

// --- 2. API FETCHING ---
async function fetchB2BWavePage(offset = 0) {
    const endpoint = `${baseUrl}/api_customer/products?offset=${offset}`;
    
    const response = await fetch(endpoint, {
        method: 'GET',
        headers: {
            'Accept': 'application/json',
            'Authorization': authHeader
        }
    });

    if (!response.ok) {
        throw new Error(`API fetch failed: ${response.status} ${response.statusText}`);
    }

    return await response.json();
}

// --- 3. MAIN EXECUTION ---
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
        
        // Exact same schema as the Magento script for consistency
        const createTableQuery = `
            CREATE TABLE \`${values.db_table}\` (
                \`API_Vis_Product_List ID\` CHAR(36) PRIMARY KEY,
                \`Sku\` VARCHAR(255),
                \`Id\` VARCHAR(255),
                \`Attribute_set_id\` INT,
                \`Status\` VARCHAR(50),
                \`Price\` DECIMAL(10,2),
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

        let currentOffset = 0;
        let totalItems = 1; // Will update after the first request
        let totalFetched = 0;

        console.log(`Authenticating and starting fetch from ${baseUrl}...`);

        while (currentOffset < totalItems) {
            console.log(`Fetching from offset ${currentOffset}...`);
            const responseData = await fetchB2BWavePage(currentOffset);
            
            if (currentOffset === 0) {
                totalItems = responseData.pagination.total;
                console.log(`Total catalog size to fetch: ${totalItems} products.`);
            }

            const items = responseData.data;

            if (items && items.length > 0) {
                const rowValues = items.map(item => {
                    // B2BWave returns quantity as a string (e.g., "701.0"), so we parse it
                    const stockQty = parseFloat(item.quantity || 0);
                    const stockStatus = stockQty > 0 ? 'IN_STOCK' : 'OUT_OF_STOCK';

                    return [
                        crypto.randomUUID(), 
                        item.code, // B2BWave 'code' maps to 'Sku'
                        item.id?.toString(), 
                        null, // Attribute_set_id (Not applicable to B2BWave Customer API)
                        stockStatus, 
                        parseFloat(item.price || 0), 
                        item.name, 
                        'simple', // B2Bwave lists typically resolve to simple products in this API
                        stockQty,
                        null, // Special Price
                        null, // Special From
                        null, // Special To
                        null, // Url_key
                        null, // Manufacturer
                        null, // Category_IDs
                        item.category_path || '' // Category_Names
                    ];
                });

                const insertQuery = `
                    INSERT INTO \`${values.db_table}\` 
                    (\`API_Vis_Product_List ID\`, \`Sku\`, \`Id\`, \`Attribute_set_id\`, \`Status\`, \`Price\`, 
                     \`Name\`, \`Type id\`, \`Only_x_left_in_stock\`, \`Special_price\`, \`Special_from_date\`, 
                     \`Special_to_date\`, \`Url_key\`, \`Manufacturer\`, \`Category_IDs\`, \`Category_Names\`) 
                    VALUES ?
                `;
                
                await db.query(insertQuery, [rowValues]);
                totalFetched += items.length;
            }
            
            // Increment the offset by the limit returned in the pagination object
            currentOffset += responseData.pagination.limit;
        }

        console.log(`\nSync complete! Successfully saved ${totalFetched} products to ${values.db_table}.`);

    } catch (error) {
        console.error('Script failed:', error);
    } finally {
        if (db) await db.end();
    }
}

main();