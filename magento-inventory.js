import mysql from 'mysql2/promise';
import crypto from 'crypto';
import { parseArgs } from 'util';
import fs from 'fs/promises';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        url: { type: 'string' },
        m_user: { type: 'string' },
        m_pass: { type: 'string' },
        root_cat: { type: 'string', default: '2' }, 
        db_host: { type: 'string' },
        db_user: { type: 'string' },
        db_pass: { type: 'string' },
        db_name: { type: 'string' },
        db_table: { type: 'string' },
        log_file: { type: 'string', default: 'failed_products.log' }
    },
    strict: false
});

const requiredArgs = ['url', 'm_user', 'm_pass', 'db_host', 'db_user', 'db_pass', 'db_name', 'db_table'];
for (const arg of requiredArgs) {
    if (!values[arg]) {
        console.error(`Missing required parameter: --${arg}`);
        process.exit(1);
    }
}

const baseUrl = values.url.replace(/\/$/, '');

// Helper to write to the error log
async function logFailedProduct(message) {
    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] ${message}\n`;
    await fs.appendFile(values.log_file, logEntry);
}

// --- 2. AUTHENTICATION ---
async function getAuthToken() {
    console.log(`Authenticating with ${baseUrl}...`);
    const authUrl = `${baseUrl}/rest/V1/integration/customer/token`;
    
    const response = await fetch(authUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            username: values.m_user,
            password: values.m_pass
        })
    });

    if (!response.ok) {
        throw new Error(`Auth failed: ${response.status} ${response.statusText}`);
    }

    const token = await response.json();
    console.log('Authentication successful. Token retrieved.');
    return token;
}

// --- 3. GRAPHQL FETCHING ---
// Added the "... on ConfigurableProduct" fragment to extract hidden children
const QUERY = `
query GetProducts($categoryId: String!, $pageSize: Int!, $currentPage: Int!) {
  products(filter: { category_id: { eq: $categoryId } }, pageSize: $pageSize, currentPage: $currentPage) {
    total_count
    page_info { current_page total_pages }
    items {
      id
      attribute_set_id
      sku 
      name 
      __typename 
      stock_status
      only_x_left_in_stock
      url_key
      manufacturer
      special_price
      special_from_date
      special_to_date
      categories { id name }
      price_range { minimum_price { final_price { value } } }
      
      ... on ConfigurableProduct {
        variants {
          product {
            id
            attribute_set_id
            sku
            name
            __typename
            stock_status
            only_x_left_in_stock
            url_key
            manufacturer
            special_price
            special_from_date
            special_to_date
            price_range { minimum_price { final_price { value } } }
          }
        }
      }
    }
  }
}
`;

async function fetchMagentoPage(token, page) {
    const response = await fetch(`${baseUrl}/graphql`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
            query: QUERY,
            variables: { 
                categoryId: values.root_cat, 
                pageSize: 100, 
                currentPage: page 
            } 
        })
    });

    const result = await response.json();
    
    // FAULT TOLERANCE & LOGGING
    if (result.errors) {
        console.warn(`\n[WARNING] Magento encountered an issue on page ${page}. Check ${values.log_file} for details.`);
        
        for (const err of result.errors) {
            const index = err.path && err.path.length > 0 ? err.path[err.path.length - 1] : 'Unknown Index';
            const logMsg = `Page ${page} | Item Index ${index} | Error: ${err.message}`;
            await logFailedProduct(logMsg);
        }

        if (!result.data || !result.data.products) {
            throw new Error(`Fatal GraphQL Error: ${JSON.stringify(result.errors)}`);
        }
    }
    
    return result.data.products;
}

// --- 4. MAIN EXECUTION ---
async function main() {
    let db;
    try {
        await fs.writeFile(values.log_file, `--- Starting New Sync: ${new Date().toISOString()} ---\n`);

        const token = await getAuthToken();

        console.log(`Connecting to database ${values.db_name}...`);
        db = await mysql.createConnection({
            host: values.db_host,
            user: values.db_user,
            password: values.db_pass,
            database: values.db_name
        });

        console.log(`Dropping and recreating table \`${values.db_table}\`...`);
        await db.execute(`DROP TABLE IF EXISTS \`${values.db_table}\``);
        
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

        let currentPage = 1;
        let totalPages = 1;
        let totalFetched = 0;

        while (currentPage <= totalPages) {
            console.log(`Fetching page ${currentPage} of ${totalPages}...`);
            const productsData = await fetchMagentoPage(token, currentPage);
            
            if (currentPage === 1) {
                totalPages = productsData.page_info.total_pages;
                // Note: This count represents PARENT products, the final row count will be higher
                console.log(`Total visible catalog items to fetch: ${productsData.total_count}. (Child variants will be added dynamically).`);
            }

            const validItems = (productsData.items || []).filter(item => item !== null);

            // FLATTEN THE DATA: Extract parents and their hidden children
            const flattenedProducts = [];
            for (const item of validItems) {
                // 1. Add the Parent Product
                flattenedProducts.push(item);

                // 2. Add the Child Products (if it is a Configurable Product)
                if (item.variants && Array.isArray(item.variants)) {
                    for (const variant of item.variants) {
                        if (variant.product) {
                            // Pass down parent category info if the child has none (common in Magento)
                            variant.product.categories = variant.product.categories || item.categories;
                            flattenedProducts.push(variant.product);
                        }
                    }
                }
            }

            if (flattenedProducts.length > 0) {
                const rowValues = flattenedProducts.map(item => {
                    const categoryIds = item.categories ? item.categories.map(c => c.id).join(',') : '';
                    const categoryNames = item.categories ? item.categories.map(c => c.name).join(' > ') : '';
                    const typeId = (item.__typename || 'unknown').replace('Product', '').toLowerCase();

                    return [
                        crypto.randomUUID(), 
                        item.sku, 
                        item.id?.toString() || null, 
                        item.attribute_set_id || null,
                        item.stock_status || 'UNKNOWN', 
                        item.price_range?.minimum_price?.final_price?.value || 0, 
                        item.name || 'Unknown Product', 
                        typeId,
                        item.only_x_left_in_stock || null,
                        item.special_price || null,
                        item.special_from_date || null,
                        item.special_to_date || null,
                        item.url_key || null,
                        item.manufacturer?.toString() || null, 
                        categoryIds,
                        categoryNames
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
                totalFetched += flattenedProducts.length;
            }
            currentPage++;
        }

        console.log(`\nSync complete! Successfully saved ${totalFetched} total products (including variants) to ${values.db_table}.`);

    } catch (error) {
        console.error('Script failed:', error);
    } finally {
        if (db) await db.end();
    }
}

main();