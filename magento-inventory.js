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
        db_options_table: { type: 'string' }, // NEW: Table for the option configurations
        log_file: { type: 'string', default: 'failed_products.log' }
    },
    strict: false
});

const requiredArgs = ['url', 'm_user', 'm_pass', 'db_host', 'db_user', 'db_pass', 'db_name', 'db_table', 'db_options_table'];
for (const arg of requiredArgs) {
    if (!values[arg]) {
        console.error(`Missing required parameter: --${arg}`);
        process.exit(1);
    }
}

const baseUrl = values.url.replace(/\/$/, '');

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
        body: JSON.stringify({ username: values.m_user, password: values.m_pass })
    });

    if (!response.ok) throw new Error(`Auth failed: ${response.status} ${response.statusText}`);
    const token = await response.json();
    console.log('Authentication successful. Token retrieved.');
    return token;
}

// --- 3. GRAPHQL FETCHING ---
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
        configurable_options {
          id
          attribute_id_v2
          label
          position
          values {
            value_index
          }
        }
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
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ query: QUERY, variables: { categoryId: values.root_cat, pageSize: 100, currentPage: page } })
    });

    const result = await response.json();
    
    if (result.errors) {
        console.warn(`\n[WARNING] Magento issue on page ${page}. Check ${values.log_file} for details.`);
        for (const err of result.errors) {
            const index = err.path && err.path.length > 0 ? err.path[err.path.length - 1] : 'Unknown Index';
            await logFailedProduct(`Page ${page} | Item Index ${index} | Error: ${err.message}`);
        }
        if (!result.data || !result.data.products) throw new Error(`Fatal GraphQL Error: ${JSON.stringify(result.errors)}`);
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
        db = await mysql.createConnection({ host: values.db_host, user: values.db_user, password: values.db_pass, database: values.db_name });

        // Table 1: Main Products
        console.log(`Dropping and recreating main table \`${values.db_table}\`...`);
        await db.execute(`DROP TABLE IF EXISTS \`${values.db_table}\``);
        await db.execute(`
            CREATE TABLE \`${values.db_table}\` (
                \`API_Vis_Product_List ID\` CHAR(36) PRIMARY KEY, \`Sku\` VARCHAR(255), \`Id\` VARCHAR(255),
                \`Attribute_set_id\` INT, \`Status\` VARCHAR(50), \`Price\` DECIMAL(10,2), \`Name\` TEXT,
                \`Type id\` VARCHAR(50), \`Only_x_left_in_stock\` DECIMAL(10,2), \`Special_price\` DECIMAL(10,2),
                \`Special_from_date\` VARCHAR(50), \`Special_to_date\` VARCHAR(50), \`Url_key\` VARCHAR(255),
                \`Manufacturer\` VARCHAR(255), \`Category_IDs\` TEXT, \`Category_Names\` TEXT
            )
        `);

        // Table 2: Configurable Options
        console.log(`Dropping and recreating options table \`${values.db_options_table}\`...`);
        await db.execute(`DROP TABLE IF EXISTS \`${values.db_options_table}\``);
        await db.execute(`
            CREATE TABLE \`${values.db_options_table}\` (
                \`Configurable product ID\` CHAR(36) PRIMARY KEY,
                \`API_Vis_Product_List ID\` CHAR(36),
                \`Configurable product options\` TEXT,
                \`Opt_Attribute id\` VARCHAR(50),
                \`Opt_Id\` VARCHAR(50),
                \`Opt_Label\` VARCHAR(255),
                \`Position\` VARCHAR(10),
                \`Opt_Product id\` VARCHAR(50)
            )
        `);

        let currentPage = 1, totalPages = 1, totalProductsFetched = 0, totalOptionsFetched = 0;

        while (currentPage <= totalPages) {
            console.log(`Fetching page ${currentPage} of ${totalPages}...`);
            const productsData = await fetchMagentoPage(token, currentPage);
            if (currentPage === 1) totalPages = productsData.page_info.total_pages;

            const validItems = (productsData.items || []).filter(item => item !== null);
            const flattenedProducts = [];
            const optionsDataBuffer = [];

            for (const item of validItems) {
                const parentUuid = crypto.randomUUID();
                
                // --- CONFIGURABLE OPTIONS EXTRACTION ---
                if (item.configurable_options && Array.isArray(item.configurable_options)) {
                    for (const opt of item.configurable_options) {
                        const optUuid = crypto.randomUUID();
                        // Recreate the Sequentum string (OptID AttrID Label Position ValueIndexes ProductID)
                        const valueIndices = opt.values ? opt.values.map(v => v.value_index).join(' ') : '';
                        const seqString = `${opt.id} ${opt.attribute_id_v2} ${opt.label} ${opt.position} ${valueIndices} ${item.id}`;

                        optionsDataBuffer.push([
                            optUuid, 
                            parentUuid, 
                            seqString, 
                            opt.attribute_id_v2?.toString(), 
                            opt.id?.toString(), 
                            opt.label, 
                            opt.position?.toString(), 
                            item.id?.toString()
                        ]);
                    }
                }

                // 1. Add Parent
                // Attach the UUID so it matches the options table foreign key
                flattenedProducts.push({ ...item, _assigned_uuid: parentUuid }); 

                // 2. Add Children
                if (item.variants && Array.isArray(item.variants)) {
                    for (const variant of item.variants) {
                        if (variant.product) {
                            variant.product.categories = variant.product.categories || item.categories;
                            flattenedProducts.push({ ...variant.product, _assigned_uuid: crypto.randomUUID() });
                        }
                    }
                }
            }

            // --- DATABASE INSERTS ---
            if (flattenedProducts.length > 0) {
                const rowValues = flattenedProducts.map(item => {
                    const catIds = item.categories ? item.categories.map(c => c.id).join(',') : '';
                    const catNames = item.categories ? item.categories.map(c => c.name).join(' > ') : '';
                    const typeId = (item.__typename || 'unknown').replace('Product', '').toLowerCase();

                    return [
                        item._assigned_uuid, item.sku, item.id?.toString() || null, item.attribute_set_id || null,
                        item.stock_status || 'UNKNOWN', item.price_range?.minimum_price?.final_price?.value || 0, 
                        item.name || 'Unknown Product', typeId, item.only_x_left_in_stock || null,
                        item.special_price || null, item.special_from_date || null, item.special_to_date || null,
                        item.url_key || null, item.manufacturer?.toString() || null, catIds, catNames
                    ];
                });

                await db.query(`INSERT INTO \`${values.db_table}\` VALUES ?`, [rowValues]);
                totalProductsFetched += flattenedProducts.length;
            }

            if (optionsDataBuffer.length > 0) {
                await db.query(`INSERT INTO \`${values.db_options_table}\` VALUES ?`, [optionsDataBuffer]);
                totalOptionsFetched += optionsDataBuffer.length;
            }

            currentPage++;
        }

        console.log(`\nSync complete!`);
        console.log(`Saved ${totalProductsFetched} products to ${values.db_table}.`);
        console.log(`Saved ${totalOptionsFetched} option configurations to ${values.db_options_table}.`);

    } catch (error) {
        console.error('Script failed:', error);
    } finally {
        if (db) await db.end();
    }
}

main();