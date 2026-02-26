import mysql from 'mysql2/promise';
import crypto from 'crypto';
import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        url: { type: 'string' },
        m_user: { type: 'string' },
        m_pass: { type: 'string' },
        root_cat: { type: 'string', default: '2' }, // Standard Magento root category is 2
        db_host: { type: 'string' },
        db_user: { type: 'string' },
        db_pass: { type: 'string' },
        db_name: { type: 'string' },
        db_table: { type: 'string' },
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
// We now filter by category_id to capture all product types, including Configurables
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
      categories {
        id
        name
      }
      price_range {
        minimum_price { final_price { value } }
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
    if (result.errors) {
        throw new Error(`GraphQL Error: ${JSON.stringify(result.errors)}`);
    }
    return result.data.products;
}

// --- 4. MAIN EXECUTION ---
async function main() {
    let db;
    try {
        const token = await getAuthToken();

        console.log(`Connecting to database ${values.db_name}...`);
        db = await mysql.createConnection({
            host: values.db_host,
            user: values.db_user,
            password: values.db_pass,
            database: values.db_name
        });

        // --- DYNAMIC TABLE RECREATION ---
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

        // --- FETCH & INSERT LOOP ---
        let currentPage = 1;
        let totalPages = 1;
        let totalFetched = 0;

        while (currentPage <= totalPages) {
            console.log(`Fetching page ${currentPage} of ${totalPages}...`);
            const productsData = await fetchMagentoPage(token, currentPage);
            
            if (currentPage === 1) {
                totalPages = productsData.page_info.total_pages;
                console.log(`Total catalog size to fetch: ${productsData.total_count} products.`);
            }

            const items = productsData.items;
            if (items.length > 0) {
                const rowValues = items.map(item => {
                    // Extract Categories (Safe fallback if empty)
                    const categoryIds = item.categories ? item.categories.map(c => c.id).join(',') : '';
                    const categoryNames = item.categories ? item.categories.map(c => c.name).join(' > ') : '';

                    return [
                        crypto.randomUUID(), 
                        item.sku, 
                        item.id?.toString() || null, 
                        item.attribute_set_id || null,
                        item.stock_status, 
                        item.price_range?.minimum_price?.final_price?.value || 0, 
                        item.name, 
                        item.__typename.replace('Product', '').toLowerCase(),
                        item.only_x_left_in_stock || null,
                        item.special_price || null,
                        item.special_from_date || null,
                        item.special_to_date || null,
                        item.url_key || null,
                        item.manufacturer?.toString() || null, // Custom attributes are often returned as Ints/Strings
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
                totalFetched += items.length;
            }
            currentPage++;
        }

        console.log(`Sync complete! Successfully dropped, recreated, and saved ${totalFetched} products to ${values.db_table}.`);

    } catch (error) {
        console.error('Script failed:', error);
    } finally {
        if (db) await db.end();
    }
}

main();