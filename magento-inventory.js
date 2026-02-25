import mysql from 'mysql2/promise';
import crypto from 'crypto';
import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        url: { type: 'string' },        // e.g., https://yourwebsite.com
        m_user: { type: 'string' },     // Magento Customer Email
        m_pass: { type: 'string' },     // Magento Customer Password
        db_host: { type: 'string' },    // DB Host (e.g., localhost)
        db_user: { type: 'string' },    // DB User
        db_pass: { type: 'string' },    // DB Password
        db_name: { type: 'string' },    // DB Schema (e.g., vwr)
        db_table: { type: 'string' },   // Target Table (e.g., API_Vis_Product_List)
    },
    strict: false // Allows running without all args if you want to hardcode defaults for testing
});

// Validate required parameters
const requiredArgs = ['url', 'm_user', 'm_pass', 'db_host', 'db_user', 'db_pass', 'db_name', 'db_table'];
for (const arg of requiredArgs) {
    if (!values[arg]) {
        console.error(`Missing required parameter: --${arg}`);
        process.exit(1);
    }
}

// Clean URL to ensure no trailing slash
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
const QUERY = `
query GetProducts($pageSize: Int!, $currentPage: Int!) {
  products(search: "a", pageSize: $pageSize, currentPage: $currentPage) {
    total_count
    page_info { current_page total_pages }
    items {
      sku name uid __typename stock_status
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
            variables: { pageSize: 200, currentPage: page }
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

        // Wipe the target table for a fresh snapshot
        console.log(`Clearing table \`${values.db_table}\`...`);
        await db.execute(`TRUNCATE TABLE \`${values.db_table}\``);

        let currentPage = 1;
        let totalPages = 1;
        let totalFetched = 0;

        while (currentPage <= totalPages) {
            console.log(`Fetching page ${currentPage} of ${totalPages}...`);
            const productsData = await fetchMagentoPage(token, currentPage);
            
            if (currentPage === 1) {
                totalPages = productsData.page_info.total_pages;
                console.log(`Total catalog size: ${productsData.total_count} products.`);
            }

            const items = productsData.items;
            if (items.length > 0) {
                const rowValues = items.map(item => {
                    return [
                        crypto.randomUUID(), 
                        item.sku, 
                        item.uid, 
                        item.stock_status, 
                        item.price_range?.minimum_price?.final_price?.value || 0, 
                        item.name, 
                        item.__typename.replace('Product', '').toLowerCase()
                    ];
                });

                const query = `
                    INSERT INTO \`${values.db_table}\` 
                    (\`API_Vis_Product_List ID\`, \`Sku\`, \`Id\`, \`Status\`, \`Price\`, \`Name\`, \`Type id\`) 
                    VALUES ?
                `;
                
                await db.query(query, [rowValues]);
                totalFetched += items.length;
            }
            currentPage++;
        }

        console.log(`Sync complete! Saved ${totalFetched} products to ${values.db_table}.`);

    } catch (error) {
        console.error('Script failed:', error);
    } finally {
        if (db) await db.end();
    }
}

main();