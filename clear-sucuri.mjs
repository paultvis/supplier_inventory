import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        api_key: { type: 'string' },
        api_secret: { type: 'string' }
    },
    strict: false
});

if (!values.api_key || !values.api_secret) {
    console.error('❌ Missing required parameters: --api_key and --api_secret');
    process.exit(1);
}

// --- 2. MAIN EXECUTION ---
async function main() {
    console.log('Initiating Sucuri WAF/CDN cache purge...');
    
    const url = 'https://waf.sucuri.net/api?v2';
    
    // Sucuri expects standard URL-encoded form data
    const params = new URLSearchParams();
    params.append('k', values.api_key);
    params.append('s', values.api_secret);
    params.append('a', 'clear_cache');

    try {
        const response = await fetch(url, {
            method: 'POST',
            body: params
        });

        const resultText = await response.text();

        // Sucuri usually returns a simple text/HTML confirmation or JSON depending on the exact account tier
        if (!response.ok) {
            throw new Error(`Sucuri API Error: ${response.status} - ${resultText}`);
        }

        console.log(`✅ Success: Sucuri global cache has been cleared.`);
        console.log(`Sucuri Response: ${resultText.trim()}`);

    } catch (error) {
        console.error('\n❌ Script failed:', error.message);
        process.exit(1);
    }
}

main();