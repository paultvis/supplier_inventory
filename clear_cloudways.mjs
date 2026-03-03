import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        email: { type: 'string' },
        api_key: { type: 'string' },
        server_label: { type: 'string', default: 'Vision_HP' }, // Finds your server by name
        action: { type: 'string', default: 'purge_varnish' }    // purge_varnish OR restart_server
    },
    strict: false
});

if (!values.email || !values.api_key) {
    console.error('❌ Missing required parameters: --email and --api_key');
    process.exit(1);
}

const authUrl = 'https://api.cloudways.com/api/v1'; // Cloudways still uses v1 for OAuth
const apiUrl = 'https://api.cloudways.com/api/v2';  // Using v2 for the server actions

// --- 2. MAIN EXECUTION ---
async function main() {
    try {
        console.log('Authenticating with Cloudways API...');
        
        // STEP 1: Get OAuth Bearer Token
        const authParams = new URLSearchParams();
        authParams.append('email', values.email);
        authParams.append('api_key', values.api_key);

        const authResponse = await fetch(`${authUrl}/oauth/access_token`, {
            method: 'POST',
            body: authParams
        });

        if (!authResponse.ok) {
            throw new Error(`Authentication failed: ${authResponse.statusText}`);
        }

        const authData = await authResponse.json();
        const token = authData.access_token;
        console.log('✅ Authentication successful.');

        // STEP 2: Find the Server ID dynamically by Label
        console.log(`Searching for server labeled "${values.server_label}"...`);
        const serversResponse = await fetch(`${apiUrl}/server`, {
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
        });

        const serversData = await serversResponse.json();
        
        // The API returns an array of servers inside the "servers" object
        const serverList = serversData.servers || (serversData.server ? [serversData.server] : []);
        const targetServer = serverList.find(s => s.label === values.server_label);

        if (!targetServer) {
            throw new Error(`Could not find a server with the label "${values.server_label}". Available servers: ${serverList.map(s => s.label).join(', ')}`);
        }

        const serverId = targetServer.id;
        console.log(`✅ Found Server ID: ${serverId} for label "${values.server_label}".`);

        // STEP 3: Perform the Requested Action
        if (values.action === 'restart_server') {
            console.log(`Sending RESTART command to Server ID: ${serverId}...`);
            
            const restartParams = new URLSearchParams();
            restartParams.append('server_id', serverId);

            const restartResponse = await fetch(`${apiUrl}/server/restart`, {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${token}` },
                body: restartParams
            });

            if (!restartResponse.ok) throw new Error(`Server restart failed: ${await restartResponse.text()}`);
            console.log(`✅ Success: Server restart initiated. This may take a few minutes.`);

        } else if (values.action === 'purge_varnish') {
            console.log(`Sending PURGE VARNISH command to Server ID: ${serverId}...`);
            
            const purgeParams = new URLSearchParams();
            purgeParams.append('server_id', serverId);
            purgeParams.append('action', 'purge');

            const purgeResponse = await fetch(`${apiUrl}/service/varnish`, {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${token}` },
                body: purgeParams
            });

            if (!purgeResponse.ok) throw new Error(`Varnish purge failed: ${await purgeResponse.text()}`);
            console.log(`✅ Success: Cloudways Varnish cache has been purged.`);
            
        } else {
            throw new Error(`Unknown action: ${values.action}. Use 'purge_varnish' or 'restart_server'.`);
        }

    } catch (error) {
        console.error('\n❌ Script failed:', error.message);
        process.exit(1);
    }
}

main();