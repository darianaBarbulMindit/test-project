function getDatabricksHost() {
  return (
    process.env.DATABRICKS_SERVER_HOSTNAME || process.env.DATABRICKS_HOST || ''
  );
}

function getDatabricksWarehouseHttpPath() {
  if (process.env.DATABRICKS_HTTP_PATH) return process.env.DATABRICKS_HTTP_PATH;
  if (process.env.DATABRICKS_SQL_HTTP_PATH)
    return process.env.DATABRICKS_SQL_HTTP_PATH;
  if (process.env.DATABRICKS_WAREHOUSE_ID)
    return `/sql/1.0/warehouses/${process.env.DATABRICKS_WAREHOUSE_ID}`;
  return '';
}

function getDatabricksToken(req) {
  const forwardedAccessToken = req.headers['x-forwarded-access-token'];
  if (
    typeof forwardedAccessToken === 'string' &&
    forwardedAccessToken.length > 0
  ) {
    return { token: forwardedAccessToken, source: 'x-forwarded-access-token' };
  }

  if (
    typeof process.env.DATABRICKS_TOKEN === 'string' &&
    process.env.DATABRICKS_TOKEN.length > 0
  ) {
    return { token: process.env.DATABRICKS_TOKEN, source: 'DATABRICKS_TOKEN' };
  }

  return { token: '', source: null };
}

let _spTokenCache = null;

async function getServicePrincipalToken() {
  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;
  const host = getDatabricksHost();

  if (!clientId || !clientSecret || !host) return null;

  const now = Date.now();
  if (_spTokenCache && _spTokenCache.expiresAt > now + 60_000) {
    return _spTokenCache.token;
  }

  const response = await fetch(`https://${host}/oidc/v1/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: clientId,
      client_secret: clientSecret,
      scope: 'all-apis',
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to obtain service principal token (${response.status}): ${text}`);
  }

  const data = await response.json();
  _spTokenCache = {
    token: data.access_token,
    expiresAt: now + (data.expires_in ?? 3600) * 1000,
  };
  return _spTokenCache.token;
}

async function getSqlToken(req) {
  if (
    typeof process.env.DATABRICKS_TOKEN === 'string' &&
    process.env.DATABRICKS_TOKEN.length > 0
  ) {
    return { token: process.env.DATABRICKS_TOKEN, source: 'DATABRICKS_TOKEN' };
  }

  const forwarded = getDatabricksToken(req);
  if (forwarded.token) return forwarded;

  try {
    const spToken = await getServicePrincipalToken();
    if (spToken) return { token: spToken, source: 'client_credentials' };
  } catch (err) {
    console.warn('Service principal token fetch failed:', err.message);
  }

  return { token: '', source: null };
}

function isAllowedReadOnlyStatement(statement) {
  if (typeof statement !== 'string') {
    return false;
  }

  const normalized = statement.trim().replace(/;+\s*$/, '');
  if (normalized.length === 0) {
    return false;
  }

  if (normalized.includes(';')) {
    return false;
  }

  const upper = normalized.toUpperCase();
  return (
    upper.startsWith('SELECT ') ||
    upper.startsWith('WITH ') ||
    upper.startsWith('SHOW ') ||
    upper.startsWith('DESCRIBE ') ||
    upper.startsWith('EXPLAIN ')
  );
}

async function executeSqlStatement({ host, httpPath, token, statement }) {
  const warehouseId = httpPath.split('/').pop();

  const response = await fetch(`https://${host}/api/2.0/sql/statements`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      statement,
      warehouse_id: warehouseId,
      wait_timeout: '10s',
      disposition: 'INLINE',
    }),
  });

  const text = await response.text();
  let result;
  try {
    result = JSON.parse(text);
  } catch {
    throw new Error(`SQL execution failed (${response.status}): ${text}`);
  }
  if (!response.ok || result.status?.state === 'FAILED') {
    throw new Error(result.status?.error?.message || `SQL execution failed: ${response.status}`);
  }

  const cols = result.manifest?.schema?.columns ?? [];
  const dataArray = result.result?.data_array ?? [];
  return dataArray.map(row =>
    Object.fromEntries(row.map((val, i) => [cols[i].name, val]))
  );
}

async function fetchCurrentUserFromDatabricks(host, accessToken) {
  const endpointPath = '/api/2.0/preview/scim/v2/Me';
  const response = await fetch(`https://${host}${endpointPath}`, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  });

  if (response.ok) {
    const payload = await response.json();
    return { payload, source: endpointPath };
  }

  const body = await response.text();
  throw new Error(
    `Databricks user endpoint failed: ${endpointPath} -> ${response.status} ${body}`,
  );
}

async function runDatabricksJob(host, accessToken, jobId) {
  const endpointPath = '/api/2.0/jobs/run-now';
  const response = await fetch(`https://${host}${endpointPath}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ job_id: jobId }),
  });

  if (response.ok) {
    const payload = await response.json();
    return { payload, source: endpointPath };
  }

  const body = await response.text();
  throw new Error(
    `Databricks run job endpoint failed: ${endpointPath} -> ${response.status} ${body}`,
  );
}

module.exports = {
  getDatabricksHost,
  getDatabricksWarehouseHttpPath,
  getDatabricksToken,
  getSqlToken,
  isAllowedReadOnlyStatement,
  executeSqlStatement,
  fetchCurrentUserFromDatabricks,
  runDatabricksJob,
};
