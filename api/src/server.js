const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json({ limit: '1mb' }));
const PORT = Number(process.env.PORT) || 3001;
const HOST = '0.0.0.0';
const webDistPath = path.resolve(
  __dirname,
  '../../table-app/dist/table-app/browser',
);
const webIndexPath = path.join(webDistPath, 'index.html');

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

  try {
    const spToken = await getServicePrincipalToken();
    if (spToken) return { token: spToken, source: 'client_credentials' };
  } catch (err) {
    console.warn('Service principal token fetch failed, falling back to forwarded token:', err.message);
  }

  return getDatabricksToken(req);
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

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/api/debug/env', (_req, res) => {
  const databricksEnv = Object.fromEntries(
    Object.entries(process.env).filter(([key]) =>
      key.toUpperCase().includes('DATABRICKS') || key.toUpperCase().includes('WAREHOUSE')
    )
  );
  res.json(databricksEnv);
});

app.get('/api/hello', (req, res) => {
  res.json({ message: 'Hello world!' });
});

app.get('/api/databricks/current-user', async (req, res) => {
  const forwardedUser = req.headers['x-forwarded-user'] || null;
  const forwardedEmail = req.headers['x-forwarded-email'] || null;
  const forwardedPreferredUsername =
    req.headers['x-forwarded-preferred-username'] || null;
  const forwardedAccessToken = req.headers['x-forwarded-access-token'];
  const host = getDatabricksHost();

  let userInfo = null;
  let userInfoSource = null;
  let userInfoError = null;

  if (
    host &&
    typeof forwardedAccessToken === 'string' &&
    forwardedAccessToken.length > 0
  ) {
    try {
      const result = await fetchCurrentUserFromDatabricks(
        host,
        forwardedAccessToken,
      );
      userInfo = result.payload;
      userInfoSource = result.source;
    } catch (error) {
      userInfoError = error.message;
      console.warn(
        'Databricks user info lookup failed, falling back to forwarded headers:',
        error.message,
      );
    }
  }

  const payload = {
    user: userInfo,
    userInfoSource,
    userInfoError,
    forwardedIdentity: {
      user: forwardedUser,
      email: forwardedEmail,
      preferredUsername: forwardedPreferredUsername,
    },
    mode: userInfo !== null ? 'obo_user_token' : 'forwarded_headers_only',
  };

  console.log('Databricks current user details:', payload);
  res.json(payload);
});

app.get('/api/databricks/unity-catalog/catalogs', async (req, res) => {
  try {
    const host = getDatabricksHost();
    const httpPath = getDatabricksWarehouseHttpPath();
    const tokenInfo = getSqlToken(req);

    if (!host || !httpPath || !tokenInfo.token) {
      res.status(400).json({
        error:
          'Missing Databricks SQL configuration. Required: DATABRICKS_HOST, DATABRICKS_HTTP_PATH, and token (x-forwarded-access-token or DATABRICKS_TOKEN).',
      });
      return;
    }

    const rows = await executeSqlStatement({
      host,
      httpPath,
      token: tokenInfo.token,
      statement: 'SHOW CATALOGS',
    });

    res.json({
      rows,
      rowCount: rows.length,
      tokenSource: tokenInfo.source,
      statement: 'SHOW CATALOGS',
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/databricks/query', async (req, res) => {
  try {
    const host = getDatabricksHost();
    const httpPath = getDatabricksWarehouseHttpPath();
    const tokenInfo = getSqlToken(req);
    const statement = req.body?.statement;

    if (!host || !httpPath || !tokenInfo.token) {
      res.status(400).json({
        error:
          'Missing Databricks SQL configuration. Required: DATABRICKS_HOST, DATABRICKS_HTTP_PATH, and token (x-forwarded-access-token or DATABRICKS_TOKEN).',
      });
      return;
    }

    if (!isAllowedReadOnlyStatement(statement)) {
      res.status(400).json({
        error:
          'Only single read-only statements are allowed (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN).',
      });
      return;
    }

    const rows = await executeSqlStatement({
      host,
      httpPath,
      token: tokenInfo.token,
      statement,
    });

    res.json({
      rows,
      rowCount: rows.length,
      tokenSource: tokenInfo.source,
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/databricks/unity-catalog/persons', async (req, res) => {
  try {
    const host = getDatabricksHost();
    const httpPath = getDatabricksWarehouseHttpPath();
    const tokenInfo = getSqlToken(req);

    if (!host || !httpPath || !tokenInfo.token) {
      res.status(400).json({
        error:
          'Missing Databricks SQL configuration. Required: DATABRICKS_HOST, DATABRICKS_HTTP_PATH, and token (x-forwarded-access-token or DATABRICKS_TOKEN).',
      });
      return;
    }

    const catalog = process.env.DATABRICKS_CATALOG;
    const schema = process.env.DATABRICKS_SCHEMA;

    if (!catalog || !schema) {
      res.status(400).json({
        error:
          'Missing table configuration. Required: DATABRICKS_CATALOG and DATABRICKS_SCHEMA environment variables.',
      });
      return;
    }

    const statement = `SELECT id, name, role, email FROM \`${catalog}\`.\`${schema}\`.\`person\``;

    const rows = await executeSqlStatement({
      host,
      httpPath,
      token: tokenInfo.token,
      statement,
    });

    res.json({
      rows,
      rowCount: rows.length,
      tokenSource: tokenInfo.source,
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/databricks/jobs/run', async (req, res) => {
  try {
    const host = getDatabricksHost();
    const tokenInfo = getDatabricksToken(req);
    const jobId = req.body?.job_id;

    if (!host || !tokenInfo.token) {
      res.status(400).json({
        error:
          'Missing Databricks configuration. Required: DATABRICKS_HOST and token (x-forwarded-access-token or DATABRICKS_TOKEN).',
      });
      return;
    }

    if (!jobId) {
      res.status(400).json({
        error: 'job_id is required in the request body.',
      });
      return;
    }

    const result = await runDatabricksJob(host, tokenInfo.token, jobId);

    res.json({
      ...result.payload,
      tokenSource: tokenInfo.source,
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

if (fs.existsSync(webIndexPath)) {
  app.use(express.static(webDistPath));

  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api/')) {
      next();
      return;
    }
    res.sendFile(webIndexPath);
  });
} else {
  console.warn(
    `Frontend build not found at ${webDistPath}. Run "npm run build" from repository root.`,
  );
}

app.listen(PORT, HOST, () => {
  console.log(`Server running on http://${HOST}:${PORT}`);
});
