const express = require('express');
const fs = require('fs');
const path = require('path');
const {
  getDatabricksHost,
  getDatabricksWarehouseHttpPath,
  getDatabricksToken,
  getSqlToken,
  executeSqlStatement,
  fetchCurrentUserFromDatabricks,
  runDatabricksJob,
} = require('./databricks');

const app = express();
app.use(express.json({ limit: '1mb' }));
const PORT = Number(process.env.PORT) || 3001;
const HOST = '0.0.0.0';
const webDistPath = path.resolve(
  __dirname,
  '../../table-app/dist/table-app/browser',
);
const webIndexPath = path.join(webDistPath, 'index.html');

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/api/debug/env', (_req, res) => {
  const databricksEnv = Object.fromEntries(
    Object.entries(process.env).filter(
      ([key]) =>
        key.toUpperCase().includes('DATABRICKS') ||
        key.toUpperCase().includes('WAREHOUSE'),
    ),
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
    const tokenInfo = await getSqlToken(req);

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

app.get('/api/databricks/unity-catalog/persons', async (req, res) => {
  try {
    const host = getDatabricksHost();
    const httpPath = getDatabricksWarehouseHttpPath();
    const tokenInfo = await getSqlToken(req);

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
