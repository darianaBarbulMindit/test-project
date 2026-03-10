const express = require('express');
const fs = require('fs');
const path = require('path');
const { DBSQLClient } = require('@databricks/sql');

const app = express();
const PORT = Number(process.env.PORT) || 3001;
const HOST = '0.0.0.0';
const webDistPath = path.resolve(__dirname, '../../table-app/dist/table-app/browser');
const webIndexPath = path.join(webDistPath, 'index.html');

async function fetchCurrentDatabricksUser() {
  const host = process.env.DATABRICKS_SERVER_HOSTNAME;
  const httpPath = process.env.DATABRICKS_HTTP_PATH;
  const token = process.env.DATABRICKS_TOKEN;

  if (!host || !httpPath || !token) {
    throw new Error(
      'Missing Databricks settings: DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN'
    );
  }

  const client = new DBSQLClient();
  await client.connect({
    host,
    path: httpPath,
    token
  });

  const session = await client.openSession();

  try {
    const operation = await session.executeStatement(`
      SELECT
        current_user() AS current_user,
        current_catalog() AS current_catalog,
        current_schema() AS current_schema
    `);

    try {
      const rows = await operation.fetchAll();
      return rows[0] ?? null;
    } finally {
      await operation.close();
    }
  } finally {
    await session.close();
    await client.close();
  }
}

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/api/hello', (req, res) => {
  res.json({ message: 'Hello world!' });
});

app.get('/api/databricks/current-user', async (req, res) => {
  try {
    const userInfo = await fetchCurrentDatabricksUser();
    console.log('Databricks current user details:', userInfo);
    res.json({ user: userInfo });
  } catch (error) {
    console.error('Databricks query failed:', error.message);
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
    `Frontend build not found at ${webDistPath}. Run "npm run build" from repository root.`
  );
}

app.listen(PORT, HOST, () => {
  console.log(`Server running on http://${HOST}:${PORT}`);
});
