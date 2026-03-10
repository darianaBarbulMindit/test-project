const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = Number(process.env.PORT) || 3001;
const HOST = '0.0.0.0';
const webDistPath = path.resolve(__dirname, '../../table-app/dist/table-app/browser');
const webIndexPath = path.join(webDistPath, 'index.html');

function getDatabricksHost() {
  return process.env.DATABRICKS_SERVER_HOSTNAME || process.env.DATABRICKS_HOST || '';
}

async function fetchCurrentUserFromDatabricks(host, accessToken) {
  const endpointPath = '/api/2.0/preview/scim/v2/Me';
  const response = await fetch(`https://${host}${endpointPath}`, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${accessToken}`
    }
  });

  if (response.ok) {
    const payload = await response.json();
    return { payload, source: endpointPath };
  }

  const body = await response.text();
  throw new Error(`Databricks user endpoint failed: ${endpointPath} -> ${response.status} ${body}`);
}

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/api/hello', (req, res) => {
  res.json({ message: 'Hello world!' });
});

app.get('/api/databricks/current-user', async (req, res) => {
  const forwardedUser = req.headers['x-forwarded-user'] || null;
  const forwardedEmail = req.headers['x-forwarded-email'] || null;
  const forwardedPreferredUsername = req.headers['x-forwarded-preferred-username'] || null;
  const forwardedAccessToken = req.headers['x-forwarded-access-token'];
  const host = getDatabricksHost();

  let userInfo = null;
  let userInfoSource = null;
  let userInfoError = null;

  if (host && typeof forwardedAccessToken === 'string' && forwardedAccessToken.length > 0) {
    try {
      const result = await fetchCurrentUserFromDatabricks(host, forwardedAccessToken);
      userInfo = result.payload;
      userInfoSource = result.source;
    } catch (error) {
      userInfoError = error.message;
      console.warn('Databricks user info lookup failed, falling back to forwarded headers:', error.message);
    }
  }

  const payload = {
    user: userInfo,
    userInfoSource,
    userInfoError,
    forwardedIdentity: {
      user: forwardedUser,
      email: forwardedEmail,
      preferredUsername: forwardedPreferredUsername
    },
    mode:
      userInfo !== null
        ? 'obo_user_token'
        : 'forwarded_headers_only'
  };

  console.log('Databricks current user details:', payload);
  res.json(payload);
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
