const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = Number(process.env.PORT) || 3001;
const HOST = '0.0.0.0';
const webDistPath = path.resolve(__dirname, '../../table-app/dist/table-app/browser');
const webIndexPath = path.join(webDistPath, 'index.html');

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/api/hello', (req, res) => {
  res.json({ message: 'Hello world!' });
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
