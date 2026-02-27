const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Serve static Angular files
app.use(express.static(path.join(__dirname, 'dist/table-app/browser')));

// For Angular routing
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'dist/table-app/browser/index.html'));
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
