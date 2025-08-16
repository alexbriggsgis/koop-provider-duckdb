const path = require('path');

function handleRequest(req, res) {
  const indexPath = path.join(__dirname, 'index.html');
  res.sendFile(indexPath);
}

module.exports = handleRequest
