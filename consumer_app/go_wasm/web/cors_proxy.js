// Simple CORS proxy for gRPC-Web
const http = require('http');
const net = require('net');

// Target gRPC server
const target = {
  host: 'localhost',
  port: 50054
};

// Create proxy server
const server = http.createServer((req, res) => {
  // Set CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, grpc-timeout, x-grpc-web, x-user-agent');
  
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // Forward the request to the gRPC server
  const proxy = net.createConnection(target, () => {
    proxy.write(req.method + ' ' + req.url + ' HTTP/' + req.httpVersion + '\r\n');
    
    // Copy headers
    for (let header in req.headers) {
      proxy.write(header + ': ' + req.headers[header] + '\r\n');
    }
    
    proxy.write('\r\n');
    
    // Pipe request body to proxy
    req.pipe(proxy);
  });
  
  // Pipe proxy response back to client
  proxy.pipe(res);
  
  // Handle errors
  proxy.on('error', (err) => {
    console.error('Proxy error:', err);
    res.writeHead(502);
    res.end('Proxy error: ' + err.message);
  });
  
  req.on('error', (err) => {
    console.error('Request error:', err);
    proxy.end();
  });
});

// Start proxy server on port 8000
server.listen(8000, () => {
  console.log('Proxy server listening on port 8000');
});