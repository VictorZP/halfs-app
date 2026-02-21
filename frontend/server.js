import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { join, extname } from 'node:path';
import { existsSync } from 'node:fs';

const PORT = process.env.PORT || 3000;
const DIST = join(process.cwd(), 'dist');

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.json': 'application/json',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif':  'image/gif',
  '.svg':  'image/svg+xml',
  '.ico':  'image/x-icon',
  '.woff': 'font/woff',
  '.woff2':'font/woff2',
  '.ttf':  'font/ttf',
  '.webp': 'image/webp',
};

async function serveFile(res, filePath) {
  const ext = extname(filePath);
  const contentType = MIME_TYPES[ext] || 'application/octet-stream';
  const data = await readFile(filePath);
  res.writeHead(200, { 'Content-Type': contentType });
  res.end(data);
}

createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://localhost:${PORT}`);
    let filePath = join(DIST, url.pathname);

    if (existsSync(filePath) && filePath.startsWith(DIST) && extname(filePath)) {
      await serveFile(res, filePath);
    } else {
      await serveFile(res, join(DIST, 'index.html'));
    }
  } catch {
    res.writeHead(500);
    res.end('Internal Server Error');
  }
}).listen(PORT, '0.0.0.0', () => {
  console.log(`Frontend serving on port ${PORT}`);
});
