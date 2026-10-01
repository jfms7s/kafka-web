import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

// The /api proxy keeps the default changeOrigin=false: the backend only accepts a local Host
// header and a local Origin on writes, and 127.0.0.1:5173 is both.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: { '/api': { target: 'http://127.0.0.1:8000', ws: true } },
  },
  test: { environment: 'jsdom', setupFiles: 'src/test/setup.ts' },
})
