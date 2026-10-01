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
  test: {
    environment: 'jsdom',
    setupFiles: 'src/test/setup.ts',
    // Tests that convert local date-times pin the zone: UTC+5:30 with no DST, so a conversion
    // bug cannot hide behind a host that happens to run in UTC.
    env: { TZ: 'Asia/Kolkata' },
  },
})
