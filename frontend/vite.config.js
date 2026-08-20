import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
// The backend has no CORS middleware, so requests from the Vite dev server
// (a different origin) would be blocked by the browser and its HttpOnly
// auth cookies couldn't be sent cross-site. Proxying keeps the frontend and
// backend same-origin during development without any backend changes.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/auth': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/analysis': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/analyses': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
