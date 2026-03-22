import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@dnd-kit/core': fileURLToPath(new URL('./src/shims/dnd-core.js', import.meta.url)),
      '@dnd-kit/sortable': fileURLToPath(new URL('./src/shims/dnd-sortable.js', import.meta.url)),
      '@dnd-kit/utilities': fileURLToPath(new URL('./src/shims/dnd-utilities.js', import.meta.url)),
      dagre: fileURLToPath(new URL('./src/shims/dagre.js', import.meta.url)),
      reactflow: fileURLToPath(new URL('./src/shims/reactflow.jsx', import.meta.url)),
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.js',
    globals: true,
  },
})
