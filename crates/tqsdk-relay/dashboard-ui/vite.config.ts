/// <reference types="vitest" />

import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  base: '/dashboard/',
  plugins: [svelte(), tailwindcss(), trimGeneratedTrailingWhitespace()],
  resolve: {
    conditions: ['browser'],
  },
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: {
      '/metrics': 'http://127.0.0.1:7789',
      '/symbol-metrics': 'http://127.0.0.1:7789',
    },
  },
  build: {
    outDir: '../src/dashboard-dist',
    emptyOutDir: true,
    cssCodeSplit: false,
    minify: false,
    rollupOptions: {
      output: {
        entryFileNames: 'assets/app.js',
        assetFileNames: (assetInfo) => {
          if (assetInfo.name?.endsWith('.css')) return 'assets/app.css';
          return 'assets/[name][extname]';
        },
        chunkFileNames: 'assets/chunk-[name].js',
      },
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    include: ['src/**/*.test.ts'],
  },
});

function trimGeneratedTrailingWhitespace() {
  return {
    name: 'trim-generated-trailing-whitespace',
    generateBundle(
      _options: unknown,
      bundle: Record<string, { type: string; code?: string; source?: string | Uint8Array }>,
    ) {
      for (const item of Object.values(bundle)) {
        if (item.type === 'chunk' && typeof item.code === 'string') {
          item.code = trimLineEndWhitespace(item.code);
        } else if (typeof item.source === 'string') {
          item.source = trimLineEndWhitespace(item.source);
        }
      }
    },
  };
}

function trimLineEndWhitespace(text: string): string {
  return text.replace(/[ \t]+(\r?\n)/g, '$1');
}
