import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const r = (path: string): string => fileURLToPath(new URL(path, import.meta.url));

export default defineConfig({
  // examples import rxjs-fp by its public names; tests run them against the sources
  resolve: {
    alias: [
      { find: /^rxjs-fp\/concurrency$/, replacement: r('src/vocabulary/concurrency.ts') },
      { find: /^rxjs-fp\/combination$/, replacement: r('src/vocabulary/combination.ts') },
      { find: /^rxjs-fp\/multicast$/, replacement: r('src/vocabulary/multicast.ts') },
      { find: /^rxjs-fp\/creation$/, replacement: r('src/vocabulary/creation.ts') },
      { find: /^rxjs-fp\/testing$/, replacement: r('src/testing/index.ts') },
      { find: /^rxjs-fp\/kernel$/, replacement: r('src/kernel.ts') },
      { find: /^rxjs-fp\/values$/, replacement: r('src/vocabulary/values.ts') },
      { find: /^rxjs-fp\/errors$/, replacement: r('src/vocabulary/errors.ts') },
      { find: /^rxjs-fp\/time$/, replacement: r('src/vocabulary/time.ts') },
      { find: /^rxjs-fp\/fp$/, replacement: r('src/fp/index.ts') },
      { find: /^rxjs-fp$/, replacement: r('src/index.ts') },
    ],
  },
  test: {
    include: ['test/**/*.test.ts', 'examples/**/*.test.ts'],
    environment: 'node',
  },
});
