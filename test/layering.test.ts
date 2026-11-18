import { describe, expect, it } from 'vitest';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import ts from 'typescript';
import * as root from '../src/index.js';
import * as kernel from '../src/kernel.js';
import * as concurrency from '../src/vocabulary/concurrency.js';

/**
 * The layering guard (docs/kernel.md):
 *
 * 1. Vocabulary modules import only from the kernel entry point (or from
 *    another vocabulary module). Type imports count too.
 * 2. The kernel never reaches the vocabulary, directly or transitively.
 * 3. The root entry point exports the very same functions as the family
 *    modules, so importing by family or from the root is interchangeable.
 */
const SRC = resolve('src');
const VOCAB = join(SRC, 'vocabulary');

const specifiers = (file: string): string[] => {
  const src = ts.createSourceFile(file, readFileSync(file, 'utf8'), ts.ScriptTarget.Latest, true);
  const out: string[] = [];
  src.forEachChild((n) => {
    if ((ts.isImportDeclaration(n) || ts.isExportDeclaration(n)) && n.moduleSpecifier && ts.isStringLiteral(n.moduleSpecifier))
      out.push(n.moduleSpecifier.text);
  });
  return out;
};

const toFile = (from: string, spec: string): string | null =>
  spec.startsWith('.') ? resolve(dirname(from), spec.replace(/\.js$/, '.ts')) : null;

const vocabularyFiles = readdirSync(VOCAB)
  .filter((f) => f.endsWith('.ts'))
  .map((f) => join(VOCAB, f));

describe('layering guard', () => {
  it('finds the vocabulary modules', () => {
    expect(vocabularyFiles.length).toBeGreaterThan(0);
  });

  it.each(vocabularyFiles.map((f) => [relative(SRC, f), f]))('%s imports only from the kernel', (_, file) => {
    const bad = specifiers(file).filter((s) => s !== '../kernel.js' && !/^\.\/[\w-]+\.js$/.test(s));
    expect(bad).toEqual([]);
  });

  it('the kernel never reaches the vocabulary', () => {
    const seen = new Set<string>();
    const walk = (file: string): void => {
      if (seen.has(file) || !existsSync(file)) return;
      seen.add(file);
      specifiers(file)
        .map((s) => toFile(file, s))
        .forEach((f) => f && walk(f));
    };
    walk(join(SRC, 'kernel.ts'));
    expect([...seen].filter((f) => f.startsWith(VOCAB)).map((f) => relative(SRC, f))).toEqual([]);
  });

  it('family modules and the root export the same functions', () => {
    const differ = Object.entries({ ...kernel, ...concurrency }).filter(
      ([name, value]) => (root as Record<string, unknown>)[name] !== value,
    );
    expect(differ.map(([n]) => n)).toEqual([]);
  });
});
