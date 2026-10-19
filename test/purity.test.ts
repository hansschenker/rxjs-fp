import { describe, expect, it } from 'vitest';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import ts from 'typescript';

/**
 * The purity guard: the library source must not use object-oriented
 * machinery. Checked on the TypeScript AST (comments and strings ignored).
 *
 * Forbidden: `class`, `this`, `instanceof`, `.prototype`, `extends` in a
 * class sense, and `new` on anything other than platform value types.
 */
const ALLOWED_NEW = new Set(['Error', 'TypeError', 'Promise', 'Set', 'Map', 'Array', 'WeakMap', 'WeakSet']);

const files = (dir: string): string[] =>
  readdirSync(dir).flatMap((f) => {
    const p = join(dir, f);
    return statSync(p).isDirectory() ? files(p) : p.endsWith('.ts') ? [p] : [];
  });

const violations = (file: string): string[] => {
  const src = ts.createSourceFile(file, readFileSync(file, 'utf8'), ts.ScriptTarget.Latest, true);
  const out: string[] = [];
  const at = (n: ts.Node) => `${relative(process.cwd(), file)}:${src.getLineAndCharacterOfPosition(n.getStart()).line + 1}`;
  const visit = (n: ts.Node): void => {
    if (ts.isClassDeclaration(n) || ts.isClassExpression(n)) out.push(`${at(n)} class`);
    if (n.kind === ts.SyntaxKind.ThisKeyword || n.kind === ts.SyntaxKind.ThisType) out.push(`${at(n)} this`);
    if (ts.isBinaryExpression(n) && n.operatorToken.kind === ts.SyntaxKind.InstanceOfKeyword) out.push(`${at(n)} instanceof`);
    if (ts.isPropertyAccessExpression(n) && n.name.text === 'prototype') out.push(`${at(n)} .prototype`);
    if (ts.isNewExpression(n)) {
      const callee = n.expression.getText(src);
      if (!ALLOWED_NEW.has(callee)) out.push(`${at(n)} new ${callee}`);
    }
    ts.forEachChild(n, visit);
  };
  visit(src);
  return out;
};

describe('purity guard', () => {
  it('src/ and examples/ use no class, this, instanceof, prototype or custom `new`', () => {
    const all = [...files(join(process.cwd(), 'src')), ...files(join(process.cwd(), 'examples'))].flatMap(violations);
    expect(all).toEqual([]);
  });
});
