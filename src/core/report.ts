/**
 * Errors that have nowhere to go (an error notification without an error
 * handler, or a consumer callback that throws) are rethrown on a fresh
 * call stack, like RxJS does. They never disturb the running pipeline.
 */
export const reportUnhandledError = (err: unknown): void => {
  setTimeout(() => {
    throw err;
  });
};
