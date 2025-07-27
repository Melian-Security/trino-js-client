declare module 'lz4-napi' {
  export function uncompressSync(input: Buffer, maxOutputSize?: number): Buffer;
  export function compressSync(input: Buffer): Buffer;
  export function decompressFrameSync(input: Buffer): Buffer;
  export function compressFrameSync(input: Buffer): Buffer;
} 