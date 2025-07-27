import axios from 'axios';

// Optional compression dependencies - gracefully handle missing packages
let zstdDecompress: ((data: Uint8Array) => ArrayBuffer) | null = null;
let lz4: any = null;

// Load optional dependencies at runtime
(async () => {
  try {
    const fzstd = await import('fzstd');
    zstdDecompress = fzstd.decompress;
  } catch (e) {
    // fzstd not available
  }

  try {
    lz4 = await import('lz4');
  } catch (e) {
    // lz4 not available
  }
})();

// Encoding types
export type Encoding = 'json' | 'json+lz4' | 'json+zstd';

// Segment metadata
export interface SegmentMetadata {
  uncompressedSize?: string;
  segmentSize: string;
}

// Base segment interface
export interface BaseSegment {
  type: SegmentType;
  metadata: SegmentMetadata;
}

// Inline segment (data embedded in response)
export interface InlineSegment extends BaseSegment {
  type: SegmentType.INLINE;
  data: string; // base64 encoded
}

// Spooled segment (data fetched from URI)
export interface SpooledSegment extends BaseSegment {
  type: SegmentType.SPOOLED;
  uri: string;
  ackUri: string;
  headers?: Record<string, string[]>;
}

// Union type for segments
export type Segment = InlineSegment | SpooledSegment;

// Spooled protocol response
export interface SpooledProtocolResponse {
  encoding: Encoding;
  segments: Segment[];
}

// Segment type enum
export enum SegmentType {
  INLINE = 'inline',
  SPOOLED = 'spooled'
}

// Segment wrapper interface for processing
export interface SegmentWrapper {
  type: 'inline' | 'spooled';
  segment: Segment;
  encoding: Encoding;
}

// Type for query result rows
export type QueryRow = any[]; // Array of column values
export type QueryRows = QueryRow[];

/**
 * Utilities for processing spooled protocol data
 */
export class SpoolingProcessor {
  /**
   * Check if a query result contains spooled data
   */
  static isSpoolingResponse(result: any): boolean {
    return !!(result && typeof result === 'object' && 'encoding' in result && 'segments' in result);
  }

  /**
   * Convert spooled protocol response to segments
   */
  static toSegments(rows: SpooledProtocolResponse): SegmentWrapper[] {
    const encoding = rows.encoding;
    const segments: SegmentWrapper[] = [];

    for (const segment of rows.segments) {
      const segmentType = segment.type;
      
      if (segmentType === SegmentType.INLINE) {
        segments.push({
          type: 'inline',
          segment: segment as InlineSegment,
          encoding
        });
      } else if (segmentType === SegmentType.SPOOLED) {
        segments.push({
          type: 'spooled',
          segment: segment as SpooledSegment,
          encoding,
        });
      } else {
        throw new Error(`Unsupported segment type: ${segmentType}`);
      }
    }

    return segments;
  }

  /**
   * Process all segments and return combined rows
   */
  static async processSegments(segments: SegmentWrapper[]): Promise<QueryRows> {
    const allRows: QueryRows = [];

    for (const segmentWrapper of segments) {
      try {
        if (segmentWrapper.type === 'inline') {
          // Process inline segment
          const inlineSegment = segmentWrapper.segment as InlineSegment;
          const decodedData = Buffer.from(inlineSegment.data, 'base64');
          const rows = this.decodeSegmentData(decodedData, inlineSegment.metadata, segmentWrapper.encoding);
          if (Array.isArray(rows)) {
            allRows.push(...rows);
          }
        } else if (segmentWrapper.type === 'spooled') {
          // Process spooled segment
          const spooledSegment = segmentWrapper.segment as SpooledSegment;
          
          // Convert multi-value headers to single values (take first value)
          const requestHeaders: Record<string, string> = {};
          if (spooledSegment.headers) {
            for (const [key, values] of Object.entries(spooledSegment.headers)) {
              if (values.length > 1) {
                throw new Error(`Header '${key}' contains multiple values: ${values}`);
              }
              requestHeaders[key] = values[0];
            }
          }
          
          // Make direct HTTP request to external storage (S3, etc.) - bypass coordinator
          const response = await axios.get(spooledSegment.uri, {
            headers: requestHeaders,
            responseType: 'arraybuffer' // Ensure we get binary data
          });
          
          let data = response.data;
          if (data instanceof ArrayBuffer) {
            data = new Uint8Array(data);
          } else if (typeof data === 'string') {
            data = new TextEncoder().encode(data);
          }

          const rows = this.decodeSegmentData(data, spooledSegment.metadata, segmentWrapper.encoding);
          if (Array.isArray(rows)) {
            allRows.push(...rows);
          }

          // Acknowledge the segment (fire and forget) - direct axios call
          axios.get(spooledSegment.ackUri, {
            timeout: 2000 // 2 second timeout for acknowledgments
          }).catch((error: Error) => {
            console.warn('Failed to acknowledge segment:', error);
          });
        }
      } catch (error) {
        console.error('Failed to process segment:', error);
      }
    }

    return allRows;
  }

  /**
   * Decode segment data based on encoding (handles compression)
   */
  static decodeSegmentData(data: Uint8Array | Buffer, metadata: SegmentMetadata, encoding: string): QueryRows {
    let decodedData: Uint8Array;

    if ('uncompressedSize' in metadata && metadata.uncompressedSize) {
      // Data is compressed
      const expectedCompressedSize = Number(metadata.segmentSize);
      if (data.length !== expectedCompressedSize) {
        throw new Error(`Expected to read ${expectedCompressedSize} bytes but got ${data.length}`);
      }

      decodedData = this.decompressData(data, metadata, encoding);

      const expectedUncompressedSize = Number(metadata.uncompressedSize);
      if (decodedData.length !== expectedUncompressedSize) {
        throw new Error(
          `Decompressed size does not match expected segment size, ` +
          `expected ${expectedUncompressedSize}, got ${decodedData.length}`
        );
      }
    } else {
      // Data not compressed - below threshold
      decodedData = data instanceof Buffer ? new Uint8Array(data) : data;
    }

    // Parse JSON data
    const jsonString = new TextDecoder().decode(decodedData);
    return JSON.parse(jsonString);
  }

  /**
   * Decompress data based on encoding
   */
  static decompressData(data: Uint8Array | Buffer, _metadata: SegmentMetadata, encoding: string): Uint8Array {
    const dataArray = data instanceof Buffer ? new Uint8Array(data) : data;

    switch (encoding) {
      case 'json+zstd':
        if (!zstdDecompress) {
          throw new Error('ZStandard decompression not available. Install "fzstd" package: npm install fzstd');
        }
        try {
          return new Uint8Array(zstdDecompress(dataArray));
        } catch (error) {
          throw new Error(`ZStandard decompression failed: ${error}`);
        }
      
      case 'json+lz4': {
        if (!lz4) {
          throw new Error('LZ4 decompression not available. Install "lz4" package: npm install lz4');
        }
        // Trino sends raw LZ4 block data, use decodeBlock for raw blocks
        const uncompressedSize = parseInt(_metadata.uncompressedSize || '0');
        if (uncompressedSize > 0) {
          // Create output buffer with known uncompressed size
          const output = Buffer.alloc(uncompressedSize);
          // Use lz4.decodeBlock for raw block decompression
          const actualSize = lz4.decodeBlock(Buffer.from(dataArray), output);
          if (actualSize > 0) {
            return output.subarray(0, actualSize);
          } else {
            throw new Error(`LZ4 decodeBlock failed, returned: ${actualSize}`);
          }
        } else {
          throw new Error('Unknown uncompressed size for LZ4 data');
        }
      }
        
      
      case 'json':
        // No compression
        return dataArray;
      
      default:
        throw new Error(`Unsupported encoding: ${encoding}`);
    }
  }
}
