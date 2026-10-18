// A streaming pipeline's sample messages, kept next to the pipeline as JSON lines.
interface StreamSampleType {
  message_count?: number;
  messages?: any[];
  name: string;
  // Messages read per topic partition, after a capture.
  partitions?: { [partition: string]: number };
  // Fields replaced by <redacted> on capture, with the messages each was in.
  redacted?: { [path: string]: number };
  size_bytes?: number;
  updated_at?: string;
}

// Blocks run on a sample with the service engine's Python worker.
export interface StreamSampleRunType {
  block_uuid: string;
  blocks?: { block_uuid: string; seconds: number }[];
  error?: { block_uuid?: string; details?: string; message?: string };
  failing_message?: { index: number; message: any };
  input_messages: number;
  logs?: { block_uuid?: string; line: string }[];
  messages?: any[];
  outputs?: { [blockUUID: string]: any[] };
  sample?: string;
  seconds: number;
}

export default StreamSampleType;
