export enum AuditOutcomeEnum {
  FAILED = 'failed',
  REFUSED = 'refused',
  SUCCEEDED = 'succeeded',
}

export default interface AuditEventType {
  action: 'create' | 'update' | 'delete';
  changes?: { [attribute: string]: any } | null;
  count: number;
  created_at: string;
  error?: string | null;
  id: number;
  last_at?: string | null;
  outcome: AuditOutcomeEnum;
  parent?: string | null;
  parent_id?: string | null;
  object_id?: string | null;
  object_type: string;
  source_ip?: string | null;
  user_id?: number | null;
  user_name?: string | null;
}
