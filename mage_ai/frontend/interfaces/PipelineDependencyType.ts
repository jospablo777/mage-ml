export enum PipelineDependencyKindEnum {
  AFTER_RUN = 'after_run',
  API = 'api',
  DATA_PRODUCT = 'data_product',
}

export interface PipelineDependencyEdgeType {
  active?: boolean;
  block?: string;
  kind: PipelineDependencyKindEnum;
  label: string;
  source: string;
  target: string;
  trigger?: string;
  trigger_id?: number;
}

export interface PipelineDependencyNodeType {
  exists: boolean;
  last_run?: {
    completed_at?: string;
    id: number;
    started_at?: string;
    status: string;
  } | null;
  name: string;
  type?: string;
  uuid: string;
}

export default interface PipelineDependencyType {
  edges: PipelineDependencyEdgeType[];
  id: string;
  nodes: PipelineDependencyNodeType[];
  pipelines_without_dependencies: number;
}
