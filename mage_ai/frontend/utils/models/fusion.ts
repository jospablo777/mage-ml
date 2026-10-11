import type PipelineType from '@interfaces/PipelineType';

export const BLOCK_FUSION_CHAINS = 'chains';
export const BLOCK_FUSION_OFF = 'off';

// Production runs fuse chains of blocks unless the pipeline turns it off; an unset setting
// means the server's default (chains).
export function blockFusionOn(pipeline: PipelineType): boolean {
  const mode = pipeline?.block_fusion || pipeline?.block_fusion_default || BLOCK_FUSION_CHAINS;
  return mode === BLOCK_FUSION_CHAINS;
}

export type FusionStagePositionType = {
  position: number;
  size: number;
  stage: number;
};

// The chains of blocks that run as one stage when block fusion is on, numbered from 1 in
// the order of the plan; blocks that run alone are left out.
export function fusionStages(pipeline: PipelineType): string[][] {
  if (!blockFusionOn(pipeline)) {
    return [];
  }

  return (pipeline?.fusion_plan || []).filter(stage => stage?.length > 1);
}

export function fusionStagePositions(pipeline: PipelineType): {
  [blockUUID: string]: FusionStagePositionType;
} {
  const positions = {};
  fusionStages(pipeline).forEach((stage, index) => {
    stage.forEach((uuid, position) => {
      positions[uuid] = {
        position: position + 1,
        size: stage.length,
        stage: index + 1,
      };
    });
  });

  return positions;
}

export function fusionStageText(position?: FusionStagePositionType): string {
  if (!position) {
    return '';
  }

  return `stage ${position.stage} (${position.position} of ${position.size})`;
}
