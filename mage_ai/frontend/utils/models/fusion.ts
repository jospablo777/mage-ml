import type PipelineType from '@interfaces/PipelineType';

export const BLOCK_FUSION_CHAINS = 'chains';

export type FusionStagePositionType = {
  position: number;
  size: number;
  stage: number;
};

// The chains of blocks that run as one stage when block fusion is on, numbered from 1 in
// the order of the plan; blocks that run alone are left out.
export function fusionStages(pipeline: PipelineType): string[][] {
  if (pipeline?.block_fusion !== BLOCK_FUSION_CHAINS) {
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
