import BlockType, { BlockLanguageEnum } from '@interfaces/BlockType';
import PipelineType from '@interfaces/PipelineType';

// The named Python worker a block runs in (mage_ai/data_preparation/environments.py
// worker_selection): its own python_worker, else its pipeline's; null for MageML's Python
// and for blocks that are not Python.
export function blockPythonWorker(block: BlockType, pipeline?: PipelineType): string | null {
  if (block?.language !== BlockLanguageEnum.PYTHON) {
    return null;
  }
  const chosen = block?.configuration?.python_worker || pipeline?.environment?.worker;
  if (!chosen || ['mage', 'builtin'].includes(String(chosen).toLowerCase())) {
    return null;
  }

  return String(chosen);
}
