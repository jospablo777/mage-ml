import styled from 'styled-components';
import { useRef, useState } from 'react';

import BlockType, { BlockLanguageEnum, BlockTypeEnum } from '@interfaces/BlockType';
import FlyoutMenuWrapper from '@oracle/components/FlyoutMenu/FlyoutMenuWrapper';
import PipelineType from '@interfaces/PipelineType';
import Tooltip from '@oracle/components/Tooltip';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { PythonWorkerType } from '@components/PipelineDetail/Settings/PythonWorker';
import { UNIT } from '@oracle/styles/units/spacing';
import { blockPythonWorker } from '@utils/models/pythonWorker';

// The Python blocks a worker can run (environments.SUPPORTED_BLOCK_TYPES).
export const WORKER_BLOCK_TYPES = [
  BlockTypeEnum.CUSTOM,
  BlockTypeEnum.DATA_EXPORTER,
  BlockTypeEnum.DATA_LOADER,
  BlockTypeEnum.TRANSFORMER,
];

export function canChooseWorker(block: BlockType): boolean {
  return block?.language === BlockLanguageEnum.PYTHON && WORKER_BLOCK_TYPES.includes(block?.type);
}

const ChipStyle = styled.button<{ builtin?: boolean; warning?: boolean }>`
  align-items: center;
  background: none;
  border-radius: ${BORDER_RADIUS_SMALL}px;
  cursor: pointer;
  display: inline-flex;
  font-size: 12px;
  gap: ${UNIT / 2}px;
  line-height: 18px;
  // A long name does not push the header's buttons out: the tooltip has all of it.
  max-width: ${UNIT * 24}px;
  overflow: hidden;
  padding: 0 ${UNIT * 0.75}px;
  text-overflow: ellipsis;
  white-space: nowrap;

  ${props => `
    border: 1px ${props.builtin ? 'dashed' : 'solid'} ${props.warning
      ? (props.theme.accent || dark.accent).warning
      : (props.theme.borders || dark.borders).medium};
    color: ${props.warning
      ? (props.theme.accent || dark.accent).warning
      : (props.theme.content || dark.content).muted};
  `}

  &:hover, &:focus-visible {
    ${props => `border-color: ${(props.theme.accent || dark.accent).sky};`}
  }
`;

// Where a Python block runs, in its header, and the place to change it: MageML's Python, the
// pipeline's default worker or one of the project's workers. A worker that is not
// prepared, or that the project does not have, shows as a warning: the block fails.
function WorkerChip({ block, onChange, pipeline }: {
  block: BlockType;
  onChange: (pythonWorker: string | null) => void;
  pipeline: PipelineType;
}) {
  const ref = useRef(null);
  const [open, setOpen] = useState(false);
  // One request for every block of the page: SWR shares it by its key.
  const { data } = api.python_workers.list({}, { revalidateOnFocus: false });
  const workers: PythonWorkerType[] = data?.python_workers || [];
  const worker = blockPythonWorker(block, pipeline);
  const own = block?.configuration?.python_worker || '';
  const fallback = pipeline?.environment?.worker;
  const labelOf = (name: string) => workers.find(w => w.name === name)?.label || name;

  const found = worker ? workers.find(w => w.name === worker) : null;
  const missing = !!worker && !!data && !found;
  const unprepared = !!found && !found.prepared;
  const name = worker ? labelOf(worker) : 'MageML\'s Python';
  const text = missing ? `${worker} · missing` : unprepared ? `${name} · not prepared` : name;
  const tip = missing
    ? `This block selects Python worker ${worker}, which the project does not have. Choose `
      + 'another.'
    : unprepared
      ? `Runs in ${name}, which is not prepared: the block fails until it is.`
      : worker
        ? `Runs in ${name}, a Python environment of its own. Click to choose another.`
        : 'Runs in MageML\'s own Python. Click to run it in a worker with other packages.';

  const choose = (value: string | null) => {
    setOpen(false);
    if ((value || '') !== own) {
      onChange(value);
    }
  };
  const mark = (selected: boolean, label: string) => `${selected ? '✓ ' : ''}${label}`;
  const items = [
    { isGroupingTitle: true, label: () => 'Runs in', uuid: 'runs-in' },
    ...(fallback
      ? [{
        label: () => mark(!own, `The pipeline's default: ${labelOf(fallback)}`),
        onClick: () => choose(null),
        uuid: 'default',
      }]
      : []),
    {
      label: () => mark(fallback ? own === 'mage' : !own, 'MageML\'s Python'),
      onClick: () => choose(fallback ? 'mage' : null),
      uuid: 'mage',
    },
    ...workers.map(w => ({
      label: () => mark(own === w.name, w.label || w.name),
      onClick: () => choose(w.name),
      tag: w.prepared ? null : 'not prepared',
      uuid: `worker-${w.name}`,
    })),
    { isGroupingTitle: true, label: () => 'Workers', uuid: 'workers' },
    {
      label: () => workers.length ? 'Manage workers' : 'Add a worker',
      linkProps: workers.length ? { href: '/workers' } : { as: '/workers?new=1', href: '/workers' },
      uuid: 'manage',
    },
  ];

  return (
    <FlyoutMenuWrapper
      items={items}
      onClickCallback={() => setOpen(false)}
      onClickOutside={() => setOpen(false)}
      open={open}
      parentRef={ref}
      roundedStyle
      uuid={`runs-in/${block?.uuid}`}
    >
      <div ref={ref}>
        <Tooltip appearBefore label={tip} size={null} widthFitContent>
          <ChipStyle
            aria-expanded={open}
            aria-haspopup="menu"
            aria-label={`Runs in: ${text}`}
            builtin={!worker}
            onClick={(e) => {
              e.stopPropagation();
              setOpen(!open);
            }}
            warning={missing || unprepared}
          >
            {text}
            <span aria-hidden>▾</span>
          </ChipStyle>
        </Tooltip>
      </div>
    </FlyoutMenuWrapper>
  );
}

export default WorkerChip;
