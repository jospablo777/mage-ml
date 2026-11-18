import styled from 'styled-components';

import BlockType from '@interfaces/BlockType';
import Select from '@oracle/elements/Inputs/Select';
import Text from '@oracle/elements/Text';
import dark from '@oracle/styles/themes/dark';
import { PythonWorkerType } from './PythonWorker';
import { UNIT } from '@oracle/styles/units/spacing';

const TableStyle = styled.table`
  border-collapse: collapse;
  margin-top: ${UNIT}px;
  width: 100%;

  td, th {
    padding: ${UNIT * 0.5}px ${UNIT}px ${UNIT * 0.5}px 0;
    text-align: left;
    vertical-align: middle;
    ${props => `border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};`}
  }

  td:first-child {
    max-width: ${UNIT * 32}px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  td:last-child {
    white-space: nowrap;
  }
`;

// Where each Python block of the saved pipeline runs, chosen here for all of them at
// once: its own worker, the pipeline's default, or MageML's Python, and whether that
// worker is ready. A choice is the block's python_worker; '' is the pipeline's default.
function BlockWorkers({ blocks, choices, defaultWorker, onChoose, workers }: {
  blocks: BlockType[];
  choices: { [block: string]: string };
  defaultWorker?: string;
  onChoose: (block: string, worker: string) => void;
  workers: PythonWorkerType[];
}) {
  const python = blocks.filter(block => block?.language === 'python');
  if (!python.length) {
    return null;
  }

  const defaultLabel = !defaultWorker || defaultWorker === 'mage'
    ? 'MageML\'s Python'
    : workers.find(w => w.name === defaultWorker)?.label || defaultWorker;

  return (
    <TableStyle>
      <thead>
        <tr>
          {['Block', 'Runs in', ''].map(title => (
            <th key={title}><Text bold muted small>{title}</Text></th>
          ))}
        </tr>
      </thead>
      <tbody>
        {python.map((block) => {
          const own = block.uuid in choices
            ? choices[block.uuid]
            : (block?.configuration?.python_worker || '');
          const chosen = own || defaultWorker;
          const builtin = !chosen || chosen === 'mage';
          const worker = builtin ? null : workers.find(w => w.name === chosen);
          let state = <Text muted small>Ready</Text>;
          if (!builtin && !worker) {
            state = <Text danger small>Missing</Text>;
          } else if (worker && !worker.prepared) {
            state = <Text small warning>Not prepared</Text>;
          }

          return (
            <tr key={block.uuid}>
              <td title={block.uuid}><Text monospace small>{block.uuid}</Text></td>
              <td>
                <Select
                  aria-label={`Where ${block.uuid} runs`}
                  compact
                  onChange={e => onChoose(block.uuid, e.target.value)}
                  small
                  value={own}
                >
                  <option value="">Default: {defaultLabel}</option>
                  <option value="mage">MageML&apos;s Python</option>
                  {workers.map(w => (
                    <option key={w.name} value={w.name}>{w.label || w.name}</option>
                  ))}
                  {/* A worker the block names that the project does not have. */}
                  {!!own && own !== 'mage' && !workers.some(w => w.name === own) && (
                    <option value={own}>{own}</option>
                  )}
                </Select>
              </td>
              <td>{state}</td>
            </tr>
          );
        })}
      </tbody>
    </TableStyle>
  );
}

export default BlockWorkers;
