import styled from 'styled-components';

import Tooltip from '@oracle/components/Tooltip';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { PythonWorkerType } from '@components/PipelineDetail/Settings/PythonWorker';
import { UNIT } from '@oracle/styles/units/spacing';

const ChipStyle = styled.button<{ warning?: boolean }>`
  background: none;
  border-radius: ${BORDER_RADIUS_SMALL}px;
  cursor: pointer;
  font-size: 12px;
  line-height: 18px;
  // A long name does not push the header's buttons out: the tooltip has all of it.
  max-width: ${UNIT * 22}px;
  overflow: hidden;
  padding: 0 ${UNIT * 0.75}px;
  text-overflow: ellipsis;
  white-space: nowrap;

  ${props => `
    border: 1px solid ${props.warning
      ? (props.theme.accent || dark.accent).warning
      : (props.theme.borders || dark.borders).medium};
    color: ${props.warning
      ? (props.theme.accent || dark.accent).warning
      : (props.theme.content || dark.content).muted};
  `}
`;

// Where a Python block runs, in its header: the worker's name, and a warning while the
// worker is not prepared (the block fails until it is). A click opens its settings.
function WorkerChip({ onClick, worker }: { onClick: () => void; worker: string }) {
  // One request for every block of the page: SWR shares it by its key.
  const { data } = api.python_workers.list({}, { revalidateOnFocus: false });
  const found: PythonWorkerType = (data?.python_workers || []).find(w => w.name === worker);
  const name = found?.label || worker;
  const missing = !!data && !found;
  const unprepared = !!found && !found.prepared;
  const warning = missing || unprepared;
  const text = missing ? `${worker} · missing` : unprepared ? `${name} · not prepared` : name;
  const label = missing
    ? `This block selects Python worker ${worker}, which the project does not have.`
    : unprepared
      ? `Runs in ${name} (${worker}), which is not prepared: the block fails until it is. `
        + 'Prepare it in the block settings.'
      : `Runs in ${name} (${worker}), a Python worker of its own.`;

  return (
    <Tooltip appearBefore label={label} size={null} widthFitContent>
      <ChipStyle aria-label={`Python worker: ${text}`} onClick={onClick} warning={warning}>
        {text}
      </ChipStyle>
    </Tooltip>
  );
}

export default WorkerChip;
