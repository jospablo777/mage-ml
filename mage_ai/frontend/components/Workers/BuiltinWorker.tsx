import NextLink from 'next/link';
import styled from 'styled-components';

import CopyToClipboard from '@oracle/components/CopyToClipboard';
import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { CardStyle, MonoStyle, PillStyle, TableStyle } from './shared';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { WorkerType } from './types';

const CodeStyle = styled.pre`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 1.7;
  margin: 0;
  overflow-x: auto;
  padding: ${UNIT * 1.5}px;
  white-space: pre;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const HOW = {
  docker: 'MageML runs from a Docker image: add packages in your own image, built from it.',
  environment: 'MageML runs in this Python environment: install packages into it with uv.',
  project: 'MageML is a package of this uv project: change its pyproject.toml with uv.',
  uv_tool: 'MageML is installed as a uv tool: install it again with the packages it needs.',
};

// MageML's own Python, the worker of every block that names none. Shown, not edited: it is
// the environment the server runs in (python_workers.mageml_environment says why).
function BuiltinWorker({ worker }: { worker: WorkerType }) {
  const commands = (worker.commands || []).join('\n');

  return (
    <>
      <NextLink href="/workers" legacyBehavior passHref><Link small>Workers</Link></NextLink>
      <Spacing mt={1} />
      <Text bold xlarge>{worker.label}</Text>
      <Text default>Every block that does not choose a worker runs here.</Text>
      <Spacing mt={1} />
      <FlexContainer>
        <PillStyle tone="positive">Runs MageML</PillStyle>
        <Spacing ml={1} />
        <PillStyle tone="muted">Python {worker.python}</PillStyle>
        <Spacing ml={1} />
        <PillStyle tone="muted">{worker.package_count} packages</PillStyle>
      </FlexContainer>

      <Spacing mt={3}>
        <CardStyle>
          <Text bold>Change it with uv, then restart MageML</Text>
          <Text muted small>{HOW[worker.kind] || HOW.environment}</Text>
          {worker.project && (
            <Spacing mt={1}>
              <MonoStyle><Text muted small>{worker.project}</Text></MonoStyle>
            </Spacing>
          )}
          <Spacing mt={1.5}>
            <FlexContainer justifyContent="flex-end">
              <CopyToClipboard copiedText={commands} linkText="Copy" small withCopyIcon />
            </FlexContainer>
            <Spacing mt={0.5} />
            <CodeStyle aria-label="Commands">{commands}</CodeStyle>
          </Spacing>
          <Spacing mt={1.5}>
            <Text muted small>
              The app does not change it: it is the environment the server runs in. A change
              while MageML runs can leave it with half of a package, and a version MageML
              cannot start with leaves no page to undo it. For packages only some blocks need,
              or versions that clash with MageML&apos;s (Feast with pandas 2, an older
              MLflow),{' '}
              <NextLink href="/workers?new=1" legacyBehavior passHref>
                <Link inline small>add a worker</Link>
              </NextLink>.
            </Text>
          </Spacing>
        </CardStyle>
      </Spacing>

      <Spacing mt={3}>
        <CardStyle>
          <Text bold>Main packages</Text>
          <Text muted small>
            {worker.package_count} packages in all, in {worker.prefix}.
          </Text>
          <Spacing mt={1.5}>
            <TableStyle aria-label="Main packages">
              <thead>
                <tr><th>Package</th><th>Version</th></tr>
              </thead>
              <tbody>
                {(worker.main_packages || []).map(p => (
                  <tr key={p.name}>
                    <td><MonoStyle>{p.name}</MonoStyle></td>
                    <td><MonoStyle>{p.version}</MonoStyle></td>
                  </tr>
                ))}
              </tbody>
            </TableStyle>
          </Spacing>
        </CardStyle>
      </Spacing>
    </>
  );
}

export default BuiltinWorker;
