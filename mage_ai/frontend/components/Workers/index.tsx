import styled from 'styled-components';
import { useEffect, useMemo, useState } from 'react';
import NextLink from 'next/link';
import { useRouter } from 'next/router';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import NewWorker from './NewWorker';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import api from '@api';
import {
  CardStyle,
  MonoStyle,
  PillStyle,
  RowLinkStyle,
  TableStyle,
  formatBytes,
  needsAttention,
  plural,
  securityState,
  usageText,
  workerState,
} from './shared';
import { NumberStyle, TileStyle } from '@components/PipelineServiceExport';
import { UNIT } from '@oracle/styles/units/spacing';
import { WorkerType } from './types';

const TilesStyle = styled.div`
  display: grid;
  gap: ${UNIT}px;
  grid-template-columns: repeat(auto-fit, minmax(${UNIT * 20}px, 1fr));
`;

const HeaderStyle = styled.div`
  align-items: flex-start;
  display: flex;
  gap: ${UNIT * 2}px;
  justify-content: space-between;
`;

const POLL_MS = 2000;

export const WORKERS_GUIDE_URL =
  'https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/pipelines/python-workers.md';

// The declared packages in a line: the first few, then how many more.
function packagesText(worker: WorkerType): string {
  const names = (worker.dependencies || []).map(d => d.split(/[<>=!~;[ ]/)[0]);
  if (!names.length) {
    return 'No packages yet';
  }
  return names.length <= 3 ? names.join(', ')
    : `${names.slice(0, 3).join(', ')} and ${names.length - 3} more`;
}

const HELD_BACK_HINT = 'Locked below their newest release, which does not fit the other packages';

// The Workers page: every Python worker of the project, whether it can run blocks, how much
// it installs, where it is used and whether it has known vulnerabilities; New worker.
function Workers() {
  const router = useRouter();
  // /workers?new=1, from "Add a worker" in a block's Runs in menu, opens the form.
  const [adding, setAdding] = useState(false);
  useEffect(() => {
    if (router.query?.new) {
      setAdding(true);
    }
  }, [router.query?.new]);
  const { data, mutate } = api.python_workers.list({}, {
    // Every few seconds while one prepares, so its state follows; else on focus.
    refreshInterval: (latest) => (latest?.python_workers || []).some(
      w => w.job?.state === 'running') ? POLL_MS : 0,
  });
  const workers: WorkerType[] = useMemo(() => data?.python_workers || [], [data]);
  const { data: dataMage } = api.python_workers.detail('mage', {}, { revalidateOnFocus: false });
  const mage: WorkerType = dataMage?.python_worker;
  const attention = workers.filter(needsAttention).length;
  const disk = workers.reduce((sum, w) => sum + (w.size_bytes || 0), 0);
  const open = (name: string) => router.push('/workers/[worker]', `/workers/${name}`);

  return (
    <>
      <HeaderStyle>
        <div>
          <Text bold xlarge>Workers</Text>
          <Spacing mt={1} />
          <div style={{ maxWidth: UNIT * 90 }}>
            <Text default>
              A worker is a Python environment of its own, chosen block by block: a block that
              needs Feast with pandas 2 runs in one while the blocks around it run in
              MageML&apos;s Python. Each is prepared once from its uv.lock and kept on this
              server. <Link href={WORKERS_GUIDE_URL} inline openNewWindow>How workers work</Link>
            </Text>
          </div>
        </div>
        {!adding && workers.length > 0 && (
          <Button onClick={() => setAdding(true)} primary>New worker</Button>
        )}
      </HeaderStyle>

      {adding && (
        <Spacing mt={3}>
          <NewWorker
            onCancel={() => setAdding(false)}
            onCreated={(name) => {
              setAdding(false);
              mutate();
              open(name);
            }}
            workers={workers}
          />
        </Spacing>
      )}

      {!data && (
        <Spacing mt={4}><Spinner /></Spacing>
      )}

      {data && workers.length === 0 && !adding && (
        <Spacing mt={3}>
          <CardStyle>
            <Text bold large>No workers yet</Text>
            <Spacing mt={1} />
            <Text default>
              Every Python block runs in MageML&apos;s Python until you add a worker. Add one
              when a block needs a library MageML does not ship, or another version of one it
              does: Feast, an MLflow model, scikit-learn and ONNX Runtime for scoring.
              MageML&apos;s own Python is changed with uv in a terminal:{' '}
              <NextLink href="/workers/mage" passHref><Link inline>see how</Link></NextLink>.
            </Text>
            <Spacing mt={2} />
            <Button onClick={() => setAdding(true)} primary>Add the first worker</Button>
          </CardStyle>
        </Spacing>
      )}

      {workers.length > 0 && (
        <>
          <Spacing mt={3}>
            <TilesStyle>
              <TileStyle>
                <Text muted small>Workers</Text>
                <NumberStyle>{workers.length}</NumberStyle>
              </TileStyle>
              <TileStyle>
                <Text muted small>Ready to run blocks</Text>
                <NumberStyle>{workers.filter(w => w.prepared).length}</NumberStyle>
              </TileStyle>
              <TileStyle>
                <Text muted small>Need attention</Text>
                <NumberStyle>
                  <Text inline large warning={attention > 0}>{attention}</Text>
                </NumberStyle>
              </TileStyle>
              <TileStyle>
                <Text muted small>Packages to install</Text>
                <NumberStyle>{formatBytes(disk)}</NumberStyle>
              </TileStyle>
            </TilesStyle>
          </Spacing>

          <Spacing mt={3}>
            <CardStyle>
              <TableStyle aria-label="Workers">
                <thead>
                  <tr>
                    <th>Worker</th>
                    <th>Python</th>
                    <th>Packages</th>
                    <th>Size</th>
                    <th>Used by</th>
                    <th>State</th>
                    <th>Security</th>
                  </tr>
                </thead>
                <tbody>
                  {mage && (
                    <RowLinkStyle
                      aria-label="Open MageML (default)"
                      onClick={() => open('mage')}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          open('mage');
                        }
                      }}
                      role="link"
                      tabIndex={0}
                    >
                      <td>
                        <Text bold>{mage.label}</Text>
                        <MonoStyle><Text inline muted small>mage</Text></MonoStyle>
                      </td>
                      <td><Text small>{mage.python}</Text></td>
                      <td>
                        <Text small>
                          {(mage.main_packages || []).slice(1, 4).map(p => p.name).join(', ')}
                          {' '}and {Math.max((mage.package_count || 0) - 3, 0)} more
                        </Text>
                      </td>
                      <td><Text muted small>In MageML&apos;s environment</Text></td>
                      <td><Text small>Blocks without a worker</Text></td>
                      <td title="The server runs in it">
                        <PillStyle tone="positive">Runs MageML</PillStyle>
                      </td>
                      <td><Text muted small>Changed with uv</Text></td>
                    </RowLinkStyle>
                  )}
                  {workers.map((worker) => {
                    const state = workerState(worker);
                    const security = securityState(worker.security);
                    return (
                      <RowLinkStyle
                        aria-label={`Open ${worker.label || worker.name}`}
                        key={worker.name}
                        onClick={() => open(worker.name)}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            open(worker.name);
                          }
                        }}
                        role="link"
                        tabIndex={0}
                      >
                        <td>
                          <Text bold>{worker.label || worker.name}</Text>
                          <MonoStyle><Text inline muted small>{worker.name}</Text></MonoStyle>
                        </td>
                        <td><Text small>{worker.python}</Text></td>
                        <td><Text small>{packagesText(worker)}</Text></td>
                        <td><Text small>{formatBytes(worker.size_bytes)}</Text></td>
                        <td>
                          <Text muted={!worker.used_by?.length} small>
                            {usageText(worker.used_by)}
                          </Text>
                        </td>
                        <td title={state.hint}>
                          <PillStyle tone={state.tone}>{state.label}</PillStyle>
                        </td>
                        <td>
                          <PillStyle tone={security.tone}>{security.label}</PillStyle>
                          {worker.security?.fixable > 0 && (
                            <Text muted xsmall>
                              {plural(worker.security.fixable, 'package')} can be fixed
                            </Text>
                          )}
                          {worker.held_back?.length > 0 && (
                            <Text title={HELD_BACK_HINT} warning xsmall>
                              {worker.held_back.join(', ')} held back
                            </Text>
                          )}
                        </td>
                      </RowLinkStyle>
                    );
                  })}
                </tbody>
              </TableStyle>
            </CardStyle>
          </Spacing>

          <Spacing mt={2}>
            <FlexContainer>
              <Text muted small>
                Sizes are what each worker installs on this server&apos;s platform, from its
                uv.lock. Security comes from uv audit (OSV), checked in the background.
              </Text>
            </FlexContainer>
          </Spacing>
        </>
      )}
    </>
  );
}

export default Workers;
