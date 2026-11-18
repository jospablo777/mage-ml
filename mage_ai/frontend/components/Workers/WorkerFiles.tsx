import { useState } from 'react';
import { useMutation } from 'react-query';

import Button from '@oracle/elements/Button';
import CodeEditor from '@components/CodeEditor';
import ErrorCallout from './ErrorCallout';
import OlderReleases from './OlderReleases';
import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import { CardStyle, ChangesList, MonoStyle, plural } from './shared';
import { HeldPackageType, WorkerType } from './types';
import { onSuccess } from '@api/utils/response';

// Monaco has no TOML mode; its ini mode colors TOML's tables, keys and strings.
const LANGUAGE = 'ini';

// uv.lock as uv wrote it, read only: a hand edit would only break the lock.
function LockFile({ path }: { path: string }) {
  const { data } = api.file_contents.detail(encodeURIComponent(path));
  if (data?.error) {
    return <Text muted small>{data.error.message}</Text>;
  }
  return (
    <CodeEditor
      fontLigatures={false}
      height={420}
      language={LANGUAGE}
      readOnly
      uuid={`worker-lock/${path}`}
      value={data?.file_content?.content || ''}
      width="100%"
    />
  );
}

// The worker's pyproject.toml as text, for what the package list does not show: extras,
// markers, [tool.uv] indexes and sources. Saved like a change of packages: refused when it
// is not TOML, locked in a copy first, then written and prepared.
function WorkerFiles({ onSaved, worker }: { onSaved: () => void; worker: WorkerType & {
  pyproject?: string;
} }) {
  const [text, setText] = useState<string>(worker.pyproject || '');
  const [error, setError] = useState<string>(null);
  const [saved, setSaved] = useState<WorkerType>(null);
  const [showLock, setShowLock] = useState(false);
  const [confirm, setConfirm] = useState<HeldPackageType[]>(null);
  const [save, { isLoading }] = useMutation(
    api.python_workers.useUpdate(worker.name),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ python_worker: result }) => {
          setError(null);
          if (result?.confirm?.length) {
            setConfirm(result.confirm);
            return;
          }
          setConfirm(null);
          setSaved(result);
          onSaved();
        },
        onErrorCallback: response => setError(response?.error?.message || 'Not saved.'),
      }),
    },
  );
  const dirty = text !== (worker.pyproject || '');

  return (
    <>
      <CardStyle>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <div>
            <Text bold>
              <MonoStyle>{worker.definition}/pyproject.toml</MonoStyle>
            </Text>
            <Text muted small>
              The worker&apos;s uv project. Saving locks it in a copy first: if uv cannot lock
              it, nothing changes.
            </Text>
          </div>
        </FlexContainer>
        <Spacing mt={2}>
          <CodeEditor
            fontLigatures={false}
            height={420}
            language={LANGUAGE}
            onChange={(value) => {
              setConfirm(null);
              setText(value);
            }}
            onSave={() => {
              if (dirty) {
                // @ts-ignore
                save({ python_worker: { pyproject: text } });
              }
            }}
            uuid={`worker-pyproject/${worker.name}`}
            value={text}
            width="100%"
          />
        </Spacing>
        {error && (
          <Spacing mt={2}><ErrorCallout message={error} title="Not saved" /></Spacing>
        )}
        {confirm && dirty && (
          <Spacing mt={2}>
            <OlderReleases
              accepting={isLoading}
              // @ts-ignore
              onAccept={() => save({ python_worker: { accept_older: true, pyproject: text } })}
              onCancel={() => setConfirm(null)}
              packages={confirm}
            />
          </Spacing>
        )}
        {saved && !dirty && (
          <Spacing mt={2}>
            <Text bold small>
              Saved{saved.changes?.length
                ? `: ${plural(saved.changes.length, 'package')} changed` : ''}
            </Text>
            {saved.changes?.length > 0 && <ChangesList changes={saved.changes} />}
          </Spacing>
        )}
        <Spacing mt={2}>
          <FlexContainer alignItems="center">
            <Button
              disabled={!dirty}
              loading={isLoading}
              // @ts-ignore
              onClick={() => save({ python_worker: { pyproject: text } })}
              primary
            >
              Save and prepare
            </Button>
            {dirty && (
              <>
                <Spacing ml={1} />
                <Button onClick={() => setText(worker.pyproject || '')}>Discard changes</Button>
              </>
            )}
          </FlexContainer>
        </Spacing>
      </CardStyle>

      <Spacing mt={3}>
        <CardStyle>
          <FlexContainer alignItems="center" justifyContent="space-between">
            <div>
              <Text bold><MonoStyle>{worker.definition}/uv.lock</MonoStyle></Text>
              <Text muted small>
                The exact versions, written by uv; it changes when the packages do.
              </Text>
            </div>
            <Link onClick={() => setShowLock(!showLock)} small>
              {showLock ? 'Hide uv.lock' : 'Show uv.lock'}
            </Link>
          </FlexContainer>
          {showLock && (
            <Spacing mt={2}><LockFile path={`${worker.definition}/uv.lock`} /></Spacing>
          )}
        </CardStyle>
      </Spacing>
    </>
  );
}

export default WorkerFiles;
