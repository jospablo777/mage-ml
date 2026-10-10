import { ReactNode, useContext, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import styled, { ThemeContext } from 'styled-components';

import { AtlasRequestError, atlasClient, isAbort } from './client';
import { ColumnAtlas } from './ColumnAtlas';
import { HEADER_HEIGHT, ROW_HEIGHT } from './logic';
import { resetViewState, sourceKey, subscribeVersion } from './store';
import { themeVariables } from './styles';
import { AtlasMetadata, AtlasSource } from './types';

const TOOLBAR_HEIGHT = 42;
const STATUS_HEIGHT = 32;
const MAX_INLINE_HEIGHT = 440;

// The server turned the explorer off, or it is not installed: stop asking for this page.
let turnedOff = false;

const Overlay = styled.div`
  background: rgba(0, 0, 0, 0.55);
  inset: 0;
  padding: 24px;
  position: fixed;
  z-index: 10000;

  > div {
    border-radius: 10px;
    box-shadow: 0 24px 64px rgba(0, 0, 0, 0.5);
    height: 100%;
    overflow: hidden;
  }

  @media (max-width: 700px) {
    padding: 0;
    > div { border-radius: 0; }
  }
`;

const Placeholder = styled.div`
  align-items: center;
  border: 1px solid var(--ca-border);
  border-radius: 8px;
  color: var(--ca-muted);
  display: flex;
  font-size: 12px;
  justify-content: center;
`;

export function inlineHeight(rows: number): number {
  const body = Math.max(4, Math.min(rows, 11)) * ROW_HEIGHT;
  return Math.min(MAX_INLINE_HEIGHT, TOOLBAR_HEIGHT + HEADER_HEIGHT + body + STATUS_HEIGHT + 2);
}

function columnsSignature(metadata: AtlasMetadata): string {
  return JSON.stringify(metadata.columns.map(column => [column.name, column.dtype]));
}

type ColumnAtlasOutputProps = {
  // The plain table, shown when the output cannot be explored here.
  fallback: ReactNode;
  // Changes when the block runs again (from the output message), so the explorer reads
  // the new output.
  refreshKey?: string;
  // Rows of the output, when known, to size the placeholder before the metadata arrives.
  rowCountHint?: number;
  source: AtlasSource;
};

export function ColumnAtlasOutput({
  fallback,
  refreshKey,
  rowCountHint,
  source,
}: ColumnAtlasOutputProps) {
  const theme = useContext(ThemeContext);
  const [metadata, setMetadata] = useState<AtlasMetadata | null>(null);
  const [unavailable, setUnavailable] = useState(turnedOff);
  const [expanded, setExpanded] = useState(false);
  const sourceText = JSON.stringify(source);
  const key = sourceKey(source);
  const [reload, setReload] = useState(0);
  // The metadata shown and the source it describes.
  const shown = useRef<{ metadata: AtlasMetadata; sourceText: string } | null>(null);

  // A response reported a newer version of this output: the block ran again.
  useEffect(
    () =>
      subscribeVersion(key, version => {
        const current = shown.current;
        if (current && current.sourceText === sourceText && current.metadata.source_version !== version) {
          setReload(count => count + 1);
        }
      }),
    [key, sourceText],
  );

  useEffect(() => {
    if (turnedOff) {
      setUnavailable(true);
      return undefined;
    }
    const controller = new AbortController();
    const sameSource = shown.current?.sourceText === sourceText;
    // A reload of the same output keeps the explorer on screen until the new metadata
    // arrives; another output starts from the placeholder.
    if (!sameSource) {
      shown.current = null;
      setMetadata(null);
    }
    setUnavailable(false);
    atlasClient
      .metadata(source, controller.signal)
      .then(result => {
        const previous = shown.current;
        if (previous && previous.sourceText === sourceText
          && columnsSignature(previous.metadata) !== columnsSignature(result)) {
          // Filters and sort name columns by position; a new set of columns voids them.
          resetViewState(key);
        }
        shown.current = { metadata: result, sourceText };
        setMetadata(result);
      })
      .catch(error => {
        if (isAbort(error)) return;
        if (error instanceof AtlasRequestError && /turned off/.test(error.message)) {
          turnedOff = true;
        }
        // Any failure shows the plain table; the explorer is an addition to it.
        shown.current = null;
        setUnavailable(true);
      });
    return () => controller.abort();
    // sourceText stands for the source.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceText, refreshKey, reload]);

  useEffect(() => {
    if (!expanded) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setExpanded(false);
    };
    const overflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = overflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [expanded]);

  if (unavailable) return <>{fallback}</>;

  if (!metadata) {
    return (
      <Placeholder
        aria-busy
        style={{ ...themeVariables(theme), height: inlineHeight(rowCountHint ?? 10) }}
      >
        Opening the output…
      </Placeholder>
    );
  }

  return (
    <>
      <ColumnAtlas
        height={inlineHeight(metadata.row_count)}
        metadata={metadata}
        mode="inline"
        onExpand={() => setExpanded(true)}
        source={source}
      />
      {expanded &&
        typeof document !== 'undefined' &&
        createPortal(
          <Overlay
            aria-label={`Output ${metadata.label}`}
            aria-modal
            onMouseDown={event => {
              if (event.target === event.currentTarget) setExpanded(false);
            }}
            role="dialog"
          >
            <div>
              <ColumnAtlas
                metadata={metadata}
                mode="expanded"
                onClose={() => setExpanded(false)}
                source={source}
              />
            </div>
          </Overlay>,
          document.body,
        )}
    </>
  );
}
