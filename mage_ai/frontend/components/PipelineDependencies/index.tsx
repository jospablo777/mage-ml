import NextLink from 'next/link';
import styled, { ThemeContext } from 'styled-components';
import { useContext, useMemo, useRef } from 'react';

import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import PipelineDependencyType, {
  PipelineDependencyEdgeType,
  PipelineDependencyKindEnum,
  PipelineDependencyNodeType,
} from '@interfaces/PipelineDependencyType';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { Canvas, Edge, Node } from '@components/DependencyGraph';
import { GraphContainerStyle } from '@components/DependencyGraph/index.style';
import { UNIT } from '@oracle/styles/units/spacing';
import { ZOOMABLE_CANVAS_SIZE } from '@components/DependencyGraph/constants';
import { recenterCanvas } from '@components/DependencyGraph/recenter';
import { timeAgo } from '@utils/models/compilation';

const NODE_WIDTH = 240;
const NODE_HEIGHT = 64;

const KINDS: Record<PipelineDependencyKindEnum, { color: (theme) => string; label: string }> = {
  [PipelineDependencyKindEnum.AFTER_RUN]: {
    color: theme => (theme?.accent || dark.accent).purple,
    label: 'After-run trigger',
  },
  [PipelineDependencyKindEnum.DATA_PRODUCT]: {
    color: theme => (theme?.accent || dark.accent).teal,
    label: 'Reads a global data product',
  },
  [PipelineDependencyKindEnum.API]: {
    color: theme => (theme?.accent || dark.accent).warning,
    label: 'Started by trigger_pipeline in a block',
  },
};

const STATUS_COLORS = {
  cancelled: theme => (theme?.content || dark.content).muted,
  completed: theme => (theme?.accent || dark.accent).positive,
  failed: theme => (theme?.accent || dark.accent).negative,
  running: theme => (theme?.accent || dark.accent).info,
};

const NodeStyle = styled.div<{ missing?: boolean }>`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  box-sizing: border-box;
  cursor: pointer;
  height: ${NODE_HEIGHT}px;
  overflow: hidden;
  padding: ${UNIT}px ${UNIT * 1.25}px;
  width: ${NODE_WIDTH}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px ${props.missing ? 'dashed' : 'solid'} ${
  (props.theme.borders || dark.borders).light};
  `}
`;

const DotStyle = styled.span<{ color: string }>`
  border-radius: 50%;
  display: inline-block;
  height: ${UNIT}px;
  margin-right: ${UNIT * 0.75}px;
  width: ${UNIT}px;

  ${props => `background-color: ${props.color};`}
`;

const TableStyle = styled.table`
  border-collapse: collapse;
  width: 100%;

  td, th {
    padding: ${UNIT * 0.75}px ${UNIT}px ${UNIT * 0.75}px 0;
    text-align: left;
    vertical-align: top;
    ${props => `border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};`}
  }
`;

function lastRunText(node: PipelineDependencyNodeType): string {
  if (!node.exists) {
    return 'not in this project';
  }
  const run = node.last_run;
  if (!run) {
    return 'never run';
  }
  const at = run.completed_at || run.started_at;

  return at ? `${run.status} ${timeAgo(Date.parse(at) / 1000)}` : run.status;
}

function PipelineDependencies() {
  const themeContext = useContext(ThemeContext);
  const canvasRef = useRef(null);
  const { data } = api.pipeline_dependencies.detail('project');
  const graph: PipelineDependencyType = data?.pipeline_dependency;
  const nodesByUUID = useMemo(
    () => Object.fromEntries((graph?.nodes || []).map(node => [node.uuid, node])),
    [graph],
  );

  const { edges, nodes } = useMemo(() => ({
    edges: (graph?.edges || []).map((edge: PipelineDependencyEdgeType, index: number) => ({
      data: edge,
      from: edge.source,
      id: `${edge.source}-${edge.target}-${index}`,
      to: edge.target,
    })),
    nodes: (graph?.nodes || []).map(node => ({
      data: node,
      height: NODE_HEIGHT,
      id: node.uuid,
      width: NODE_WIDTH,
    })),
  }), [graph]);

  if (!graph) {
    return <Spinner inverted />;
  }

  if (!graph.edges.length) {
    return (
      <Text default>
        No pipeline starts or feeds another yet. Links appear here when a trigger runs a pipeline
        after another one, a pipeline reads a global data product, or a block calls
        trigger_pipeline.
      </Text>
    );
  }

  const links = graph.edges.length;
  const pipelines = graph.nodes.length;

  return (
    <>
      <Spacing mb={2}>
        <Text bold large>
          {links} link{links === 1 ? '' : 's'} between {pipelines} pipeline
          {pipelines === 1 ? '' : 's'}
        </Text>
      </Spacing>

      <FlexContainer alignItems="center" flexWrap="wrap">
        {Object.entries(KINDS).map(([kind, { color, label }]) => (
          <Spacing key={kind} mb={1} mr={3}>
            <FlexContainer alignItems="center">
              <DotStyle color={color(themeContext)} />
              <Text small>{label}</Text>
            </FlexContainer>
          </Spacing>
        ))}
      </FlexContainer>

      <Spacing mt={1}>
        <GraphContainerStyle height={UNIT * 60} onDoubleClick={() => recenterCanvas(canvasRef)}>
          <Canvas
            direction="RIGHT"
            edge={edge => (
              <Edge
                {...edge}
                style={{
                  stroke: KINDS[(edge?.properties?.data || edge?.data)?.kind]?.color(themeContext),
                  strokeWidth: 2,
                }}
              />
            )}
            edges={edges}
            fit
            forwardedRef={canvasRef}
            maxHeight={ZOOMABLE_CANVAS_SIZE}
            maxWidth={ZOOMABLE_CANVAS_SIZE}
            maxZoom={1}
            minZoom={-0.7}
            node={node => (
              <Node
                {...node}
                label={null}
                port={null}
                style={{ fill: 'transparent', stroke: 'transparent', strokeWidth: 0 }}
              >
                {(event) => {
                  const pipeline: PipelineDependencyNodeType = event.node.data;
                  const status = pipeline?.last_run?.status;

                  return (
                    <foreignObject height={event.height} width={event.width} x={0} y={0}>
                      <NextLink href={`/pipelines/${pipeline.uuid}/triggers`}>
                        <NodeStyle missing={!pipeline.exists}>
                          <Text bold default monospace noWrapping title={pipeline.uuid}>
                            {pipeline.name}
                          </Text>
                          <FlexContainer alignItems="center">
                            {status && (
                              <DotStyle
                                color={(STATUS_COLORS[status] || STATUS_COLORS.cancelled)(
                                  themeContext,
                                )}
                              />
                            )}
                            <Text muted small>{lastRunText(pipeline)}</Text>
                          </FlexContainer>
                        </NodeStyle>
                      </NextLink>
                    </foreignObject>
                  );
                }}
              </Node>
            )}
            nodes={nodes}
          />
        </GraphContainerStyle>
      </Spacing>

      <Spacing mt={3}>
        <TableStyle>
          <thead>
            <tr>
              <th><Text bold muted small>From</Text></th>
              <th><Text bold muted small>To</Text></th>
              <th><Text bold muted small>How</Text></th>
            </tr>
          </thead>
          <tbody>
            {graph.edges.map((edge, index) => (
              <tr key={`${edge.source}-${edge.target}-${index}`}>
                {[edge.source, edge.target].map(uuid => (
                  <td key={uuid}>
                    {nodesByUUID[uuid]?.exists ? (
                      <NextLink href={`/pipelines/${uuid}/triggers`} legacyBehavior passHref>
                        <Link monospace small>{uuid}</Link>
                      </NextLink>
                    ) : (
                      <Text monospace muted small>{uuid} (missing)</Text>
                    )}
                  </td>
                ))}
                <td>
                  <FlexContainer alignItems="center">
                    <DotStyle color={KINDS[edge.kind]?.color(themeContext)} />
                    <Text small>
                      {edge.label}
                      {edge.trigger && ` (trigger ${edge.trigger}${
                        edge.active === false ? ', inactive' : ''})`}
                    </Text>
                  </FlexContainer>
                </td>
              </tr>
            ))}
          </tbody>
        </TableStyle>
      </Spacing>

      {graph.pipelines_without_dependencies > 0 && (
        <Spacing mt={2}>
          <Text muted small>
            {graph.pipelines_without_dependencies} other pipeline
            {graph.pipelines_without_dependencies === 1 ? ' has' : 's have'} no links to
            other pipelines.
          </Text>
        </Spacing>
      )}
    </>
  );
}

export default PipelineDependencies;
