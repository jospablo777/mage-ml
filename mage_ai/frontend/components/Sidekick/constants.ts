import PipelineType, { PipelineTypeEnum } from '@interfaces/PipelineType';
import ProjectType, { FeatureUUIDEnum } from '@interfaces/ProjectType';
import {
  AlertTriangle,
  Callback,
  Charts,
  Interactions,
  Lightning,
  NavReport,
  PlayButton,
  Secrets,
  Settings,
  SettingsWithKnobs,
  Table,
  Terminal,
  Tree,
  Union,
  Variables,
} from '@oracle/icons';
import { indexBy } from '@utils/array';

export const VIEW_QUERY_PARAM = 'sideview';
export const VH_PERCENTAGE = 90;

export enum ViewKeyEnum {
  ADDON_BLOCKS = 'addon_blocks',
  BLOCK_SETTINGS = 'block_settings',
  CALLBACKS = 'callbacks',
  CHARTS = 'charts',
  DATA = 'data',
  EXTENSIONS = 'power_ups',
  FILES = 'files',
  FILE_VERSIONS = 'file_versions',
  GRAPHS = 'graphs',
  INTERACTIONS = 'interactions',
  REPORTS = 'reports',
  SAMPLES = 'samples',
  STREAM_RUN = 'stream_run',
  DEAD_LETTERS = 'dead_letters',
  SECRETS = 'secrets',
  SETTINGS = 'settings',
  TERMINAL = 'terminal',
  TREE = 'tree',
  VARIABLES = 'variables',
}

export const FULL_WIDTH_VIEWS = [
  ViewKeyEnum.BLOCK_SETTINGS,
  ViewKeyEnum.CALLBACKS,
  ViewKeyEnum.CHARTS,
  ViewKeyEnum.DATA,
  ViewKeyEnum.EXTENSIONS,
  ViewKeyEnum.TREE,
];

export const MESSAGE_VIEWS = [
  ViewKeyEnum.DATA,
];

export function SIDEKICK_VIEWS(opts?: {
  pipeline?: PipelineType;
  project?: ProjectType;
}): {
  buildLabel?: (opts: {
    pipeline: PipelineType;
    secrets?: {
      [key: string]: any;
    }[];
    variables?: {
      [key: string]: any;
    }[];
  }) => string;
  key: ViewKeyEnum;
  label?: string;
}[] {
  const arr: {
    buildLabel?: (opts: {
      pipeline: PipelineType;
      secrets?: {
        [key: string]: any;
      }[];
      variables?: {
        [key: string]: any;
      }[];
    }) => string;
    key: ViewKeyEnum;
    label?: string;
  }[] = [
    {
      key: ViewKeyEnum.TREE,
      label: 'Tree',
    },
    {
      buildLabel: ({
        pipeline,
      }) => {
        const { widgets = [] } = pipeline || {};

        if (widgets?.length >= 1) {
          return `Charts (${widgets.length})`;
        }

        return 'Charts';
      },
      key: ViewKeyEnum.CHARTS,
    },
    {
      buildLabel: ({
        variables,
      }) => {
        if (variables?.length >= 1) {
          return `Variables (${variables.length})`;
        }

        return 'Variables';
      },
      key: ViewKeyEnum.VARIABLES,
    },
    {
      buildLabel: ({
        secrets,
      }) => {
        if (secrets?.length >= 1) {
          return `Secrets (${secrets.length})`;
        }

        return 'Secrets';
      },
      key: ViewKeyEnum.SECRETS,
    },
  ];

  if (PipelineTypeEnum.PYSPARK !== opts?.pipeline?.type) {
    arr.push(...[
      {
        buildLabel: ({
          pipeline,
        }) => 'Add-on blocks',
        key: ViewKeyEnum.ADDON_BLOCKS,
      },
      {
        buildLabel: ({
          pipeline,
        }) => {
          const { extensions = {} } = pipeline || {};
          let extensionsCount = 0;
          Object.values(extensions).forEach(({ blocks }) => {
            extensionsCount += blocks?.length || 0;
          });

          if (extensionsCount >= 1) {
            return `Power ups (${extensionsCount})`;
          }

          return 'Power ups';
        },
        key: ViewKeyEnum.EXTENSIONS,
      },
    ]);
  }

  if (PipelineTypeEnum.STREAMING === opts?.pipeline?.type) {
    arr.unshift({
      key: ViewKeyEnum.SAMPLES,
      label: 'Samples',
    }, {
      key: ViewKeyEnum.STREAM_RUN,
      label: 'Run',
    }, {
      key: ViewKeyEnum.DEAD_LETTERS,
      label: 'Dead letters',
    });
  }

  arr.push(...[
    {
      key: ViewKeyEnum.DATA,
      label: 'Data',
    },
    {
      key: ViewKeyEnum.TERMINAL,
      label: 'Terminal',
    },
    {
      key: ViewKeyEnum.BLOCK_SETTINGS,
      label: 'Block settings',
    },
  ]);

  if (opts?.project?.features?.[FeatureUUIDEnum.INTERACTIONS]) {
    arr.push({
      key: ViewKeyEnum.INTERACTIONS,
      label: 'Interactions',
    });
  }

  return arr;
}

export function SIDEKICK_VIEWS_BY_KEY(opts?: {
  pipeline?: PipelineType;
  project?: ProjectType;
}) {
  return indexBy(SIDEKICK_VIEWS(opts), ({ key }) => key)
};

export const NAV_ICON_MAPPING = {
  [ViewKeyEnum.ADDON_BLOCKS]: Union,
  [ViewKeyEnum.BLOCK_SETTINGS]: SettingsWithKnobs,
  [ViewKeyEnum.CALLBACKS]: Callback,
  [ViewKeyEnum.CHARTS]: Charts,
  [ViewKeyEnum.DATA]: Table,
  [ViewKeyEnum.SAMPLES]: NavReport,
  [ViewKeyEnum.STREAM_RUN]: PlayButton,
  [ViewKeyEnum.DEAD_LETTERS]: AlertTriangle,
  [ViewKeyEnum.EXTENSIONS]: Lightning,
  [ViewKeyEnum.INTERACTIONS]: Interactions,
  [ViewKeyEnum.SECRETS]: Secrets,
  [ViewKeyEnum.SETTINGS]: Settings,
  [ViewKeyEnum.TERMINAL]: Terminal,
  [ViewKeyEnum.TREE]: Tree,
  [ViewKeyEnum.VARIABLES]: Variables,
};
