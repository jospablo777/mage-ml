import { useMemo, useState } from 'react';
import { useMutation } from 'react-query';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import Select from '@oracle/elements/Inputs/Select';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import { onSuccess } from '@api/utils/response';
import { useError } from '@context/Error';

export type DataContractBindingType = {
  enforcement?: 'fail' | 'warn' | 'off';
  name: string;
  output?: string;
  version?: string;
};

type ViolationType = {
  column?: string;
  examples?: number[];
  message: string;
  rule: string;
  rows: number;
  values?: string[];
};

type DataContractType = {
  error?: string;
  file_path?: string;
  name: string;
  report?: {
    passed: boolean;
    rows: number;
    violations: ViolationType[];
  };
  version?: string;
};

const ENFORCEMENTS = [
  ['fail', 'Fail the block'],
  ['warn', 'Warn and continue'],
  ['off', 'Do not check'],
];

function bindingOf(value: string | DataContractBindingType): DataContractBindingType {
  if (!value) {
    return null;
  }
  return typeof value === 'string' ? { name: value } : value;
}

type DataContractProps = {
  blockUUID: string;
  fetchFileTree?: () => void;
  onChange: (binding: DataContractBindingType) => void;
  pipelineUUID: string;
  value?: string | DataContractBindingType;
};

// The block's data contract: which contract its output must meet, and what a violation does.
function DataContract({
  blockUUID,
  fetchFileTree,
  onChange,
  pipelineUUID,
  value,
}: DataContractProps) {
  const [showError] = useError(null, {}, [], { uuid: 'BlockSettings/DataContract' });
  const [drafted, setDrafted] = useState<DataContractType>(null);
  const binding = useMemo(() => bindingOf(value), [value]);

  const { data: dataContracts, mutate: fetchContracts } = api.data_contracts.list();
  const contracts: DataContractType[] = useMemo(
    () => dataContracts?.data_contracts || [],
    [dataContracts],
  );
  const { data: dataContract } = api.data_contracts.detail(
    binding?.name,
    { block_uuid: blockUUID, pipeline_uuid: pipelineUUID },
  );
  const contract: DataContractType = dataContract?.data_contract;
  const report = contract?.report;

  const [draft, { isLoading: isDrafting }] = useMutation(
    api.data_contracts.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ data_contract: created }) => {
          setDrafted(created);
          fetchContracts();
          fetchFileTree?.();
          onChange({ enforcement: binding?.enforcement || 'fail', name: created.name });
        },
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );

  const missing = binding?.name && dataContracts && !contracts.some(c => c.name === binding.name);

  return (
    <>
      <Headline level={5}>Data contract</Headline>
      <Spacing mt={1}>
        <Text muted small>
          Each time the block runs with its tests, its whole output is checked against a
          contract in the project&apos;s contracts folder: columns, types, missing values,
          ranges, allowed values, patterns and unique keys. When the check fails the block,
          downstream blocks do not run.
        </Text>
      </Spacing>

      <Spacing mt={1}>
        <FlexContainer alignItems="center">
          <Select
            compact
            monospace
            onChange={e => onChange(e.target.value
              ? { ...binding, enforcement: binding?.enforcement || 'fail', name: e.target.value }
              : null)}
            placeholder="No contract"
            value={binding?.name || ''}
          >
            <option value="">No contract</option>
            {missing && <option value={binding.name}>{binding.name} (missing)</option>}
            {contracts.map(({ error, name, version }) => (
              <option key={name} value={name}>
                {name}{version ? ` ${version}` : ''}{error ? ' (invalid)' : ''}
              </option>
            ))}
          </Select>
          {binding?.name && (
            <>
              <Spacing ml={1} />
              <Select
                compact
                onChange={e => onChange({ ...binding, enforcement: e.target.value })}
                value={binding?.enforcement || 'fail'}
              >
                {ENFORCEMENTS.map(([key, label]) => (
                  <option key={key} value={key}>{label}</option>
                ))}
              </Select>
            </>
          )}
        </FlexContainer>
      </Spacing>

      {!binding?.name && (
        <Spacing mt={1}>
          <Button
            compact
            loading={isDrafting}
            // @ts-ignore
            onClick={() => draft({
              data_contract: {
                block_uuid: blockUUID,
                name: blockUUID.replace(/\//g, '_'),
                pipeline_uuid: pipelineUUID,
              },
            })}
          >
            Draft a contract from the output
          </Button>
        </Spacing>
      )}

      {drafted && (
        <Spacing mt={1}>
          <Text small>
            Saved as {drafted.file_path} with the output&apos;s columns and types. Add ranges,
            allowed values and unique keys there, then save these settings.
          </Text>
        </Spacing>
      )}
      {missing && (
        <Spacing mt={1}>
          <Text small warning>
            contracts/{binding.name}.yaml does not exist; the block fails until it does.
          </Text>
        </Spacing>
      )}
      {contract?.error && (
        <Spacing mt={1}>
          <Text danger preWrap small>{contract.error}</Text>
        </Spacing>
      )}

      {binding?.name && report && (
        <Spacing mt={1}>
          <Text muted small>
            Last check in the notebook: {report.passed
              ? `passed (${report.rows.toLocaleString()} rows).`
              : `${report.violations.length} broken (${report.rows.toLocaleString()} rows).`}
          </Text>
          {report.violations?.map(violation => (
            <Text
              danger
              key={`${violation.column}-${violation.rule}`}
              preWrap
              small
            >
              {violation.message}
              {violation.values?.length ? `; values: ${violation.values.join(', ')}` : ''}
              {violation.examples?.length ? `; rows: ${violation.examples.join(', ')}` : ''}
            </Text>
          ))}
        </Spacing>
      )}
    </>
  );
}

export default DataContract;
