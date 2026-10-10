import Headline from '@oracle/elements/Headline';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';

export type BlockResourcesType = {
  cpu?: number;
  gpu?: number;
  memory?: string;
  uses?: string[];
};

type ResourcesProps = {
  onChange: (resources: BlockResourcesType) => void;
  value?: BlockResourcesType;
};

function cleaned(resources: BlockResourcesType): BlockResourcesType {
  const result: BlockResourcesType = {};
  if (resources.memory) {
    result.memory = resources.memory;
  }
  if (resources.cpu) {
    result.cpu = resources.cpu;
  }
  if (resources.gpu) {
    result.gpu = resources.gpu;
  }
  if (resources.uses?.length) {
    result.uses = resources.uses;
  }
  return Object.keys(result).length ? result : null;
}

// What the block needs to start: memory, CPUs, GPUs and shared limits (resources.py).
function Resources({ onChange, value }: ResourcesProps) {
  const resources = value || {};
  const update = (changes: BlockResourcesType) => onChange(cleaned({ ...resources, ...changes }));
  const wholeNumber = (text: string) => {
    const number = parseInt(text, 10);
    return Number.isFinite(number) && number > 0 ? number : undefined;
  };

  return (
    <>
      <Headline level={5}>Resources</Headline>
      <Spacing mt={1}>
        <Text muted small>
          In triggered runs the block starts only when what it needs is free. Memory and
          GPUs are counted per scheduler; shared limits (declared under resources in the
          project&apos;s metadata.yaml) across every pipeline. CPUs set the size of the
          block&apos;s thread pools. Leave them empty to start the block as soon as it is ready.
        </Text>
      </Spacing>
      <Spacing mt={1}>
        <FlexContainer alignItems="center" flexWrap="wrap">
          <TextInput
            compact
            label="Memory (8GiB)"
            monospace
            onChange={e => update({ memory: e.target.value.trim() })}
            value={resources.memory || ''}
          />
          <Spacing ml={1} />
          <TextInput
            compact
            label="CPUs"
            monospace
            onChange={e => update({ cpu: wholeNumber(e.target.value) })}
            type="number"
            value={resources.cpu ? String(resources.cpu) : ''}
          />
          <Spacing ml={1} />
          <TextInput
            compact
            label="GPUs"
            monospace
            onChange={e => update({ gpu: wholeNumber(e.target.value) })}
            type="number"
            value={resources.gpu ? String(resources.gpu) : ''}
          />
        </FlexContainer>
      </Spacing>
      <Spacing mt={1}>
        <TextInput
          compact
          fullWidth
          label="Shared limits it uses (warehouse, gpu_host)"
          monospace
          onChange={e => update({
            uses: e.target.value.split(',').map(item => item.trim()).filter(Boolean),
          })}
          value={(resources.uses || []).join(', ')}
        />
      </Spacing>
    </>
  );
}

export default Resources;
