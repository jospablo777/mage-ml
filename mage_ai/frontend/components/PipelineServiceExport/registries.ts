// Registries the export dialog builds and pushes to, and the docker commands for them.
type Field = { key: string; label: string; placeholder: string; options?: string[] };

type Registry = {
  fields: Field[];
  image: (v: { [key: string]: string }, name: string, tag: string) => string;
  label: string;
  login?: (v: { [key: string]: string }) => string | undefined;
};

const ICR_HOSTS = [
  'icr.io', 'us.icr.io', 'uk.icr.io', 'de.icr.io', 'fr2.icr.io', 'es.icr.io', 'jp.icr.io',
  'jp2.icr.io', 'au.icr.io', 'br.icr.io', 'ca.icr.io',
];

function otherPath(v: { [key: string]: string }): string {
  return (v.path || 'registry.example.com/team').trim().replace(/\/+$/, '');
}

export const REGISTRIES: { [key: string]: Registry } = {
  none: {
    fields: [],
    image: (_, name, tag) => `${name}:${tag}`,
    label: 'No registry: an image on this machine',
  },
  dockerhub: {
    fields: [{ key: 'user', label: 'Docker Hub user or organization', placeholder: 'team' }],
    image: (v, name, tag) => `docker.io/${v.user || 'team'}/${name}:${tag}`,
    label: 'Docker Hub',
    login: v => `docker login -u ${v.user || 'team'}`,
  },
  icr: {
    fields: [
      { key: 'host', label: 'Region', options: ICR_HOSTS, placeholder: 'icr.io' },
      { key: 'namespace', label: 'Namespace', placeholder: 'team' },
    ],
    image: (v, name, tag) => `${v.host || 'icr.io'}/${v.namespace || 'team'}/${name}:${tag}`,
    label: 'IBM Cloud Container Registry',
    login: v => [
      'ibmcloud cr login',
      `# or with an API key: echo "$IBM_CLOUD_API_KEY" | docker login -u iamapikey --password-stdin ${v.host || 'icr.io'}`,
    ].join('\n'),
  },
  ghcr: {
    fields: [{ key: 'owner', label: 'GitHub user or organization', placeholder: 'team' }],
    image: (v, name, tag) => `ghcr.io/${v.owner || 'team'}/${name}:${tag}`,
    label: 'GitHub Container Registry',
    login: v => `echo "$GITHUB_TOKEN" | docker login ghcr.io -u ${v.owner || 'team'} --password-stdin`,
  },
  ecr: {
    fields: [
      { key: 'account', label: 'AWS account id', placeholder: '123456789012' },
      { key: 'region', label: 'Region', placeholder: 'us-east-1' },
    ],
    image: (v, name, tag) => `${v.account || '123456789012'}.dkr.ecr.${v.region || 'us-east-1'}.amazonaws.com/${name}:${tag}`,
    label: 'Amazon ECR',
    login: (v) => {
      const host = `${v.account || '123456789012'}.dkr.ecr.${v.region || 'us-east-1'}.amazonaws.com`;

      return `aws ecr get-login-password --region ${v.region || 'us-east-1'} | docker login --username AWS --password-stdin ${host}`;
    },
  },
  gar: {
    fields: [
      { key: 'region', label: 'Region', placeholder: 'us-central1' },
      { key: 'project', label: 'Project', placeholder: 'my-project' },
      { key: 'repository', label: 'Repository', placeholder: 'services' },
    ],
    image: (v, name, tag) => `${v.region || 'us-central1'}-docker.pkg.dev/${v.project || 'my-project'}/${v.repository || 'services'}/${name}:${tag}`,
    label: 'Google Artifact Registry',
    login: v => `gcloud auth configure-docker ${v.region || 'us-central1'}-docker.pkg.dev`,
  },
  acr: {
    fields: [{ key: 'registry', label: 'Registry name', placeholder: 'myregistry' }],
    image: (v, name, tag) => `${v.registry || 'myregistry'}.azurecr.io/${name}:${tag}`,
    label: 'Azure Container Registry',
    login: v => `az acr login --name ${v.registry || 'myregistry'}`,
  },
  other: {
    fields: [{ key: 'path', label: 'Registry and path', placeholder: 'registry.example.com/team' }],
    image: (v, name, tag) => `${otherPath(v)}/${name}:${tag}`,
    label: 'Other registry, such as localhost:5000',
    // A registry on this machine, such as registry:2, takes pushes without a login.
    login: (v) => {
      const host = otherPath(v).split('/')[0];

      return /^(localhost|127\.0\.0\.1)(:|$)/.test(host) ? undefined : `docker login ${host}`;
    },
  },
};

export const PLATFORMS = ['linux/amd64', 'linux/arm64'];

// What each platform is, in the words of the machines it runs on.
export const PLATFORM_LABELS: { [platform: string]: string } = {
  'linux/amd64': 'Linux x86-64 (amd64): most cloud servers',
  'linux/arm64': 'Linux ARM64: AWS Graviton, Ampere, Apple silicon',
};

// Per platform, the build's last step and whether it failed, from buildx's lines such as
// "#12 [linux/arm64 3/9] RUN ..." and "ERROR [linux/arm64 3/9] ...".
export function platformProgress(lines: string[]): {
  [platform: string]: { done: number; total: number; failed: boolean };
} {
  const progress = {};
  (lines || []).forEach((line) => {
    const step = line.match(/\[(linux\/[a-z0-9/]+) (\d+)\/(\d+)\]/);
    if (step) {
      const [, platform, done, total] = step;
      const known = progress[platform] || { done: 0, failed: false, total: 0 };
      progress[platform] = {
        ...known,
        done: Math.max(known.done, Number(done)),
        total: Number(total),
      };
    }
    const failed = line.match(/ERROR[^[]*\[(linux\/[a-z0-9/]+)/);
    if (failed) {
      const known = progress[failed[1]] || { done: 0, failed: false, total: 0 };
      progress[failed[1]] = { ...known, failed: true };
    }
  });
  return progress;
}

export function shipCommands({
  image,
  login,
  platforms,
  push,
}: {
  image: string;
  login?: string;
  platforms: string[];
  push: boolean;
}): { lines: string; problem?: string } {
  const lines = [];
  if (push && login) {
    lines.push(login, '');
  }
  if (!platforms.length) {
    lines.push(`docker build -t ${image} .`);
    if (push) {
      lines.push(`docker push ${image}`);
    }
  } else if (platforms.length > 1 && !push) {
    return {
      lines: `docker buildx build --platform ${platforms.join(',')} -t ${image} --push .`,
      problem: 'Two platforms need a registry: Docker keeps one platform of an image locally.',
    };
  } else {
    lines.push(
      `docker buildx build --platform ${platforms.join(',')} -t ${image} ${push ? '--push' : '--load'} .`,
    );
  }

  return { lines: lines.join('\n') };
}
