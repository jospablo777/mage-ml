export const PRODUCT_NAME = 'MageML';

export const BRAND_COLORS = Object.freeze({
  dark: Object.freeze({
    accent: '#E88B61',
    text: '#F5F5F7',
    markStart: '#7D55EC',
    markEnd: '#2AB2FE',
    surface: '#232429',
  }),
  light: Object.freeze({
    accent: '#A34820',
    text: '#1C1D22',
    markStart: '#6B3FCE',
    markEnd: '#0869B4',
    surface: '#FAFBFC',
  }),
});

export type BrandTitleOptions = {
  page?: string;
  workspace?: string;
};

export type BrandMetadataOptions = BrandTitleOptions & {
  assetBase?: string;
};

function titlePart(value?: string): string {
  return (value || '').replace(/\s+/g, ' ').trim();
}

export function buildDocumentTitle({ page, workspace }: BrandTitleOptions = {}): string {
  const parts = [titlePart(page), titlePart(workspace)]
    .filter(part => part && part !== PRODUCT_NAME);
  return [...parts, PRODUCT_NAME].join(' | ');
}

export function normalizeAssetBase(value = ''): string {
  if (!value || value === '/') {
    return '';
  }
  if (!value.startsWith('/') || value.startsWith('//') || /[?#\\\s\u0000-\u001f\u007f]/.test(value)) {
    throw new Error('Asset base must be a local path without a query or fragment.');
  }
  const path = value.replace(/\/$/, '');
  const parts = path.slice(1).split('/');
  for (const part of parts) {
    let decoded: string;
    try {
      decoded = decodeURIComponent(part);
    } catch {
      throw new Error('Asset base contains invalid percent encoding.');
    }
    if (
      !decoded
      || decoded === '.'
      || decoded === '..'
      || /[/\\\u0000-\u001f\u007f]/.test(decoded)
      || /%[0-9a-f]{2}/i.test(decoded)
    ) {
      throw new Error('Asset base contains an unsupported path segment.');
    }
  }
  return path;
}

export function buildBrandMetadata({
  page,
  workspace,
  assetBase = '',
}: BrandMetadataOptions = {}) {
  const prefix = normalizeAssetBase(assetBase);
  return {
    title: buildDocumentTitle({ page, workspace }),
    applicationName: PRODUCT_NAME,
    icon: {
      href: `${prefix}/images/branding/mageml-favicon.svg`,
      type: 'image/svg+xml',
      sizes: 'any',
    },
    fallbackIcon: {
      href: `${prefix}/favicon.ico`,
      type: 'image/x-icon',
    },
  } as const;
}
